use crate::{database::Database, network::Network};
use auto_quorum::common::{
    configs::{OmniPaxosServerConfig, *},
    kv::*,
    messages::*,
};
use chrono::Utc;
use core::panic;
use log::*;
use omnipaxos::{
    ballot_leader_election::Ballot,
    messages::{
        sequence_paxos::{PaxosMessage, PaxosMsg},
        Message,
    },
    sequence_paxos::Phase,
    util::{LogEntry, NodeId},
    OmniPaxos, OmniPaxosConfig,
};
use omnipaxos_storage::memory_storage::MemoryStorage;
use serde::Serialize;
use std::{fs::File, io::Write, time::Duration};
use tempfile::tempfile;
use tokio::sync::mpsc::Receiver;

const LEADER_WAIT: Duration = Duration::from_secs(1);

#[derive(Debug)]
pub(crate) enum DelayStrategy {
    Sleep(Duration),
    FileWrite(File, usize),
}

impl From<DelayConfig> for DelayStrategy {
    fn from(value: DelayConfig) -> Self {
        match value {
            DelayConfig::Sleep(μs) => DelayStrategy::Sleep(Duration::from_micros(μs)),
            DelayConfig::File(data_size) => {
                let file = tempfile().expect("Failed to open temp file");
                DelayStrategy::FileWrite(file, data_size)
            }
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum PersistMode {
    NoPersist,
    Individual,
    Every(usize),
    Opportunistic,
}

impl From<PersistConfig> for PersistMode {
    fn from(value: PersistConfig) -> Self {
        match value {
            PersistConfig::NoPersist => PersistMode::NoPersist,
            PersistConfig::Individual => PersistMode::Individual,
            PersistConfig::Every(n) => PersistMode::Every(n),
            PersistConfig::Opportunistic => PersistMode::Opportunistic,
        }
    }
}

type OmniPaxosInstance = OmniPaxos<Command, MemoryStorage<Command>>;

pub struct OmniPaxosServer {
    id: NodeId,
    peers: Vec<NodeId>,
    database: Database,
    network: Network,
    cluster_messages: Receiver<(ClusterMessage, i64)>,
    client_messages: Receiver<(ClientId, ClientMessage, i64)>,
    omnipaxos: OmniPaxosInstance,
    initial_leader: NodeId,
    persist_mode: PersistMode,
    delay_strat: DelayStrategy,
    accepted_buffer: Vec<(usize, u64, ClusterMessage)>,
    batch_size: usize,
    request_data: Vec<RequestInstrumentation>,
    acceptor_data: Vec<AcceptorInstrumentation>,
    config: OmniPaxosServerConfig,
}

fn n_choose_k(n: usize, k: usize) -> usize {
    if k > n {
        return 0;
    }
    let mut result = 1;
    let k = k.min(n - k); // Use the smaller k, since C(n, k) == C(n, n - k)
    for i in 0..k {
        result = result * (n - i) / (i + 1);
    }
    result
}

impl OmniPaxosServer {
    pub async fn new(
        server_config: OmniPaxosServerConfig,
        omnipaxos_config: OmniPaxosConfig,
        initial_leader: NodeId,
    ) -> Self {
        let server_id = omnipaxos_config.server_config.pid;
        let nodes = omnipaxos_config.cluster_config.nodes.clone();
        let num_nodes = nodes.len();
        let metronome_quorum = match omnipaxos_config.cluster_config.metronome_quorum_size {
            Some(quorum_size) => quorum_size,
            None => (num_nodes / 2) + 1,
        };
        let batch_size = n_choose_k(num_nodes, metronome_quorum);
        let peers: Vec<u64> = nodes
            .into_iter()
            .filter(|node| *node != server_id)
            .collect();
        let local_deployment = server_config.local_deployment.unwrap_or(false);
        info!(
            "Node: {:?}: using metronome: {}, persist_config: {:?}",
            server_id, omnipaxos_config.cluster_config.use_metronome, server_config.persist_config
        );
        let storage = MemoryStorage::default();
        let omnipaxos = omnipaxos_config.build(storage).unwrap();
        let initial_clients = match server_id == initial_leader {
            true => 1,
            false => 0,
        };
        let (cluster_message_sender, cluster_messages) = tokio::sync::mpsc::channel(1000);
        let (client_message_sender, client_messages) = tokio::sync::mpsc::channel(1000);
        let network = Network::new(
            server_config.cluster_name.clone(),
            server_id,
            peers.clone(),
            initial_clients,
            local_deployment,
            client_message_sender,
            cluster_message_sender,
        )
        .await;
        let persist_config = match server_config.delay_config {
            DelayConfig::File(0) => PersistConfig::NoPersist,
            DelayConfig::Sleep(0) => PersistConfig::NoPersist,
            _ => server_config.persist_config,
        };
        let mut server = OmniPaxosServer {
            id: server_id,
            peers,
            database: Database::new(),
            network,
            cluster_messages,
            client_messages,
            omnipaxos,
            initial_leader,
            persist_mode: persist_config.into(),
            delay_strat: server_config.delay_config.into(),
            accepted_buffer: vec![],
            batch_size,
            request_data: Vec::with_capacity(1000),
            acceptor_data: Vec::with_capacity(1000),
            config: server_config,
        };
        // Clears outgoing_messages of initial BLE messages
        let _ = server.omnipaxos.outgoing_messages();
        server
    }

    pub async fn run(&mut self) {
        // Hack to avoid slow first disk write
        match (self.persist_mode, &self.delay_strat) {
            (PersistMode::NoPersist, _) => (),
            (_, DelayStrategy::FileWrite(_, _)) => self.emulate_storage_delay(1),
            (_, _) => (),
        }

        let buffer_size = 100;
        let mut client_message_buffer = Vec::with_capacity(buffer_size);
        let mut cluster_message_buffer = Vec::with_capacity(buffer_size);
        if self.id == self.initial_leader {
            let mut leader_attempt = 0;
            let mut election_interval = tokio::time::interval(LEADER_WAIT);
            loop {
                tokio::select! {
                    // Ensures cluster is connected and leader is promoted before client starts sending
                    // requests.
                    _ = election_interval.tick() => {
                        self.become_initial_leader(&mut leader_attempt);
                        let (leader_id, phase) = self.omnipaxos.get_current_leader_state();
                        if self.id == leader_id && phase == Phase::Accept {
                            info!("{}: Leader fully initialized", self.id);
                            self.network.send_to_client(self.id, ServerMessage::Ready(self.config.clone().into()));
                            break;
                        }
                    },
                    // Still necessary for sending handshakes
                    _ = self.cluster_messages.recv_many(&mut cluster_message_buffer, buffer_size) => {
                            self.handle_cluster_messages(&mut cluster_message_buffer).await;
                        },
                    _ = self.client_messages.recv_many(&mut client_message_buffer, buffer_size) => {
                        self.handle_client_messages(&mut client_message_buffer);
                    },
                }
            }
        }

        loop {
            let done_signal = tokio::select! {
                _ = self.cluster_messages.recv_many(&mut cluster_message_buffer, buffer_size) => {
                    self.handle_cluster_messages(&mut cluster_message_buffer).await
                },
                _ = self.client_messages.recv_many(&mut client_message_buffer, buffer_size) => {
                    self.handle_client_messages(&mut client_message_buffer)
                },
            };
            if done_signal {
                break;
            }
        }
    }

    fn become_initial_leader(&mut self, leader_attempt: &mut u32) {
        let (_leader, phase) = self.omnipaxos.get_current_leader_state();
        match phase {
            Phase::Accept => {}
            _ => {
                let mut ballot = Ballot::default();
                *leader_attempt += 1;
                ballot.n = *leader_attempt;
                ballot.pid = self.id;
                ballot.config_id = 1;
                info!(
                    "Node: {:?}, Initializing prepare phase with ballot: {:?}",
                    self.id, ballot
                );
                self.omnipaxos.initialize_prepare_phase(ballot);
                self.send_outgoing_msgs();
            }
        }
    }

    fn handle_decided_entries(&mut self) {
        let decided_slots = self.omnipaxos.take_decided_slots_since_last_call();
        for slot in decided_slots {
            let commit_time = Utc::now().timestamp_micros();
            self.request_data[slot].commit = Some(commit_time);
            let decided_entry = self.omnipaxos.read(slot).unwrap();
            match decided_entry {
                LogEntry::Decided(cmd) => self.update_database_and_respond(cmd),
                // TODO: fix slot indexing
                LogEntry::Undecided(cmd) => self.update_database_and_respond(cmd),
                LogEntry::Snapshotted(_) => unimplemented!(),
                _ => unreachable!(),
            }
            let response_time = Utc::now().timestamp_micros();
            self.request_data[slot].sending_response = Some(response_time);
            let acceptor_lat = self.request_data[slot].get_accepted_latencies();
            debug!("Acceptor latencies {:?}", acceptor_lat);
        }
    }

    fn update_database_and_respond(&mut self, command: Command) {
        let read = self.database.handle_command(command.kv_cmd);
        if command.coordinator_id == self.id {
            let response = match read {
                Some(read_result) => ServerMessage::Read(command.id, read_result),
                None => ServerMessage::Write(command.id),
            };
            self.network.send_to_client(command.client_id, response);
            debug!("Sending response {}", command.id);
        }
    }

    fn send_outgoing_msgs(&mut self) {
        let messages = self.omnipaxos.outgoing_messages();
        if self.id == 1 {
            self.instrument_accdec(&messages);
            self.send_outgoing_no_persist(messages);
            return;
        }
        match self.persist_mode {
            PersistMode::NoPersist => self.send_outgoing_no_persist(messages),
            PersistMode::Individual => self.send_outgoing_individual_persist(messages),
            PersistMode::Every(n) => self.send_outgoing_batch_persist(messages, n),
            PersistMode::Opportunistic => self.send_outgoing_opportunistic_persist(messages),
        }
    }

    fn instrument_accdec(&mut self, messages: &Vec<Message<Command>>) {
        for m in messages {
            if let Message::SequencePaxos(PaxosMessage {
                from: _,
                to,
                msg: PaxosMsg::AcceptDecide(accdec),
            }) = m
            {
                let accdec_time = Utc::now().timestamp_micros();
                for offset in 0..accdec.entries.len() {
                    let node_ts = NodeTimestamp {
                        node_id: *to,
                        time: accdec_time,
                    };
                    self.request_data[accdec.start_idx + offset]
                        .send_accdec
                        .push(node_ts);
                }
            }
        }
    }

    fn send_outgoing_no_persist(&mut self, messages: Vec<Message<Command>>) {
        for msg in messages {
            if let Some(slot_idx) = Self::is_accepted_msg(&msg) {
                let accepted_time = Utc::now().timestamp_micros();
                self.acceptor_data[slot_idx].start_persist = Some(accepted_time);
                self.acceptor_data[slot_idx].send_accepted = Some(accepted_time);
            }
            let to = msg.get_receiver();
            let cluster_msg = ClusterMessage::OmniPaxosMessage(msg);
            self.network.send_to_cluster(to, cluster_msg);
        }
    }

    fn send_outgoing_individual_persist(&mut self, messages: Vec<Message<Command>>) {
        for msg in messages {
            if let Some(slot_idx) = Self::is_accepted_msg(&msg) {
                let persist_time = Utc::now().timestamp_micros();
                self.acceptor_data[slot_idx].start_persist = Some(persist_time);
                self.emulate_storage_delay(1);
                let accepted_time = Utc::now().timestamp_micros();
                self.acceptor_data[slot_idx].send_accepted = Some(accepted_time);
            }
            let to = msg.get_receiver();
            let cluster_msg = ClusterMessage::OmniPaxosMessage(msg);
            self.network.send_to_cluster(to, cluster_msg);
        }
    }

    fn send_outgoing_batch_persist(&mut self, messages: Vec<Message<Command>>, batch_size: usize) {
        for msg in messages {
            match Self::is_accepted_msg(&msg) {
                Some(slot_idx) => {
                    let to_node = msg.get_receiver();
                    let cluster_msg = ClusterMessage::OmniPaxosMessage(msg);
                    self.accepted_buffer.push((slot_idx, to_node, cluster_msg));
                    if self.accepted_buffer.len() >= batch_size {
                        let persist_time = Utc::now().timestamp_micros();
                        self.emulate_storage_delay(batch_size);
                        for (accepted_idx, to, accepted_msg) in self.accepted_buffer.drain(..) {
                            let accepted_time = Utc::now().timestamp_micros();
                            self.acceptor_data[accepted_idx].start_persist = Some(persist_time);
                            self.acceptor_data[accepted_idx].send_accepted = Some(accepted_time);
                            self.network.send_to_cluster(to, accepted_msg);
                        }
                    }
                }
                None => {
                    let to = msg.get_receiver();
                    let cluster_msg = ClusterMessage::OmniPaxosMessage(msg);
                    self.network.send_to_cluster(to, cluster_msg);
                }
            }
        }
    }

    fn send_outgoing_opportunistic_persist(&mut self, messages: Vec<Message<Command>>) {
        let num_accepted_msgs = messages
            .iter()
            .filter_map(|m| Self::is_accepted_msg(m))
            .count();
        let persist_time = Utc::now().timestamp_micros();
        if num_accepted_msgs > 0 {
            self.emulate_storage_delay(num_accepted_msgs);
        }
        for msg in messages {
            if let Some(slot_idx) = Self::is_accepted_msg(&msg) {
                let accepted_time = Utc::now().timestamp_micros();
                self.acceptor_data[slot_idx].start_persist = Some(persist_time);
                self.acceptor_data[slot_idx].send_accepted = Some(accepted_time);
            }
            let to = msg.get_receiver();
            let cluster_msg = ClusterMessage::OmniPaxosMessage(msg);
            self.network.send_to_cluster(to, cluster_msg);
        }
    }

    fn is_accepted_msg(message: &Message<Command>) -> Option<usize> {
        match message {
            Message::SequencePaxos(PaxosMessage {
                from: _,
                to: _,
                msg: PaxosMsg::Accepted(a),
            }) if a.slot_idx != usize::MAX => Some(a.slot_idx),
            _ => None,
        }
    }

    fn emulate_storage_delay(&mut self, multiplier: usize) {
        match self.delay_strat {
            DelayStrategy::Sleep(delay) => std::thread::sleep(delay * multiplier as u32),
            DelayStrategy::FileWrite(ref mut file, data_size) => {
                let buffer = vec![b'A'; data_size * multiplier];
                file.write_all(&buffer).expect("Failed to write file");
                file.sync_all().expect("Failed to flush file");
            }
        }
    }

    async fn handle_cluster_messages(&mut self, messages: &mut Vec<(ClusterMessage, i64)>) -> bool {
        let mut server_done = false;
        for (message, net_recv_time) in messages.drain(..) {
            if self.id == 1 {
                match &message {
                    ClusterMessage::OmniPaxosMessage(Message::SequencePaxos(PaxosMessage {
                        from,
                        to: _,
                        msg: PaxosMsg::Accepted(acc),
                    })) => {
                        if acc.slot_idx != usize::MAX {
                            let accepted_time = Utc::now().timestamp_micros();
                            let node_ts = NodeTimestamp {
                                node_id: *from,
                                time: accepted_time,
                            };
                            self.request_data[acc.slot_idx]
                                .receive_accepted
                                .push(node_ts);
                        }
                    }
                    _ => (),
                }
            } else {
                match &message {
                    ClusterMessage::OmniPaxosMessage(Message::SequencePaxos(PaxosMessage {
                        from: _,
                        to: _,
                        msg: PaxosMsg::AcceptDecide(accdec),
                    })) => {
                        for command in &accdec.entries {
                            self.acceptor_data
                                .push(AcceptorInstrumentation::new(command.id, net_recv_time));
                        }
                    }
                    _ => (),
                }
            }
            match message {
                ClusterMessage::OmniPaxosMessage(m) => self.omnipaxos.handle_incoming(m),
                ClusterMessage::Done => {
                    self.debug_acceptor_data().await;
                    server_done = true
                }
            }
        }
        if self.id == self.omnipaxos.get_current_leader().unwrap_or_default() {
            self.handle_decided_entries();
        }
        self.send_outgoing_msgs();
        server_done
    }

    fn handle_client_messages(
        &mut self,
        messages: &mut Vec<(ClientId, ClientMessage, i64)>,
    ) -> bool {
        let mut client_done = false;
        for message in messages.drain(..) {
            match message {
                (client_id, ClientMessage::Append(command_id, kv_command), net_recv_time) => {
                    debug!("Server: Request from client {client_id}: {command_id} {kv_command:?}");
                    let req_data = RequestInstrumentation::with(
                        command_id,
                        net_recv_time,
                        Utc::now().timestamp_micros(),
                    );
                    self.request_data.push(req_data);
                    self.commit_command_to_log(client_id, command_id, kv_command);
                }
                (_, ClientMessage::Done, _) => {
                    self.send_outgoing_msgs();
                    let done_msg = ClusterMessage::Done;
                    for peer in &self.peers {
                        self.network.send_to_cluster(*peer, done_msg.clone());
                    }
                    self.debug_request_data();
                    client_done = true;
                }
            }
        }
        self.send_outgoing_msgs();
        client_done
    }

    fn commit_command_to_log(
        &mut self,
        from: ClientId,
        command_id: CommandId,
        kv_command: KVCommand,
    ) {
        let command = Command {
            client_id: from,
            coordinator_id: self.id,
            id: command_id,
            kv_cmd: kv_command,
        };
        self.omnipaxos
            .append(command)
            .expect("Append to Omnipaxos log failed");
    }

    fn debug_request_data(&mut self) {
        let request_json = serde_json::to_string_pretty(&self.request_data).unwrap();
        print!("{request_json}");

        let latencies: Vec<RequestInstrumentation> = self.request_data.clone();
        let recv_latencies = latencies
            .iter()
            .map(|data| data.channel_receive - data.net_receive)
            .collect();
        let avg_recv_latency = self.get_average(&recv_latencies);
        eprintln!("Net Recv -> Task Recv        {avg_recv_latency:?}");

        let start_accdec_latencies = latencies
            .iter()
            .map(|data| data.send_accdec[0].time - data.channel_receive)
            .collect();
        let avg_start_accdec_latency = self.get_average(&start_accdec_latencies);
        eprintln!("Request -> Start AccDec      {avg_start_accdec_latency:?}");

        let accdec_latencies = latencies
            .iter()
            .map(|data| {
                data.send_accdec[data.send_accdec.len() - 1].time - data.send_accdec[0].time
            })
            .collect();
        let avg_accdec_latency = self.get_average(&accdec_latencies);
        eprintln!("Start AccDec -> Last AccDec  {avg_accdec_latency:?}");

        let accepted_latencies: Vec<Vec<Option<i64>>> = latencies
            .iter()
            .map(|data| data.get_accepted_latencies())
            .collect();
        let avg_accepted_latency = self.get_average_per_acceptor(&accepted_latencies);
        eprintln!("AccDec -> Accepted:");
        for acceptor_latency in &avg_accepted_latency {
            for latency in acceptor_latency {
                eprint!("{latency:<15?}");
            }
            eprintln!();
        }

        let avg_acceptor_delay = Self::get_average_acceptor_delay(&avg_accepted_latency);
        eprintln!("Average Acceptor Delay:");
        for delay in avg_acceptor_delay {
            eprint!("{delay:<15?}");
        }
        eprintln!();

        let commit_latencies = latencies
            .iter()
            .map(|data| data.commit.unwrap() - data.send_accdec[0].time)
            .collect();
        let avg_commit_latency = self.get_average(&commit_latencies);
        eprintln!("Start AccDec -> Commit      {avg_commit_latency:?}");

        let server_total_latencies = latencies
            .iter()
            .map(|data| data.sending_response.unwrap() - data.net_receive)
            .collect();
        let avg_server_total_latency = self.get_average(&server_total_latencies);
        eprintln!("Server Total Latency        {avg_server_total_latency:?}");
    }

    async fn debug_acceptor_data(&self) {
        let acceptor_json = serde_json::to_string_pretty(&self.acceptor_data).unwrap();
        print!("{acceptor_json}");

        let accepted_latencies = self
            .acceptor_data
            .iter()
            .map(|data| match data.send_accepted {
                Some(time) => time - data.net_receive,
                None => 0,
            })
            .collect();
        tokio::time::sleep(Duration::from_millis(50 * self.id)).await;
        let avg_accepted_latency = self.get_average(&accepted_latencies);
        eprintln!(
            "Node {}: Accepted Latency {avg_accepted_latency:?}",
            self.id
        );
    }

    fn get_average(&self, latencies: &Vec<i64>) -> Vec<i64> {
        let batch_count = (latencies.len() / self.batch_size) as i64;
        let mut sums = vec![0; self.batch_size];
        for batch in latencies.chunks(self.batch_size) {
            for (i, latency) in batch.into_iter().enumerate() {
                sums[i] += *latency;
            }
        }
        sums.iter().map(|&sum| (sum / batch_count)).collect()
    }

    fn get_average_per_acceptor(&self, latencies: &Vec<Vec<Option<i64>>>) -> Vec<Vec<i64>> {
        let batch_count = (latencies.len() / self.batch_size) as i64;
        let acceptor_count = latencies[0].len();
        let mut sums = vec![vec![0; acceptor_count]; self.batch_size];
        for batch in latencies.chunks(self.batch_size) {
            for (i, acceptor_latencies) in batch.into_iter().enumerate() {
                for (j, latency) in acceptor_latencies.into_iter().enumerate() {
                    match latency {
                        Some(dur) => sums[i][j] += *dur,
                        None => (),
                    }
                }
            }
        }
        sums.iter()
            .map(|acceptor_sums| {
                acceptor_sums
                    .iter()
                    .map(|sum| (*sum / batch_count))
                    .collect()
            })
            .collect()
    }

    fn get_average_acceptor_delay(acceptor_averages: &Vec<Vec<i64>>) -> Vec<i64> {
        let acceptor_count = acceptor_averages[0].len();
        let batch_count = acceptor_averages.len();

        let mut acceptor_delays = vec![0];
        for acceptor in 1..acceptor_count {
            let mut first_accepted = None;
            let mut last_accepted = None;
            let mut num_delays = 0;
            for batch in 0..batch_count {
                let accepted_latency = acceptor_averages[batch][acceptor];
                if accepted_latency != 0 {
                    num_delays += 1;
                    last_accepted = Some(accepted_latency);
                    if first_accepted.is_none() {
                        first_accepted = Some(accepted_latency);
                    }
                }
            }
            let first_accepted = first_accepted.unwrap();
            let last_accepted = last_accepted.unwrap();
            let diff_accepted = if last_accepted > first_accepted {
                last_accepted - first_accepted
            } else {
                // Can happen when no storage delay
                first_accepted - last_accepted
            };
            let avg_delay_increase = diff_accepted / (num_delays - 1);
            acceptor_delays.push(avg_delay_increase);
        }
        acceptor_delays
    }
}

#[derive(Debug, Clone, Serialize)]
struct NodeTimestamp {
    node_id: u64,
    time: i64,
}

#[derive(Debug, Clone, Serialize)]
struct RequestInstrumentation {
    command_id: CommandId,
    net_receive: i64,
    channel_receive: i64,
    send_accdec: Vec<NodeTimestamp>,
    receive_accepted: Vec<NodeTimestamp>,
    commit: Option<i64>,
    sending_response: Option<i64>,
}

impl RequestInstrumentation {
    fn with(command_id: CommandId, net_receive_time: i64, ch_receive_time: i64) -> Self {
        Self {
            command_id,
            net_receive: net_receive_time,
            channel_receive: ch_receive_time,
            send_accdec: Vec::with_capacity(5),
            receive_accepted: Vec::with_capacity(4),
            commit: None,
            sending_response: None,
        }
    }

    fn get_num_acceptors(&self) -> usize {
        self.send_accdec
            .iter()
            .map(|accdec| accdec.node_id)
            .max()
            .unwrap() as usize
    }

    fn get_accepted_latencies(&self) -> Vec<Option<i64>> {
        let acceptors = self.get_num_acceptors();
        let mut latencies = vec![None; acceptors];
        for node_accepted in &self.receive_accepted {
            let accdec_time = self
                .send_accdec
                .iter()
                .find(|accdec| accdec.node_id == node_accepted.node_id)
                .unwrap()
                .time;
            let acceptor_idx = node_accepted.node_id as usize - 1;
            let latency = node_accepted.time - accdec_time;
            latencies[acceptor_idx] = Some(latency);
        }
        latencies
    }
}

#[derive(Debug, Clone, Serialize)]
struct AcceptorInstrumentation {
    command_id: CommandId,
    net_receive: i64,
    start_persist: Option<i64>,
    send_accepted: Option<i64>,
}

impl AcceptorInstrumentation {
    fn new(command_id: CommandId, net_receive: i64) -> Self {
        AcceptorInstrumentation {
            command_id,
            net_receive,
            start_persist: None,
            send_accepted: None,
        }
    }
}
