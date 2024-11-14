use crate::{database::Database, network::Network};
use chrono::Utc;
use core::panic;
use csv::Writer;
use log::*;
use metronome::common::{
    configs::{OmniPaxosServerConfig, *},
    kv::*,
    messages::*,
};
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
    NoDelay,
    Sleep(Duration),
    FileWrite(File, usize),
}

impl From<DelayConfig> for DelayStrategy {
    fn from(value: DelayConfig) -> Self {
        match value {
            DelayConfig::Sleep(0) => DelayStrategy::NoDelay,
            DelayConfig::Sleep(μs) => DelayStrategy::Sleep(Duration::from_micros(μs)),
            DelayConfig::File(0) => DelayStrategy::NoDelay,
            DelayConfig::File(data_size) => {
                let file = tempfile().expect("Failed to open temp file");
                DelayStrategy::FileWrite(file, data_size)
            }
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum PersistMode {
    Individual,
    Every(usize),
    Opportunistic,
}

impl From<PersistConfig> for PersistMode {
    fn from(value: PersistConfig) -> Self {
        match value {
            PersistConfig::Individual => PersistMode::Individual,
            PersistConfig::Every(n) => PersistMode::Every(n),
            PersistConfig::Opportunistic => PersistMode::Opportunistic,
        }
    }
}

type OmniPaxosInstance = OmniPaxos<Command, MemoryStorage<Command>>;
type TimeStamp = i64;

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
    instrumentation_data: Option<InstrumentationData>,
    config: OmniPaxosServerConfig,
}

impl OmniPaxosServer {
    pub async fn new(
        server_config: OmniPaxosServerConfig,
        omnipaxos_config: OmniPaxosConfig,
        initial_leader: NodeId,
    ) -> Self {
        let server_id = omnipaxos_config.server_config.pid;
        let nodes = omnipaxos_config.cluster_config.nodes.clone();
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
        let instrumentation_data = match server_config.instrumentation {
            true => Some(InstrumentationData::new(server_config.clone())),
            false => None,
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
            persist_mode: server_config.persist_config.into(),
            delay_strat: server_config.delay_config.into(),
            accepted_buffer: vec![],
            instrumentation_data,
            config: server_config,
        };
        // Clears outgoing_messages of initial BLE messages
        let _ = server.omnipaxos.outgoing_messages();
        server
    }

    pub async fn run(&mut self) {
        // Hack to avoid slow first disk write
        if let DelayStrategy::FileWrite(_, _) = &self.delay_strat {
            self.emulate_storage_delay(1);
        }

        // NOTE: This will cap how many messages an Opportunistic flush strategy can batch
        let buffer_size = 1000;
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
                            debug!("Sending ready message to client {}", self.id);
                            self.network.send_to_client(self.id, ServerMessage::Ready(self.config.clone().into()));
                            break;
                        }
                    },
                    // Still necessary for sending handshakes
                    _ = self.cluster_messages.recv_many(&mut cluster_message_buffer, buffer_size) => {
                            self.handle_cluster_messages(&mut cluster_message_buffer);
                        },
                    _ = self.client_messages.recv_many(&mut client_message_buffer, buffer_size) => {
                        self.handle_client_messages(&mut client_message_buffer);
                    },
                }
            }
        }

        // Main event loop
        loop {
            let done_signal = tokio::select! {
                _ = self.cluster_messages.recv_many(&mut cluster_message_buffer, buffer_size) => {
                    self.handle_cluster_messages(&mut cluster_message_buffer)
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

    // We don't use Omnipaxos leader election and instead force an initial leader
    fn become_initial_leader(&mut self, leader_attempt: &mut u32) {
        let (_leader, phase) = self.omnipaxos.get_current_leader_state();
        match phase {
            Phase::Accept => (),
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

    fn handle_cluster_messages(&mut self, messages: &mut Vec<(ClusterMessage, i64)>) -> bool {
        let mut server_done = false;
        self.instrument_cluster_messages(&messages);
        for (message, _net_recv_time) in messages.drain(..) {
            match message {
                ClusterMessage::OmniPaxosMessage(m) => self.omnipaxos.handle_incoming(m),
                ClusterMessage::Done => {
                    info!("{}: Received Done signal from leader", self.id);
                    if let Some(data) = &self.instrumentation_data {
                        data.acceptor_data_to_csv(self.config.debug_filepath.clone())
                            .expect("File write failed");
                        data.debug_acceptor_data();
                    }
                    server_done = true
                }
            }
        }
        if self.id == self.omnipaxos.get_current_leader().unwrap_or_default() {
            self.handle_decided_entries();
        }
        // To minimize latency, rather than send outgoing messages on a timer, just check for any
        // outgoing whenever we could update the state of Omnipaxos.
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
                    if let Some(data) = &mut self.instrumentation_data {
                        data.new_request(command_id, net_recv_time)
                    }
                    self.commit_command_to_log(client_id, command_id, kv_command);
                }
                (_, ClientMessage::Done, _) => {
                    info!("{}: Received Done signal from client", self.id);
                    self.send_outgoing_msgs();
                    let done_msg = ClusterMessage::Done;
                    for peer in &self.peers {
                        self.network.send_to_cluster(*peer, done_msg.clone());
                    }
                    // NOTE: If we exit the program before Done is sent it may be dropped
                    // TODO: Allow for wait on network flushing all pending messages instead
                    std::thread::sleep(Duration::from_secs(1));
                    if let Some(data) = &self.instrumentation_data {
                        data.request_data_to_csv(self.config.debug_filepath.clone())
                            .expect("File write failed");
                        data.debug_request_data();
                    }
                    client_done = true;
                }
            }
        }
        // To minimize latency, rather than send outgoing messages on a timer, just check for any
        // outgoing whenever we could update the state of Omnipaxos.
        self.send_outgoing_msgs();
        client_done
    }

    fn handle_decided_entries(&mut self) {
        let decided_slots = self.omnipaxos.take_decided_slots_since_last_call();
        for slot in decided_slots {
            if let Some(data) = &mut self.instrumentation_data {
                data.commited(slot);
            }
            let decided_entry = self
                .omnipaxos
                .read(slot)
                .expect("Decided slots should be readable");
            match decided_entry {
                LogEntry::Decided(cmd) => self.update_database_and_respond(cmd),
                // Metronome doesn't correctly mark decided entries in storage
                LogEntry::Undecided(cmd) => self.update_database_and_respond(cmd),
                LogEntry::Snapshotted(_) => unimplemented!(),
                _ => unreachable!(),
            }
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
            .expect("Append should never fail since we never reconfig");
    }

    fn send_outgoing_msgs(&mut self) {
        let messages = self.omnipaxos.outgoing_messages();
        if self.id == self.omnipaxos.get_current_leader().unwrap_or_default() {
            // Metronome leader functions as in normal Omnipaxos
            self.instrument_accdec(&messages);
            for msg in messages {
                let to = msg.get_receiver();
                let cluster_msg = ClusterMessage::OmniPaxosMessage(msg);
                self.network.send_to_cluster(to, cluster_msg);
            }
            return;
        }
        // Metronome acceptors handle accepted messages differently
        match self.persist_mode {
            PersistMode::Individual => self.send_outgoing_individual_persist(messages),
            PersistMode::Every(n) => self.send_outgoing_batch_persist(messages, n),
            PersistMode::Opportunistic => self.send_outgoing_opportunistic_persist(messages),
        }
    }

    fn send_outgoing_individual_persist(&mut self, messages: Vec<Message<Command>>) {
        for msg in messages {
            if let Some(slot_idx) = Self::is_accepted_msg(&msg) {
                if let Some(data) = &mut self.instrumentation_data {
                    data.new_persist_start(slot_idx);
                }
                self.emulate_storage_delay(1);
                if let Some(data) = &mut self.instrumentation_data {
                    data.new_persist_end(slot_idx);
                }
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
                            if let Some(data) = &mut self.instrumentation_data {
                                data.new_persist_batch(accepted_idx, persist_time);
                            }
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
                if let Some(data) = &mut self.instrumentation_data {
                    data.new_persist_batch(slot_idx, persist_time)
                }
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
            DelayStrategy::NoDelay => (),
        }
    }

    fn instrument_cluster_messages(&mut self, messages: &Vec<(ClusterMessage, i64)>) {
        match (self.id, &mut self.instrumentation_data) {
            (_, None) => (),
            (1, Some(data)) => {
                for (message, _) in messages {
                    if let ClusterMessage::OmniPaxosMessage(Message::SequencePaxos(
                        PaxosMessage {
                            from,
                            to: _,
                            msg: PaxosMsg::Accepted(acc),
                        },
                    )) = message
                    {
                        if acc.slot_idx != usize::MAX {
                            data.recv_accepted(acc.slot_idx, *from);
                        }
                    }
                }
            }
            (_, Some(data)) => {
                for (message, net_recv_time) in messages {
                    if let ClusterMessage::OmniPaxosMessage(Message::SequencePaxos(
                        PaxosMessage {
                            from: _,
                            to: _,
                            msg: PaxosMsg::AcceptDecide(accdec),
                        },
                    )) = message
                    {
                        for command in &accdec.entries {
                            data.recv_accdec(command.id, *net_recv_time);
                        }
                    }
                }
            }
        }
    }

    fn instrument_accdec(&mut self, messages: &Vec<Message<Command>>) {
        if let Some(data) = &mut self.instrumentation_data {
            for m in messages {
                if let Message::SequencePaxos(PaxosMessage {
                    from: _,
                    to,
                    msg: PaxosMsg::AcceptDecide(accdec),
                }) = m
                {
                    data.send_accdec(accdec.start_idx, accdec.entries.len(), *to);
                }
            }
        }
    }
}

struct InstrumentationData {
    request_data: Vec<RequestInstrumentation>,
    acceptor_data: Vec<AcceptorInstrumentation>,
    metronome_batch_size: usize,
    id: NodeId,
}

impl InstrumentationData {
    fn new(config: OmniPaxosServerConfig) -> Self {
        let num_nodes = config.cluster_config.nodes.len();
        let metronome_quorum = match config.cluster_config.metronome_quorum_size {
            Some(quorum_size) => quorum_size,
            None => (num_nodes / 2) + 1,
        };
        InstrumentationData {
            request_data: Vec::with_capacity(1000),
            acceptor_data: Vec::with_capacity(1000),
            metronome_batch_size: n_choose_k(num_nodes, metronome_quorum),
            id: config.server_config.pid,
        }
    }

    #[inline]
    fn new_request(&mut self, command_id: CommandId, net_recv_time: TimeStamp) {
        let recv_request_time = Utc::now().timestamp_micros();
        let request_data = RequestInstrumentation {
            command_id,
            net_receive: net_recv_time,
            channel_receive: recv_request_time,
            send_accdec: Vec::with_capacity(5),
            receive_accepted: Vec::with_capacity(4),
            commit: None,
        };
        self.request_data.push(request_data);
    }

    #[inline]
    fn send_accdec(&mut self, start_idx: usize, num_entries: usize, to: NodeId) {
        let accdec_time = Utc::now().timestamp_micros();
        for offset in 0..num_entries {
            let node_ts = NodeTimestamp {
                node_id: to,
                time: accdec_time,
            };
            self.request_data[start_idx + offset]
                .send_accdec
                .push(node_ts);
        }
    }

    #[inline]
    fn recv_accepted(&mut self, slot_idx: usize, from: NodeId) {
        let recv_accepted_time = Utc::now().timestamp_micros();
        let node_ts = NodeTimestamp {
            node_id: from,
            time: recv_accepted_time,
        };
        self.request_data[slot_idx].receive_accepted.push(node_ts);
    }

    #[inline]
    fn commited(&mut self, slot_idx: usize) {
        let commit_time = Utc::now().timestamp_micros();
        self.request_data[slot_idx].commit = Some(commit_time);
    }

    // ===== Acceptor data =====
    #[inline]
    fn recv_accdec(&mut self, command_id: CommandId, net_recv_time: TimeStamp) {
        let accept_data = AcceptorInstrumentation {
            command_id,
            net_receive: net_recv_time,
            start_persist: None,
            send_accepted: None,
        };
        self.acceptor_data.push(accept_data);
    }

    #[inline]
    fn new_persist_start(&mut self, slot_idx: usize) {
        let persist_time = Utc::now().timestamp_micros();
        self.acceptor_data[slot_idx].start_persist = Some(persist_time);
    }

    #[inline]
    fn new_persist_end(&mut self, slot_idx: usize) {
        let accepted_time = Utc::now().timestamp_micros();
        self.acceptor_data[slot_idx].send_accepted = Some(accepted_time);
    }

    #[inline]
    fn new_persist_batch(&mut self, slot_idx: usize, batch_start: TimeStamp) {
        let accepted_time = Utc::now().timestamp_micros();
        self.acceptor_data[slot_idx].start_persist = Some(batch_start);
        self.acceptor_data[slot_idx].send_accepted = Some(accepted_time);
    }

    fn acceptor_data_to_csv(&self, file_path: String) -> Result<(), std::io::Error> {
        let file = File::create(file_path)?;
        let mut writer = Writer::from_writer(file);
        for accepted_data in &self.acceptor_data {
            writer.serialize(accepted_data)?;
        }
        writer.flush()?;
        Ok(())
    }

    fn request_data_to_csv(&self, file_path: String) -> Result<(), std::io::Error> {
        let file = File::create(file_path)?;
        let mut writer = Writer::from_writer(file);
        for request_data in &self.request_data {
            writer.serialize(request_data)?;
        }
        writer.flush()?;
        Ok(())
    }

    fn debug_request_data(&self) {
        let recv_lat = self.avg_recv_latency();
        let start_accdec_lat = self.avg_start_accdec_latency();
        let send_accdec_lat = self.avg_accdec_latency();
        let commit_lat = self.avg_commit_latency();
        eprintln!("Net Recv -> Task Recv        {recv_lat:?}");
        eprintln!("Request -> Start AccDec      {start_accdec_lat:?}");
        eprintln!("Start AccDec -> Last AccDec  {send_accdec_lat:?}");
        eprintln!("AccDec -> Accepted:");
        let avg_accepted_latency = self.avg_accepted_latency();
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
        eprintln!("Start AccDec -> Commit      {commit_lat:?}");
    }

    fn debug_acceptor_data(&self) {
        let accepted_latencies = self
            .acceptor_data
            .iter()
            .map(|data| match data.send_accepted {
                Some(time) => time - data.net_receive,
                None => 0,
            })
            .collect();
        let avg_accepted_latency = self.get_batch_average(&accepted_latencies);
        let id = self.id;
        std::thread::sleep(Duration::from_secs(1 * id));
        eprintln!("Node {id}: Accepted Latency {avg_accepted_latency:?}");
    }

    fn avg_recv_latency(&self) -> Vec<i64> {
        let recv_latencies = self
            .request_data
            .iter()
            .map(|data| data.channel_receive - data.net_receive)
            .collect();
        self.get_batch_average(&recv_latencies)
    }

    fn avg_start_accdec_latency(&self) -> Vec<i64> {
        let start_accdec_latencies = self
            .request_data
            .iter()
            .map(|data| data.send_accdec[0].time - data.channel_receive)
            .collect();
        self.get_batch_average(&start_accdec_latencies)
    }

    fn avg_accdec_latency(&self) -> Vec<i64> {
        let accdec_latencies = self
            .request_data
            .iter()
            .map(|data| {
                data.send_accdec[data.send_accdec.len() - 1].time - data.send_accdec[0].time
            })
            .collect();
        self.get_batch_average(&accdec_latencies)
    }

    fn avg_commit_latency(&self) -> Vec<i64> {
        let commit_latencies = self
            .request_data
            .iter()
            .filter_map(|data| data.commit.map(|t| t - data.send_accdec[0].time))
            .collect();
        self.get_batch_average(&commit_latencies)
    }

    fn avg_accepted_latency(&self) -> Vec<Vec<i64>> {
        let accepted_latencies: Vec<Vec<Option<i64>>> = self
            .request_data
            .iter()
            .map(|data| data.get_accepted_latencies())
            .collect();
        self.get_average_per_acceptor(&accepted_latencies)
    }

    fn get_batch_average(&self, latencies: &Vec<i64>) -> Vec<i64> {
        let batch_size = self.metronome_batch_size;
        let batch_count = (latencies.len() / batch_size) as i64;
        let mut sums = vec![0; batch_size];
        for batch in latencies.chunks(batch_size) {
            for (i, latency) in batch.into_iter().enumerate() {
                sums[i] += *latency;
            }
        }
        sums.iter().map(|&sum| (sum / batch_count)).collect()
    }

    fn get_average_per_acceptor(&self, latencies: &Vec<Vec<Option<i64>>>) -> Vec<Vec<i64>> {
        let batch_size = self.metronome_batch_size;
        let batch_count = (latencies.len() / batch_size) as i64;
        let acceptor_count = latencies[0].len();
        let mut sums = vec![vec![0; acceptor_count]; batch_size];
        for batch in latencies.chunks(batch_size) {
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
struct AcceptorInstrumentation {
    command_id: CommandId,
    net_receive: i64,
    start_persist: Option<i64>,
    send_accepted: Option<i64>,
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
    #[serde(skip_serializing)]
    send_accdec: Vec<NodeTimestamp>,
    #[serde(skip_serializing)]
    receive_accepted: Vec<NodeTimestamp>,
    commit: Option<i64>,
}

impl RequestInstrumentation {
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
