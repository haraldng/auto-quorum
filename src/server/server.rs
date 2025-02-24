use crate::{configs::*, database::Database, network::Network};
use chrono::Utc;
use core::panic;
use csv::Writer;
use log::*;
use metronome::common::{kv::*, messages::*};
use omnipaxos::{
    ballot_leader_election::Ballot,
    messages::{
        sequence_paxos::{PaxosMessage, PaxosMsg},
        Message,
    },
    sequence_paxos::{leader::ACCEPTSYNC_MAGIC_SLOT, Phase},
    util::NodeId,
    OmniPaxos, OmniPaxosConfig,
};
use omnipaxos_storage::memory_storage::MemoryStorage;
use serde::Serialize;
use std::{fs::File, os::unix::fs::FileExt, time::Duration};

const LEADER_WAIT: Duration = Duration::from_secs(1);
const INITIAL_LOG_CAPACITY: usize = 10_000_000;
const COMPACT_LIMIT: usize = 9_000_000;
const COMPACT_RETAIN_SIZE: usize = 500_000;
// NOTE: This will cap how many messages an Opportunistic flush strategy can batch
const NETWORK_BATCH_SIZE: usize = 20_000;

type OmniPaxosInstance = OmniPaxos<Command, MemoryStorage<Command>>;
type TimeStamp = i64;

#[derive(Debug)]
pub(crate) enum PersistStrategy {
    NoPersist,
    FileWrite(File, usize),
}

impl PersistStrategy {
    fn from(config: PersistConfig, path: String) -> Self {
        match config {
            PersistConfig::NoPersist => PersistStrategy::NoPersist,
            PersistConfig::File(0) => PersistStrategy::NoPersist,
            PersistConfig::File(data_size) => {
                let file = File::options()
                    .write(true)
                    .append(false)
                    .truncate(true)
                    .create(true)
                    .open(path)
                    .expect("Failed to open file");
                PersistStrategy::FileWrite(file, data_size)
            }
        }
    }
}

pub struct MetronomeServer {
    id: NodeId,
    peers: Vec<NodeId>,
    database: Database,
    network: Network,
    omnipaxos: OmniPaxosInstance,
    initial_leader: NodeId,
    persist_strat: PersistStrategy,
    decided_slots_buffer: Vec<usize>,
    instrumentation_data: Option<InstrumentationData>,
    config: MetronomeConfig,
}

impl MetronomeServer {
    pub async fn new(config: MetronomeConfig) -> Self {
        let server_id = config.server.server_id;
        let nodes = config.cluster.nodes.clone();
        let peers: Vec<u64> = nodes
            .into_iter()
            .filter(|node| *node != server_id)
            .collect();
        info!(
            "Node: {:?}: using metronome: {:?}, batch_config: {:?}",
            server_id, config.cluster.metronome_config, config.cluster.batch_config
        );
        let storage = MemoryStorage::with_capacity(INITIAL_LOG_CAPACITY);
        let omnipaxos_config: OmniPaxosConfig = config.clone().into();
        let omnipaxos = omnipaxos_config.build(storage).unwrap();
        let initial_leader = config.cluster.initial_leader.unwrap_or(1);
        let initial_clients = match server_id == initial_leader {
            true => 1,
            false => 0,
        };
        let network = Network::new(config.clone(), initial_clients, NETWORK_BATCH_SIZE).await;
        let instrumentation_data = match config.server.instrumentation {
            true => Some(InstrumentationData::new(config.clone())),
            false => None,
        };
        let persist_strat = PersistStrategy::from(
            config.cluster.persist_config,
            config.server.persist_log_filepath.clone(),
        );
        MetronomeServer {
            id: server_id,
            peers,
            database: Database::new(),
            network,
            omnipaxos,
            initial_leader,
            persist_strat,
            decided_slots_buffer: Vec::with_capacity(1000),
            instrumentation_data,
            config,
        }
    }

    pub async fn run(&mut self) {
        let mut client_msg_buffer = Vec::with_capacity(NETWORK_BATCH_SIZE);
        let mut cluster_msg_buffer = Vec::with_capacity(NETWORK_BATCH_SIZE);
        if self.id == self.initial_leader {
            self.become_initial_leader(&mut cluster_msg_buffer, &mut client_msg_buffer)
                .await;
        }
        // Main event loop
        loop {
            let done_signal = tokio::select! {
                _ = self.network.cluster_messages.recv_many(&mut cluster_msg_buffer, NETWORK_BATCH_SIZE) => {
                    self.handle_cluster_messages(&mut cluster_msg_buffer).await
                },
                _ = self.network.client_messages.recv_many(&mut client_msg_buffer, NETWORK_BATCH_SIZE) => {
                    self.handle_client_messages(&mut client_msg_buffer).await
                },
            };
            if done_signal {
                break;
            }
        }
    }

    // We don't use Omnipaxos leader election and instead force an initial leader
    async fn become_initial_leader(
        &mut self,
        cluster_msg_buffer: &mut Vec<(ClusterMessage, TimeStamp)>,
        client_msg_buffer: &mut Vec<(ClientId, ClientMessage, TimeStamp)>,
    ) {
        let mut leader_attempt = 0;
        let mut leader_takeover_interval = tokio::time::interval(LEADER_WAIT);
        loop {
            tokio::select! {
                // Ensures cluster is connected and leader is promoted before client starts sending
                // requests.
                _ = leader_takeover_interval.tick() => {
                    self.take_leadership(&mut leader_attempt).await;
                    let (leader_id, phase) = self.omnipaxos.get_current_leader_state();
                    if self.id == leader_id && phase == Phase::Accept {
                        info!("{}: Leader fully initialized", self.id);
                        debug!("Sending ready message to client {}", self.id);
                        self.network.send_to_client(self.id, ServerMessage::Ready(self.config.clone().into())).await;
                        break;
                    }
                },
                // Still necessary for network handshakes
                _ = self.network.cluster_messages.recv_many(cluster_msg_buffer, NETWORK_BATCH_SIZE) => {
                    self.handle_cluster_messages(cluster_msg_buffer).await;
                },
                _ = self.network.client_messages.recv_many(client_msg_buffer, NETWORK_BATCH_SIZE) => {
                    self.handle_client_messages(client_msg_buffer).await;
                },
            }
        }
    }

    async fn take_leadership(&mut self, leader_attempt: &mut u32) {
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
                self.send_outgoing_msgs().await;
            }
        }
    }

    async fn handle_cluster_messages(&mut self, messages: &mut Vec<(ClusterMessage, i64)>) -> bool {
        for (message, net_recv_time) in messages.drain(..) {
            match message {
                ClusterMessage::OmniPaxosMessage(m) => {
                    self.instrument_incoming_paxos_message(&m, net_recv_time);
                    self.omnipaxos.handle_incoming(m);
                }
                ClusterMessage::Done => {
                    info!("{}: Received Done signal from leader", self.id);
                    if let Some(data) = &self.instrumentation_data {
                        data.acceptor_data_to_csv(self.config.server.debug_filename.clone())
                            .expect("File write failed");
                        data.debug_acceptor_data();
                    }
                    return true;
                }
            }
        }
        if self.id == self.omnipaxos.get_current_leader().unwrap_or_default() {
            self.handle_decided_entries().await;
        }
        // To minimize latency, rather than send outgoing messages on a timer, just check for any
        // outgoing whenever we could have updated the state of Omnipaxos.
        self.send_outgoing_msgs().await;
        return false;
    }

    async fn handle_client_messages(
        &mut self,
        messages: &mut Vec<(ClientId, ClientMessage, i64)>,
    ) -> bool {
        for message in messages.drain(..) {
            match message {
                (client_id, ClientMessage::Append(command_id, kv_command), net_recv_time) => {
                    debug!("Server: Request from client {client_id}: {command_id} {kv_command:?}");
                    if let Some(data) = &mut self.instrumentation_data {
                        data.new_request(command_id, net_recv_time)
                    }
                    self.commit_command_to_log(client_id, command_id, kv_command);
                }
                (client_id, ClientMessage::BatchAppend(appends), net_recv_time) => {
                    debug!("Server: BatchRequest from client {client_id}: {appends:?}");
                    for (command_id, kv_command) in appends {
                        if let Some(data) = &mut self.instrumentation_data {
                            data.new_request(command_id, net_recv_time)
                        }
                        self.commit_command_to_log(client_id, command_id, kv_command);
                    }
                }
                (_, ClientMessage::Done, _) => {
                    info!("{}: Received Done signal from client", self.id);
                    let done_msg = ClusterMessage::Done;
                    for peer in &self.peers {
                        self.network.send_to_cluster(*peer, done_msg.clone()).await;
                    }
                    // NOTE: If we exit the program before Done is sent it may be dropped
                    // TODO: Allow for wait on network flushing all pending messages instead
                    std::thread::sleep(Duration::from_secs(1));
                    if let Some(data) = &self.instrumentation_data {
                        data.request_data_to_csv(self.config.server.debug_filename.clone())
                            .expect("File write failed");
                        //data.debug_request_data();
                    }
                    return true;
                }
            }
        }
        // To minimize latency, rather than send outgoing messages on a timer, just check for any
        // outgoing whenever we could have updated the state of Omnipaxos.
        self.send_outgoing_msgs().await;
        return false;
    }

    async fn handle_decided_entries(&mut self) {
        self.omnipaxos
            .take_decided_slots_since_last_call(&mut self.decided_slots_buffer);
        if let Some(data) = &mut self.instrumentation_data {
            for slot in &self.decided_slots_buffer {
                data.commited(*slot);
            }
        }
        if !self.decided_slots_buffer.is_empty() {
            let last_dec_slot = self.decided_slots_buffer[self.decided_slots_buffer.len() - 1];
            let in_mem_log_length = last_dec_slot - self.omnipaxos.get_compacted_idx();
            if in_mem_log_length > COMPACT_LIMIT {
                let compact_idx = last_dec_slot - COMPACT_RETAIN_SIZE;
                match self.omnipaxos.trim(Some(compact_idx)) {
                    Ok(_) => info!("Compacted in-memory log at idx {compact_idx}"),
                    Err(e) => panic!("Failed compaction at idx {compact_idx}: {e:?}"),
                }
            }
        }
        for slot in self.decided_slots_buffer.drain(..) {
            let command = self.omnipaxos.read_raw(slot);
            let read = self.database.handle_command(command.kv_cmd);
            if command.coordinator_id == self.id {
                let response = match read {
                    Some(read_result) => ServerMessage::Read(command.id, read_result),
                    None => ServerMessage::Write(command.id),
                };
                self.network
                    .send_to_client(command.client_id, response)
                    .await;
                debug!("Sending response {}", command.id);
            }
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

    async fn send_outgoing_msgs(&mut self) {
        if self.id == self.initial_leader {
            self.send_outgoing_msgs_leader().await;
        } else {
            self.send_outgoing_msgs_follower().await;
        }
    }

    // Metronome leader functions as in normal Omnipaxos
    async fn send_outgoing_msgs_leader(&mut self) {
        let messages = self.omnipaxos.outgoing_messages();
        self.instrument_accdec(&messages);
        for msg in messages {
            let to = msg.get_receiver();
            let cluster_msg = ClusterMessage::OmniPaxosMessage(msg);
            self.network.send_to_cluster(to, cluster_msg).await;
        }
    }

    // Metronome acceptors handle accepted messages differently
    async fn send_outgoing_msgs_follower(&mut self) {
        let messages = self.omnipaxos.outgoing_messages();
        for msg in messages {
            if let Some(to_flush) = msg.get_accepted_slots() {
                self.emulate_disk_flush(to_flush);
            }
            let to = msg.get_receiver();
            let cluster_msg = ClusterMessage::OmniPaxosMessage(msg);
            self.network.send_to_cluster(to, cluster_msg).await;
        }
    }

    fn emulate_disk_flush(&mut self, to_flush: &Vec<usize>) {
        match self.persist_strat {
            PersistStrategy::NoPersist => {
                if let Some(data) = &mut self.instrumentation_data {
                    data.persist_start(to_flush);
                    data.persist_end(to_flush);
                }
            }
            PersistStrategy::FileWrite(ref mut file, entry_size) => {
                if let Some(data) = &mut self.instrumentation_data {
                    data.persist_start(to_flush);
                }
                let buffer = vec![b'A'; entry_size * to_flush.len()];
                file.write_all_at(&buffer, 0).expect("Failed to write file");
                file.sync_data().expect("Failed to flush file");
                if let Some(data) = &mut self.instrumentation_data {
                    data.persist_end(to_flush);
                }
            }
        }
    }

    fn instrument_incoming_paxos_message(
        &mut self,
        message: &Message<Command>,
        net_recv_time: TimeStamp,
    ) {
        if let Some(data) = &mut self.instrumentation_data {
            if self.id == self.initial_leader {
                // Leader instruments Accepted messages
                if let Some(accepted_slots) = message.get_accepted_slots() {
                    let from = message.get_sender();
                    data.recv_accepted(accepted_slots, from);
                }
            } else {
                // Followers instrument AcceptDecide Messages
                if let Message::SequencePaxos(PaxosMessage {
                    from: _,
                    to: _,
                    msg: PaxosMsg::AcceptDecide(accdec),
                }) = message
                {
                    data.recv_accdec(&accdec.entries, net_recv_time)
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
    fn new(config: MetronomeConfig) -> Self {
        let num_nodes = config.cluster.nodes.len();
        let metronome_quorum = match config.cluster.metronome_quorum_size {
            Some(quorum_size) => quorum_size,
            None => (num_nodes / 2) + 1,
        };
        InstrumentationData {
            request_data: Vec::with_capacity(1000),
            acceptor_data: Vec::with_capacity(1000),
            metronome_batch_size: n_choose_k(num_nodes, metronome_quorum),
            id: config.server.server_id,
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
    fn recv_accepted(&mut self, accepted_slots: &Vec<usize>, from: NodeId) {
        let recv_accepted_time = Utc::now().timestamp_micros();
        let node_ts = NodeTimestamp {
            node_id: from,
            time: recv_accepted_time,
        };
        if accepted_slots[0] == ACCEPTSYNC_MAGIC_SLOT {
            return;
        }
        for slot_idx in accepted_slots {
            self.request_data[*slot_idx].receive_accepted.push(node_ts);
        }
    }

    #[inline]
    fn commited(&mut self, slot_idx: usize) {
        let commit_time = Utc::now().timestamp_micros();
        self.request_data[slot_idx].commit = Some(commit_time);
    }

    // ===== Acceptor data =====
    #[inline]
    fn recv_accdec(&mut self, entries: &Vec<Command>, net_recv_time: TimeStamp) {
        for command in entries {
            let accept_data = AcceptorInstrumentation {
                command_id: command.id,
                net_receive: net_recv_time,
                start_persist: None,
                send_accepted: None,
            };
            self.acceptor_data.push(accept_data);
        }
    }

    #[inline]
    fn persist_start(&mut self, slots: &Vec<usize>) {
        let persist_time = Utc::now().timestamp_micros();
        if slots[0] == ACCEPTSYNC_MAGIC_SLOT {
            return;
        }
        for slot_idx in slots {
            self.acceptor_data[*slot_idx].start_persist = Some(persist_time);
        }
    }

    #[inline]
    fn persist_end(&mut self, slots: &Vec<usize>) {
        let accepted_time = Utc::now().timestamp_micros();
        if slots[0] == ACCEPTSYNC_MAGIC_SLOT {
            return;
        }
        for slot_idx in slots {
            self.acceptor_data[*slot_idx].send_accepted = Some(accepted_time);
        }
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
            .filter_map(|data| {
                if let Some(first_accdec) = data.send_accdec.get(0) {
                    Some(first_accdec.time - data.channel_receive)
                } else {
                    None
                }
            })
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

#[derive(Debug, Clone, Copy, Serialize)]
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
