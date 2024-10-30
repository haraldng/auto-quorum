use core::panic;
use log::*;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::Write;
use std::time::Duration;
use tempfile::tempfile;
use tokio::sync::mpsc::Receiver;
use tokio::time::Instant;

use omnipaxos::ballot_leader_election::Ballot;
use omnipaxos::messages::sequence_paxos::{PaxosMessage, PaxosMsg};
use omnipaxos::messages::Message;
use omnipaxos::sequence_paxos::Phase;
use omnipaxos::{
    util::{LogEntry, NodeId},
    OmniPaxos, OmniPaxosConfig,
};
use omnipaxos_storage::memory_storage::MemoryStorage;

use crate::{database::Database, network::Network};
use common::{kv::*, messages::*};

const LEADER_WAIT: Duration = Duration::from_secs(1);

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OmniPaxosServerConfig {
    pub cluster_name: String,
    pub location: String,
    pub initial_leader: Option<NodeId>,
    pub local_deployment: Option<bool>,
    pub storage_duration_micros: Option<u64>,
    pub data_size: Option<usize>,
}

type OmniPaxosInstance = OmniPaxos<Command, MemoryStorage<Command>>;

enum DelayStrategy {
    NoDelay,
    Sleep(Duration),
    FileWrite(File, usize),
}

#[derive(Debug, Clone)]
struct RequestInstrumentation {
    net_receive: Instant,
    channel_receive: Instant,
    send_accdec: Vec<(NodeId, Instant)>,
    receive_accepted: Vec<(NodeId, Instant)>,
    commit: Option<Instant>,
    sending_response: Option<Instant>,
}

impl RequestInstrumentation {
    fn with(net_receive_time: Instant, ch_receive_time: Instant) -> Self {
        Self {
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
            .map(|(node, _)| *node)
            .max()
            .unwrap() as usize
    }

    fn get_accepted_latencies(&self) -> Vec<Option<Duration>> {
        let acceptors = self.get_num_acceptors();
        let mut latencies = vec![None; acceptors];
        for (acceptor, accepted_time) in &self.receive_accepted {
            let accdec_time = self
                .send_accdec
                .iter()
                .find(|(to, _accdec_time)| *to == *acceptor)
                .unwrap()
                .1;
            let acceptor_idx = *acceptor as usize - 1;
            let latency = *accepted_time - accdec_time;
            latencies[acceptor_idx] = Some(latency);
        }
        latencies
    }
}

#[derive(Debug, Clone)]
struct AcceptorInstrumentation {
    // net_receive: Instant,
    // channel_receive: Instant,
    receive_accdec: Instant,
    send_accepted: Option<Instant>,
}

impl AcceptorInstrumentation {
    fn new(propose_time: Instant) -> Self {
        AcceptorInstrumentation {
            receive_accdec: propose_time,
            send_accepted: None,
        }
    }
}

pub struct OmniPaxosServer {
    id: NodeId,
    peers: Vec<NodeId>,
    database: Database,
    network: Network,
    cluster_messages: Receiver<ClusterMessage>,
    client_messages: Receiver<(ClientId, ClientMessage, Instant)>,
    omnipaxos: OmniPaxosInstance,
    initial_leader: NodeId,
    leader_attempt: u32,
    delay_strategy: DelayStrategy,
    batch_size: usize,
    use_metronome: bool,
    request_data: Vec<Option<RequestInstrumentation>>,
    acceptor_data: Vec<AcceptorInstrumentation>,
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
        let delay_strategy = match (
            server_config.data_size,
            server_config.storage_duration_micros,
        ) {
            (Some(size), _) if size > 0 => {
                let file = tempfile().expect("Failed to open temp file");
                DelayStrategy::FileWrite(file, size)
            }
            (_, Some(0)) => DelayStrategy::NoDelay,
            (_, Some(micros)) => DelayStrategy::Sleep(Duration::from_micros(micros)),
            (_, None) => panic!(
                "Either data_size or storage_duration_micros configuration fields must be set."
            ),
        };
        let storage = MemoryStorage::default();
        info!(
            "Node: {:?}: using metronome: {}, storage_duration: {:?}",
            server_id,
            omnipaxos_config.cluster_config.use_metronome,
            server_config.storage_duration_micros
        );
        // TODO:
        let use_metronome = match omnipaxos_config.cluster_config.use_metronome {
            0 => false,
            2 => true,
            _ => unreachable!(),
        };
        let omnipaxos = omnipaxos_config.build(storage).unwrap();
        let initial_clients = match server_id == initial_leader {
            true => 1,
            false => 0,
        };
        let (cluster_message_sender, cluster_messages) = tokio::sync::mpsc::channel(1000);
        let (client_message_sender, client_messages) = tokio::sync::mpsc::channel(1000);
        let network = Network::new(
            server_config.cluster_name,
            server_id,
            peers.clone(),
            initial_clients,
            local_deployment,
            client_message_sender,
            cluster_message_sender,
        )
        .await
        .unwrap();
        let mut server = OmniPaxosServer {
            id: server_id,
            peers,
            database: Database::new(),
            network,
            cluster_messages,
            client_messages,
            omnipaxos,
            initial_leader,
            leader_attempt: 0,
            delay_strategy,
            batch_size,
            use_metronome,
            request_data: Vec::with_capacity(1000),
            acceptor_data: Vec::with_capacity(1000),
        };
        // Clears outgoing_messages of initial BLE messages
        let _ = server.omnipaxos.outgoing_messages();
        server
    }

    pub async fn run(&mut self) {
        let buffer_size = 100;
        let mut client_message_buffer = Vec::with_capacity(buffer_size);
        let mut cluster_message_buffer = Vec::with_capacity(buffer_size);
        if self.id == self.initial_leader {
            let mut election_interval = tokio::time::interval(LEADER_WAIT);
            loop {
                tokio::select! {
                    // Ensures cluster is connected and leader is promoted before client starts sending
                    // requests.
                    _ = election_interval.tick() => {
                        self.become_initial_leader();
                        let (leader_id, phase) = self.omnipaxos.get_current_leader_state();
                        if self.id == leader_id && phase == Phase::Accept {
                            info!("{}: Leader fully initialized", self.id);
                            self.network.send_to_client(self.id, ServerMessage::Ready);
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
                    // let num_messages = cluster_message_buffer.len();
                    // let now = Instant::now();
                    let done = self.handle_cluster_messages(&mut cluster_message_buffer).await;
                    // let process_time = now.elapsed();
                    // if process_time > Duration::from_micros(800) {
                    //     eprintln!("Handle {num_messages} cluster messages time {process_time:?}");
                    // }
                    done
                },
                _ = self.client_messages.recv_many(&mut client_message_buffer, buffer_size) => {
                    // let num_messages = client_message_buffer.len();
                    // let now = Instant::now();
                    let done = self.handle_client_messages(&mut client_message_buffer);
                    // let process_time = now.elapsed();
                    // if process_time > Duration::from_micros(800) {
                    //     eprintln!("Handle {num_messages} client messages time {process_time:?}");
                    // }
                    done
                },
            };
            if done_signal {
                break;
            }
        }
    }

    fn become_initial_leader(&mut self) {
        let (_leader, phase) = self.omnipaxos.get_current_leader_state();
        match phase {
            Phase::Accept => {}
            _ => {
                let mut ballot = Ballot::default();
                self.leader_attempt += 1;
                ballot.n = self.leader_attempt;
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
            let commit_time = Instant::now();
            // eprintln!("Committing {slot}");
            self.request_data[slot].as_mut().unwrap().commit = Some(commit_time);
            let decided_entry = self.omnipaxos.read(slot).unwrap();
            match decided_entry {
                LogEntry::Decided(cmd) => self.update_database_and_respond(cmd),
                // TODO: fix slot indexing
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
            let response_time = Instant::now();
            // eprintln!("Sending response {}", command.id);
            self.request_data[command.id]
                .as_mut()
                .unwrap()
                .sending_response = Some(response_time);
        }
    }

    fn send_outgoing_msgs(&mut self) {
        let messages = self.omnipaxos.outgoing_messages();
        if self.id == 1 {
            for m in &messages {
                match m {
                    Message::SequencePaxos(PaxosMessage {
                        from: _,
                        to,
                        msg: PaxosMsg::AcceptDecide(accdec),
                    }) => {
                        let accdec_time = Instant::now();
                        for entry in &accdec.entries {
                            // eprintln!("Sending Acceptdecide {}", entry.id);
                            self.request_data[entry.id]
                                .as_mut()
                                .unwrap()
                                .send_accdec
                                .push((*to, accdec_time));
                        }
                    }
                    _ => (),
                }
            }
        }
        for msg in messages {
            let to = msg.get_receiver();
            let cluster_msg = ClusterMessage::OmniPaxosMessage(msg);
            match &cluster_msg {
                ClusterMessage::OmniPaxosMessage(Message::SequencePaxos(PaxosMessage {
                    from: _,
                    to: _,
                    msg: PaxosMsg::Accepted(a),
                })) => {
                    match &mut self.delay_strategy {
                        DelayStrategy::Sleep(delay) => std::thread::sleep(*delay),
                        DelayStrategy::FileWrite(ref mut file, data_size) => {
                            let buffer = vec![b'A'; *data_size];
                            file.write_all(&buffer).expect("Failed to write file");
                            file.sync_all().expect("Failed to flush file");
                        }
                        DelayStrategy::NoDelay => (),
                    }
                    if a.slot_idx != usize::MAX {
                        let accepted_time = Instant::now();
                        self.acceptor_data[a.slot_idx].send_accepted = Some(accepted_time);
                    }
                    self.network.send_to_cluster(to, cluster_msg);
                }
                _ => self.network.send_to_cluster(to, cluster_msg),
            }
        }
    }

    async fn handle_cluster_messages(&mut self, messages: &mut Vec<ClusterMessage>) -> bool {
        let mut server_done = false;
        for message in messages.drain(..) {
            if self.id == 1 {
                match &message {
                    ClusterMessage::OmniPaxosMessage(Message::SequencePaxos(PaxosMessage {
                        from,
                        to: _,
                        msg: PaxosMsg::Accepted(acc),
                    })) => {
                        // eprintln!("Got accepted from {from}: {}", acc.slot_idx);
                        if acc.slot_idx != usize::MAX {
                            let accepted_time = Instant::now();
                            self.request_data[acc.slot_idx]
                                .as_mut()
                                .unwrap()
                                .receive_accepted
                                .push((*from, accepted_time));
                        }
                    }
                    _ => (),
                }
            } else {
                match &message {
                    ClusterMessage::OmniPaxosMessage(Message::SequencePaxos(PaxosMessage {
                        from: _,
                        to: _,
                        msg: PaxosMsg::AcceptDecide(_),
                    })) => {
                        let propose_time = Instant::now();
                        self.acceptor_data
                            .push(AcceptorInstrumentation::new(propose_time));
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
        messages: &mut Vec<(ClientId, ClientMessage, Instant)>,
    ) -> bool {
        let mut client_done = false;
        for message in messages.drain(..) {
            match message {
                (client_id, ClientMessage::Append(command_id, kv_command), net_recv_time) => {
                    if command_id % 30 == 0 {
                        debug!(
                            "Server: Request from client {client_id}: {command_id} {kv_command:?}"
                        );
                    }
                    let req_data = RequestInstrumentation::with(net_recv_time, Instant::now());
                    for i in self.request_data.len()..command_id + 1 {
                        self.request_data.push(None);
                    }
                    self.request_data[command_id] = Some(req_data);
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
        let latencies: Vec<RequestInstrumentation> = self
            .request_data
            .iter()
            .cloned()
            .map(|d| d.unwrap())
            .collect();
        let recv_latencies: Vec<Duration> = latencies
            .iter()
            .map(|data| data.channel_receive - data.net_receive)
            .collect();
        let avg_recv_latency = self.get_average(&recv_latencies);
        eprintln!("Net Recv -> Task Recv        {avg_recv_latency:?}");

        let start_accdec_latencies: Vec<Duration> = latencies
            .iter()
            .map(|data| data.send_accdec[0].1 - data.channel_receive)
            .collect();
        let avg_start_accdec_latency = self.get_average(&start_accdec_latencies);
        eprintln!("Request -> Start AccDec      {avg_start_accdec_latency:?}");

        let accdec_latencies: Vec<Duration> = latencies
            .iter()
            .map(|data| data.send_accdec[data.send_accdec.len() - 1].1 - data.send_accdec[0].1)
            .collect();
        let avg_accdec_latency = self.get_average(&accdec_latencies);
        eprintln!("Start AccDec -> Last AccDec  {avg_accdec_latency:?}");

        let accepted_latencies: Vec<Vec<Option<Duration>>> = latencies
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
            .map(|data| data.commit.unwrap() - data.send_accdec[0].1)
            .collect();
        let avg_commit_latency = self.get_average(&commit_latencies);
        eprintln!("Start AccDec -> Commit      {avg_commit_latency:?}");

        let server_total_latencies: Vec<Duration> = latencies
            .iter()
            .map(|data| data.sending_response.unwrap() - data.net_receive)
            .collect();
        let avg_server_total_latency = self.get_average(&server_total_latencies);
        eprintln!("Server Total Latency        {avg_server_total_latency:?}");
    }

    async fn debug_acceptor_data(&self) {
        let accepted_latencies: Vec<Duration> = self
            .acceptor_data
            .iter()
            .map(|data| match data.send_accepted {
                Some(time) => time - data.receive_accdec,
                None => Duration::from_millis(0),
            })
            .collect();
        tokio::time::sleep(Duration::from_millis(50 * self.id)).await;
        let avg_accepted_latency = self.get_average(&accepted_latencies);
        eprintln!(
            "Node {}: Accepted Latency {avg_accepted_latency:?}",
            self.id
        );
    }

    fn get_average(&self, latencies: &Vec<Duration>) -> Vec<Duration> {
        let batch_count = latencies.len() / self.batch_size;
        let mut sums = vec![Duration::from_millis(0); self.batch_size];
        for batch in latencies.chunks(self.batch_size) {
            for (i, latency) in batch.into_iter().enumerate() {
                sums[i] += *latency;
            }
        }
        sums.iter().map(|&sum| sum / batch_count as u32).collect()
    }
    fn get_average2(&self, latencies: &Vec<Duration>) -> Vec<Duration> {
        let critical_len = match self.use_metronome {
            true => 6,
            false => 10,
        };
        let batch_count = latencies.len() / critical_len;
        let mut sums = vec![Duration::from_millis(0); critical_len];
        for batch in latencies.chunks(critical_len) {
            for (i, latency) in batch.into_iter().enumerate() {
                sums[i] += *latency;
            }
        }
        sums.iter().map(|&sum| sum / batch_count as u32).collect()
    }

    fn get_average_per_acceptor(
        &self,
        latencies: &Vec<Vec<Option<Duration>>>,
    ) -> Vec<Vec<Duration>> {
        let batch_count = latencies.len() / self.batch_size;
        let acceptor_count = latencies[0].len();
        let mut sums = vec![vec![Duration::from_millis(0); acceptor_count]; self.batch_size];
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
        for acceptor_sums in sums.iter_mut() {
            for sum in acceptor_sums.iter_mut() {
                *sum = *sum / batch_count as u32;
            }
        }
        sums
    }

    fn get_average_acceptor_delay(acceptor_averages: &Vec<Vec<Duration>>) -> Vec<Duration> {
        let acceptor_count = acceptor_averages[0].len();
        let batch_count = acceptor_averages.len();

        let mut acceptor_delays = vec![Duration::from_millis(0)];
        for acceptor in 1..acceptor_count {
            let mut first_accepted = None;
            let mut last_accepted = None;
            let mut num_delays = 0;
            for batch in 0..batch_count {
                let accepted_latency = acceptor_averages[batch][acceptor];
                if !accepted_latency.is_zero() {
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
