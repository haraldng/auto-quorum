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
    net_recieve: Instant,
    channel_recieve: Instant,
    send_accdec: Option<Instant>,
    send_dec: Option<Instant>,
    sending_response: Option<Instant>,
}

impl RequestInstrumentation {
    fn with(net_receive_time: Instant, ch_receive_time: Instant) -> Self {
        Self {
            net_recieve: net_receive_time,
            channel_recieve: ch_receive_time,
            send_accdec: None,
            send_dec: None,
            sending_response: None,
        }
    }
}

pub struct OmniPaxosServer {
    id: NodeId,
    peers: Vec<NodeId>,
    database: Database,
    network: Network,
    cluster_messages: Receiver<ClusterMessage>,
    client_messages: Receiver<(ClientMessage, Instant)>,
    omnipaxos: OmniPaxosInstance,
    initial_leader: NodeId,
    leader_attempt: u32,
    delay_strategy: DelayStrategy,
    batch_size: usize,
    request_data: Vec<RequestInstrumentation>,
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
        // let batch_size = omnipaxos_config.server_config.batch_size;
        let batch_size = 10;
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
            request_data: vec![],
        };
        // Clears outgoing_messages of initial BLE messages
        let _ = server.omnipaxos.outgoing_messages();
        server
    }

    pub async fn run(&mut self) {
        if self.id == self.initial_leader {
            let mut election_interval = tokio::time::interval(LEADER_WAIT);
            loop {
                tokio::select! {
                    // Ensures cluster is connected and leader is promoted before client starts sending
                    // requests.
                    _ = election_interval.tick() => {
                        self.become_initial_leader().await;
                        let (leader_id, phase) = self.omnipaxos.get_current_leader_state();
                        if self.id == leader_id && phase == Phase::Accept {
                            debug!("{}: Leader fully initialized", self.id);
                            self.network.send_to_client(self.id, ServerMessage::Ready).await;
                            break;
                        }
                    },
                    // Still necessary for sending handshakes
                    Some(msg) = self.cluster_messages.recv() => {
                        self.handle_cluster_message(msg).await;
                    },
                    Some(msg) = self.client_messages.recv() => {
                        self.handle_client_request(msg).await;
                    },
                }
            }
        }

        let reasonable_process_time = Duration::from_micros(200);
        loop {
            let done_signal = tokio::select! {
                Some(msg) = self.client_messages.recv() => {
                    // let now = Instant::now();
                    let is_done = self.handle_client_request(msg.clone()).await;
                    // let process_time = now.elapsed();
                    // let process_time = now.elapsed();
                    // if process_time > reasonable_process_time {
                    // eprintln!("Handle client request time {process_time:?}\n{msg:?}",);
                    // }
                    is_done
                },
                Some(msg) = self.cluster_messages.recv() => {
                    // let now = Instant::now();
                    let is_done = self.handle_cluster_message(msg.clone()).await;
                    // let process_time = now.elapsed();
                    // if process_time > reasonable_process_time {
                    // eprintln!("Handle cluster message time {process_time:?}\n{msg:?}",);
                    // }
                    is_done
                },
            };
            if done_signal {
                break;
            }
        }
    }

    async fn become_initial_leader(&mut self) {
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
                self.send_outgoing_msgs().await;
            }
        }
    }

    async fn handle_decided_entries(&mut self) {
        let decided_slots = self.omnipaxos.take_decided_slots_since_last_call();
        for slot in decided_slots {
            let decided_entry = self.omnipaxos.read(slot).unwrap();
            match decided_entry {
                LogEntry::Decided(cmd) => self.update_database_and_respond(cmd).await,
                // TODO: fix slot indexing
                LogEntry::Undecided(cmd) => self.update_database_and_respond(cmd).await,
                LogEntry::Snapshotted(_) => unimplemented!(),
                _ => unreachable!(),
            }
        }
    }

    async fn update_database_and_respond(&mut self, command: Command) {
        let read = self.database.handle_command(command.kv_cmd);
        if command.coordinator_id == self.id {
            let response = match read {
                Some(read_result) => ServerMessage::Read(command.id, read_result),
                None => ServerMessage::Write(command.id),
            };
            self.network
                .send_to_client(command.client_id, response)
                .await;
            let response_time = Instant::now();
            // eprintln!("Sending response {}", command.id);
            self.request_data[command.id].sending_response = Some(response_time);
        }
    }
    // async fn handle_decided_entries(&mut self) {
    //     let new_decided_idx = self.omnipaxos.get_decided_idx();
    //     if self.current_decided_idx < new_decided_idx {
    //         let decided_entries = self.omnipaxos.read_decided_suffix(self.current_decided_idx);
    //         match decided_entries {
    //             Some(ents) => {
    //                 self.current_decided_idx = new_decided_idx;
    //                 let decided_commands = ents.into_iter().filter_map(|e| match e {
    //                     LogEntry::Decided(cmd) => Some(cmd),
    //                     LogEntry::Snapshotted(_) => unimplemented!(),
    //                     _ => None,
    //                 });
    //                 self.update_database_and_respond(decided_commands.collect())
    //                     .await;
    //             }
    //             None => {
    //                 let log_len = self.omnipaxos.read_entries(0..).unwrap_or_default().len();
    //                 warn!(
    //                     "Node: {:?}, Decided {new_decided_idx} but log len is: {log_len}",
    //                     self.id
    //                 );
    //             }
    //         }
    //     }
    // }
    //
    // async fn update_database_and_respond(&mut self, commands: Vec<Command>) {
    //     // TODO: batching responses possible here.
    //     for command in commands {
    //         let read = self.database.handle_command(command.kv_cmd);
    //         if command.coordinator_id == self.id {
    //             let response = match read {
    //                 Some(read_result) => ServerMessage::Read(command.id, read_result),
    //                 None => ServerMessage::Write(command.id),
    //             };
    //             let response_time = Instant::now();
    //             self.request_data[command.id].sending_response = Some(response_time);
    //             self.network
    //                 .send_to_client(command.client_id, response)
    //                 .await;
    //         }
    //     }
    // }

    async fn send_outgoing_msgs(&mut self) {
        let messages = self.omnipaxos.outgoing_messages();
        if self.id == 1 && !messages.is_empty() {
            for m in &messages {
                // debug!("{m:?}");
                match m {
                    Message::SequencePaxos(PaxosMessage {
                        from: _,
                        to: _,
                        msg: PaxosMsg::Decide(dec),
                    }) => {
                        let dec_time = Instant::now();
                        // eprintln!("Sending decide {}", dec.decided_idx);
                        self.request_data[dec.decided_idx - 1].send_dec = Some(dec_time);
                    }
                    Message::SequencePaxos(PaxosMessage {
                        from: _,
                        to: _,
                        msg: PaxosMsg::AcceptDecide(accdec),
                    }) => {
                        let accdec_time = Instant::now();
                        for entry in &accdec.entries {
                            // eprintln!("Sending Acceptdecide {}", entry.id);
                            self.request_data[entry.id].send_accdec = Some(accdec_time);
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
                    msg: PaxosMsg::Accepted(_),
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
                    self.network.send_to_cluster(to, cluster_msg).await;
                }
                _ => self.network.send_to_cluster(to, cluster_msg).await,
            }
        }
    }

    async fn handle_cluster_message(&mut self, message: ClusterMessage) -> bool {
        match message {
            ClusterMessage::OmniPaxosMessage(m) => {
                self.omnipaxos.handle_incoming(m);
                self.send_outgoing_msgs().await;
                if self.id == self.omnipaxos.get_current_leader().unwrap_or_default() {
                    self.handle_decided_entries().await;
                }
                false
            }
            ClusterMessage::Done => true,
        }
    }

    async fn handle_client_request(&mut self, request: (ClientMessage, Instant)) -> bool {
        match request {
            (ClientMessage::Append(client_id, command_id, kv_command), net_recv_time) => {
                debug!("Server: Request from client {client_id}: {command_id} {kv_command:?}");
                let req_data = RequestInstrumentation::with(net_recv_time, Instant::now());
                self.request_data.push(req_data);
                self.commit_command_to_log(client_id, command_id, kv_command)
                    .await;
                false
            }
            (ClientMessage::Done, _) => {
                let done_msg = ClusterMessage::Done;
                for peer in &self.peers {
                    self.network.send_to_cluster(*peer, done_msg.clone()).await;
                }
                self.debug_request_data();
                true
            }
        }
    }

    async fn commit_command_to_log(
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
        self.send_outgoing_msgs().await;
    }

    fn debug_request_data(&mut self) {
        // let missing: Vec<(usize, RequestInstrumentation)> = self
        //     .request_data
        //     .iter()
        //     .cloned()
        //     .enumerate()
        //     .filter_map(|(i, d)| {
        //         if d.send_dec.is_none() || d.send_accdec.is_none() || d.sending_response.is_none() {
        //             Some((i, d.clone()))
        //         } else {
        //             None
        //         }
        //     })
        //     .collect();
        // if !missing.is_empty() {
        //     error!("The send decideds\n{missing:?}");
        //
        //     self.request_data = self
        //         .request_data
        //         .iter()
        //         .cloned()
        //         .filter(|d| d.send_dec.is_some())
        //         .collect();
        // }
        let server_task_request_latencies: Vec<Duration> = self
            .request_data
            .iter()
            .map(|data| data.sending_response.unwrap() - data.channel_recieve)
            .collect();
        let avg_server_task_latency = self.get_average(server_task_request_latencies);
        let server_total_request_latencies: Vec<Duration> = self
            .request_data
            .iter()
            .map(|data| data.sending_response.unwrap() - data.net_recieve)
            .collect();
        let avg_server_total_latency = self.get_average(server_total_request_latencies);

        let recv_latencies: Vec<Duration> = self
            .request_data
            .iter()
            .map(|data| data.channel_recieve - data.net_recieve)
            .collect();
        let avg_recv_latency = self.get_average(recv_latencies);

        let accdec_latencies: Vec<Duration> = self
            .request_data
            .iter()
            .map(|data| data.send_accdec.unwrap() - data.channel_recieve)
            .collect();
        let avg_accdec_latency = self.get_average(accdec_latencies);

        let resp_latencies: Vec<Duration> = self
            .request_data
            .iter()
            .map(|data| data.sending_response.unwrap() - data.send_accdec.unwrap())
            .collect();
        let avg_resp_latency = self.get_average(resp_latencies);

        // TODO: Last entry may not have a send decide time
        // let dec_latencies: Vec<Duration> = self
        //     .request_data
        //     .iter()
        //     .map(|data| data.send_dec.unwrap() - data.send_accdec.unwrap())
        //     .collect();
        // let avg_dec_latency = self.get_average(dec_latencies);

        eprintln!("Net Recv -> Task Recv   {avg_recv_latency:?}");
        eprintln!("Request -> Send AccDec  {avg_accdec_latency:?}");
        eprintln!("Send AccDec -> Response {avg_resp_latency:?}");
        eprintln!("Server Task Latency     {avg_server_task_latency:?}");
        eprintln!("Server Total Latency    {avg_server_total_latency:?}");
    }

    fn get_average(&self, latencies: Vec<Duration>) -> Vec<Duration> {
        let batch_count = latencies.len() / self.batch_size;
        let mut sums = vec![Duration::from_millis(0); self.batch_size];
        for batch in latencies.chunks(self.batch_size) {
            for (i, latency) in batch.into_iter().enumerate() {
                sums[i] += *latency;
            }
        }
        sums.iter().map(|&sum| sum / batch_count as u32).collect()
        // let mut variances = vec![Duration::from_millis(0); self.batch_size];
        // for batch in latencies.chunks(self.batch_size) {
        //     for i in 0..self.batch_size {
        //         let diff = batch[i] - avg_latencies[i];
        //         variances[i] += diff * diff;
        //     }
        // }
        // let std_devs = variances
        //     .into_iter()
        //     .map(|v| (v / batch_count as f64).sqrt())
        //     .collect();
    }
}
