use core::panic;
use log::*;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::sync::mpsc::Receiver;

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
}

type OmniPaxosInstance = OmniPaxos<Command, MemoryStorage<Command>>;

pub struct OmniPaxosServer {
    id: NodeId,
    database: Database,
    network: Network,
    cluster_messages: Receiver<ClusterMessage>,
    client_messages: Receiver<ClientMessage>,
    omnipaxos: OmniPaxosInstance,
    initial_leader: NodeId,
    current_decided_idx: usize,
    leader_attempt: u32,
    storage_delay: Option<Duration>,
}

impl OmniPaxosServer {
    pub async fn new(
        server_config: OmniPaxosServerConfig,
        omnipaxos_config: OmniPaxosConfig,
        initial_leader: NodeId,
    ) -> Self {
        let server_id = omnipaxos_config.server_config.pid;
        let nodes = omnipaxos_config.cluster_config.nodes.clone();
        let local_deployment = server_config.local_deployment.unwrap_or(false);
        let storage_delay = match server_config.storage_duration_micros {
            None => panic!("Storage duration must be set"),
            Some(0) => None, // sleeping with duration 0 yields the task
            Some(dur) => Some(Duration::from_micros(dur)),
        };
        let storage = MemoryStorage::default();
        info!(
            "Node: {:?}: using metronome: {}, storage_duration: {:?}",
            server_id,
            omnipaxos_config.cluster_config.use_metronome,
            server_config.storage_duration_micros
        );
        let omnipaxos = omnipaxos_config.build(storage).unwrap();
        let initial_clients = match server_id == initial_leader {
            true => 1,
            false => 0,
        };
        let (cluster_message_sender, cluster_messages) = tokio::sync::mpsc::channel(10000);
        let (client_message_sender, client_messages) = tokio::sync::mpsc::channel(10000);
        let network = Network::new(
            server_config.cluster_name,
            server_id,
            nodes.clone(),
            initial_clients,
            local_deployment,
            client_message_sender,
            cluster_message_sender,
        )
        .await
        .unwrap();
        let mut server = OmniPaxosServer {
            id: server_id,
            database: Database::new(),
            network,
            cluster_messages,
            client_messages,
            omnipaxos,
            initial_leader,
            current_decided_idx: 0,
            leader_attempt: 0,
            storage_delay,
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

        loop {
            tokio::select! {
                Some(msg) = self.client_messages.recv() => {
                    self.handle_client_request(msg).await;
                },
                Some(msg) = self.cluster_messages.recv() => {
                    self.handle_cluster_message(msg).await;
                },
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
        let new_decided_idx = self.omnipaxos.get_decided_idx();
        if self.current_decided_idx < new_decided_idx {
            let decided_entries = self.omnipaxos.read_decided_suffix(self.current_decided_idx);
            match decided_entries {
                Some(ents) => {
                    self.current_decided_idx = new_decided_idx;
                    let decided_commands = ents.into_iter().filter_map(|e| match e {
                        LogEntry::Decided(cmd) => Some(cmd),
                        // TODO: handle snapshotted entries
                        _ => None,
                    });
                    self.update_database_and_respond(decided_commands.collect())
                        .await;
                }
                None => {
                    let log_len = self.omnipaxos.read_entries(0..).unwrap_or_default().len();
                    warn!(
                        "Node: {:?}, Decided {new_decided_idx} but log len is: {log_len}",
                        self.id
                    );
                }
            }
        }
    }

    async fn update_database_and_respond(&mut self, commands: Vec<Command>) {
        // TODO: batching responses possible here.
        for command in commands {
            let read = self.database.handle_command(command.kv_cmd);
            if command.coordinator_id == self.id {
                let response = match read {
                    Some(read_result) => ServerMessage::Read(command.id, read_result),
                    None => ServerMessage::Write(command.id),
                };
                self.network
                    .send_to_client(command.client_id, response)
                    .await;
            }
        }
    }

    async fn send_outgoing_msgs(&mut self) {
        let messages = self.omnipaxos.outgoing_messages();
        for msg in messages {
            let to = msg.get_receiver();
            let cluster_msg = ClusterMessage::OmniPaxosMessage(msg);
            match (self.storage_delay, &cluster_msg) {
                (
                    Some(delay),
                    ClusterMessage::OmniPaxosMessage(Message::SequencePaxos(PaxosMessage {
                        from: _,
                        to: _,
                        msg: PaxosMsg::Accepted(_),
                    })),
                ) => {
                    std::thread::sleep(delay);
                    self.network.send_to_cluster(to, cluster_msg).await;
                }
                _ => self.network.send_to_cluster(to, cluster_msg).await,
            }
        }
    }

    async fn handle_cluster_message(&mut self, message: ClusterMessage) {
        match message {
            ClusterMessage::OmniPaxosMessage(m) => {
                self.omnipaxos.handle_incoming(m);
                self.send_outgoing_msgs().await;
                if self.id == self.omnipaxos.get_current_leader().unwrap_or_default() {
                    self.handle_decided_entries().await;
                }
            }
        }
    }

    async fn handle_client_request(&mut self, request: ClientMessage) {
        match request {
            ClientMessage::Append(client_id, command_id, kv_command) => {
                self.commit_command_to_log(client_id, command_id, kv_command)
                    .await;
            }
        };
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
}
