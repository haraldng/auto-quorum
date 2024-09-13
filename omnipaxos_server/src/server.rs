use chrono::Utc;
use futures::StreamExt;
use log::*;
use serde::{Deserialize, Serialize};
use std::time::Duration;

use omnipaxos::{
    util::{LogEntry, NodeId}, OmniPaxos, OmniPaxosConfig,
};
use omnipaxos::ballot_leader_election::Ballot;
use omnipaxos::messages::Message;
use omnipaxos::messages::sequence_paxos::PaxosMsg;
use omnipaxos::sequence_paxos::Phase;
use omnipaxos_storage::memory_storage::MemoryStorage;

use crate::{
    database::Database,
    metrics::MetricsHeartbeatServer,
    network::Network,
    optimizer::{ClusterOptimizer, ClusterStrategy},
};
use common::{kv::*, messages::*};

const LEADER_WAIT: Duration = Duration::from_secs(3);

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OmniPaxosServerConfig {
    pub location: String,
    pub initial_leader: Option<NodeId>,
    pub optimize: Option<bool>,
    pub optimize_threshold: Option<f64>,
    pub congestion_control: Option<bool>,
    pub local_deployment: Option<bool>,
    pub initial_read_strat: Option<Vec<ReadStrategy>>,
    pub storage_duration_micros: Option<u64>
}

type OmniPaxosInstance = OmniPaxos<Command, MemoryStorage<Command>>;

pub struct OmniPaxosServer {
    id: NodeId,
    nodes: Vec<NodeId>,
    database: Database,
    network: Network,
    omnipaxos: OmniPaxosInstance,
    current_decided_idx: usize,
    metrics_server: MetricsHeartbeatServer,
    optimizer: ClusterOptimizer,
    strategy: ClusterStrategy,
    optimize: bool,
    optimize_threshold: f64,
    leader_attempt: u32,
    storage_duration: Duration,
}

impl OmniPaxosServer {
    pub async fn new(
        server_config: OmniPaxosServerConfig,
        omnipaxos_config: OmniPaxosConfig,
    ) -> Self {
        let server_id = omnipaxos_config.server_config.pid;
        let nodes = omnipaxos_config.cluster_config.nodes.clone();
        let local_deployment = server_config.local_deployment.unwrap_or(false);
        let optimize = server_config.optimize.unwrap_or(true);
        let storage_duration_micros = server_config.storage_duration_micros.expect("Storage duration must be set");
        // let flush_duration = Duration::from_millis(storage_duration_ms);
        // let storage: DurationStorage<Command> = DurationStorage::new(flush_duration);
        let storage = MemoryStorage::default();
        info!("Node: {:?}: using metronome: {}, storage_duration: {}", server_id, omnipaxos_config.cluster_config.use_metronome, storage_duration_micros);
        let omnipaxos = omnipaxos_config.build(storage).unwrap();
        let network = Network::new(server_id, nodes.clone(), local_deployment)
            .await
            .unwrap();
        let metrics_server = MetricsHeartbeatServer::new(server_id, nodes.clone());
        let quorum_size = nodes.len() / 2 + 1;
        let init_read_quorum = quorum_size;
        let init_read_strat = match server_config.initial_read_strat {
            Some(strats) => {
                assert_eq!(strats.len(), nodes.len());
                strats
            }
            None => vec![ReadStrategy::default(); nodes.len()],
        };
        let init_strat = ClusterStrategy {
            leader: server_id,
            read_quorum_size: init_read_quorum,
            write_quorum_size: nodes.len() - init_read_quorum + 1,
            read_strategies: init_read_strat,
        };
        let optimizer = ClusterOptimizer::new(nodes.clone());
        let mut server = OmniPaxosServer {
            id: server_id,
            nodes,
            database: Database::new(),
            network,
            omnipaxos,
            current_decided_idx: 0,
            metrics_server,
            optimizer,
            strategy: init_strat,
            optimize,
            optimize_threshold: server_config.optimize_threshold.unwrap_or(0.8),
            leader_attempt: 0,
            storage_duration: Duration::from_micros(storage_duration_micros),
        };
        server.send_outgoing_msgs().await;
        server
    }

    pub async fn run(&mut self, initial_leader: NodeId) {
        let mut election_interval = tokio::time::interval(LEADER_WAIT);
        let mut outgoing_interval = tokio::time::interval(Duration::from_millis(1));
        election_interval.tick().await;

        loop {
            tokio::select! {
                biased;
                _ = election_interval.tick() => {
                    self.become_initial_leader(initial_leader).await;
                },
                _ = outgoing_interval.tick() => {
                    self.send_outgoing_msgs().await;
                }
                Some(msg) = self.network.next() => {
                    self.handle_incoming_msg(msg.unwrap()).await;
                },
            }
        }
    }

    async fn become_initial_leader(&mut self, leader_pid: NodeId) {
        if leader_pid == self.id {
            let (_leader, phase) = self.omnipaxos.get_current_leader_state();
            match phase {
                Phase::Accept => {},
                _ => {
                    let mut ballot = Ballot::default();
                    self.leader_attempt += 1;
                    ballot.n = self.leader_attempt;
                    ballot.pid = self.id;
                    ballot.config_id = 1;
                    info!("Node: {:?}, Initializing prepare phase with ballot: {:?}", self.id, ballot);
                    self.omnipaxos.initialize_prepare_phase(ballot);
                }
            }
        }
    }

    async fn handle_decided_entries(&mut self) {
        let new_decided_idx = self.omnipaxos.get_decided_idx();
        if self.current_decided_idx < new_decided_idx {
            let decided_entries = self
                .omnipaxos
                .read_decided_suffix(self.current_decided_idx);
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
                },
                None => {
                    let log_len = self.omnipaxos.read_entries(0..).unwrap_or_default().len();
                    warn!("Node: {:?}, Decided {new_decided_idx} but log len is: {log_len}", self.id);
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
                    .send(Outgoing::ServerMessage(command.client_id, response))
                    .await;
            }
        }
    }

    async fn send_outgoing_msgs(&mut self) {
        let messages = self.omnipaxos.outgoing_messages();
        for msg in messages {
            match &msg {
                Message::SequencePaxos(paxos_msg) => {
                    match paxos_msg.msg {
                        PaxosMsg::Accepted(_) => {
                            tokio::time::sleep(self.storage_duration).await;
                        },
                        _ => {}
                    }
                }
                Message::BLE(_) => {}
            }
            let to = msg.get_receiver();
            let cluster_msg = ClusterMessage::OmniPaxosMessage(msg);
            self.network
                .send(Outgoing::ClusterMessage(to, cluster_msg))
                .await;
        }
    }

    async fn handle_incoming_msg(&mut self, msg: Incoming) {
        match msg {
            Incoming::ClientMessage(from, request) => {
                self.handle_client_request(from, request).await;
            }
            Incoming::ClusterMessage(_from, ClusterMessage::OmniPaxosMessage(m)) => {
                self.omnipaxos.handle_incoming(m);
                self.send_outgoing_msgs().await;
                if self.id == self.omnipaxos.get_current_leader().unwrap_or_default() {
                    self.handle_decided_entries().await;
                }
            }
            Incoming::ClusterMessage(from, ClusterMessage::QuorumReadRequest(req)) => {
                self.handle_quorum_read_request(from, req).await;
            }
            Incoming::ClusterMessage(from, ClusterMessage::QuorumReadResponse(resp)) => {
                self.handle_quorum_read_response(from, resp).await;
            }
            Incoming::ClusterMessage(from, ClusterMessage::MetricSync(sync)) => {
                if let Some((to, reply)) = self.metrics_server.handle_metric_sync(from, sync) {
                    self.network
                        .send(Outgoing::ClusterMessage(
                            to,
                            ClusterMessage::MetricSync(reply),
                        ))
                        .await;
                }
            }
            Incoming::ClusterMessage(from, ClusterMessage::ReadStrategyUpdate(strat)) => {
                self.handle_read_strategy_update(from, strat);
            }
        }
    }

    async fn handle_client_request(&mut self, from: ClientId, request: ClientMessage) {
        match request {
            ClientMessage::Append(command_id, kv_command) => match &kv_command {
                KVCommand::Put(..) => {
                    self.metrics_server.local_write();
                    self.commit_command_to_log(from, command_id, kv_command)
                        .await;
                }
                KVCommand::Delete(_) => {
                    self.metrics_server.local_write();
                    self.commit_command_to_log(from, command_id, kv_command)
                        .await;
                }
                KVCommand::Get(_) => {
                    self.metrics_server.local_read();
                    self.handle_read_request(from, command_id, kv_command).await;
                }
            },
        };
    }

    async fn handle_read_request(
        &mut self,
        from: ClientId,
        command_id: CommandId,
        kv_command: KVCommand,
    ) {
        let read_strat = self.strategy.read_strategies[self.id as usize - 1];
        match read_strat {
            ReadStrategy::ReadAsWrite => {
                self.commit_command_to_log(from, command_id, kv_command)
                    .await
            }
            ReadStrategy::QuorumRead => {
                self.start_quorum_read(from, command_id, kv_command, false)
                    .await
            }
            ReadStrategy::BallotRead => {
                self.start_quorum_read(from, command_id, kv_command, true)
                    .await
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

    async fn handle_quorum_read_request(&mut self, from: NodeId, request: QuorumReadRequest) {
        unimplemented!()
    }

    // TODO: if reads show us that something is chosen but not decided we can decide it and
    // apply chosen writes and rinse reads immediately.
    async fn handle_quorum_read_response(&mut self, from: NodeId, response: QuorumReadResponse) {
        unimplemented!()
    }

    async fn start_quorum_read(
        &mut self,
        client_id: ClientId,
        command_id: CommandId,
        read_command: KVCommand,
        enable_ballot_read: bool,
    ) {
        unimplemented!()
    }

    async fn send_strat(&mut self) {
        unimplemented!()
    }

    // TODO: its possible for strategy updates to come in out of sync or get dropped
    fn handle_read_strategy_update(&mut self, _from: NodeId, read_strat: Vec<ReadStrategy>) {
        self.strategy.read_strategies = read_strat
    }

    fn log(
        &self,
        strategy: Option<(&ClusterStrategy, f64, f64)>,
        new_strat: bool,
        cache_update: bool,
    ) {
        let timestamp = Utc::now().timestamp_millis();
        let leader = self.omnipaxos.get_current_leader();
        let leader_json = serde_json::to_string(&leader).unwrap();
        let metrics_json = serde_json::to_string(&self.metrics_server.metrics).unwrap();
        let opt_strategy_latency =
            strategy.map(|s| s.1 / self.metrics_server.metrics.get_total_load());
        let curr_strategy_latency =
            strategy.map(|s| s.2 / self.metrics_server.metrics.get_total_load());
        let strategy_json = match strategy {
            Some((strat, _, _)) => serde_json::to_string(strat).unwrap(),
            None => serde_json::to_string(&None::<ClusterStrategy>).unwrap(),
        };
        let opt_strategy_latency_json = serde_json::to_string(&opt_strategy_latency).unwrap();
        let curr_strategy_latency_json = serde_json::to_string(&curr_strategy_latency).unwrap();
        println!("{{ \"timestamp\": {timestamp}, \"new_strat\": {new_strat}, \"opt_strat_latency\": {opt_strategy_latency_json:<20}, \"curr_strat_latency\": {curr_strategy_latency_json:<20}, \"cluster_strategy\": {strategy_json:<200}, \"leader\": {leader_json}, \"metrics_update\": {cache_update}, \"cluster_metrics\": {metrics_json} }}");
    }
}
