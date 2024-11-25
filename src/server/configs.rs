use metronome::common::messages::{BatchInfo, MetronomeConfigInfo, PersistInfo};
use omnipaxos::{
    util::NodeId, BatchSetting, ClusterConfig, MetronomeSetting, OmniPaxosConfig, ServerConfig,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MetronomeServerConfig {
    pub location: String,
    pub local_deployment: Option<bool>,
    pub server_id: NodeId,
    pub instrumentation: bool,
    pub debug_filename: String,
    pub persist_log_filepath: String,
    // Cluster-wide settings
    pub cluster_name: String,
    pub nodes: Vec<NodeId>,
    pub metronome_config: MetronomeSetting,
    pub batch_config: BatchConfig,
    pub persist_config: PersistConfig,
    pub initial_leader: Option<NodeId>,
    pub metronome_quorum_size: Option<usize>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
#[serde(tag = "persist_type", content = "persist_value")]
pub enum PersistConfig {
    NoPersist,
    File(usize),
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
#[serde(tag = "batch_type", content = "batch_value")]
pub enum BatchConfig {
    Individual,
    Every(usize),
    Opportunistic,
}

impl Into<OmniPaxosConfig> for MetronomeServerConfig {
    fn into(self) -> OmniPaxosConfig {
        let batch_setting = match self.batch_config {
            BatchConfig::Individual => BatchSetting::Individual,
            BatchConfig::Every(n) => BatchSetting::Every(n),
            BatchConfig::Opportunistic => BatchSetting::Opportunistic,
        };
        let cluster_config = ClusterConfig {
            configuration_id: 1,
            nodes: self.nodes,
            flexible_quorum: None,
            metronome_setting: self.metronome_config,
            batch_setting,
            metronome_quorum_size: self.metronome_quorum_size,
        };
        let server_config = ServerConfig {
            pid: self.server_id,
            ..Default::default()
        };
        OmniPaxosConfig {
            cluster_config,
            server_config,
        }
    }
}

impl Into<MetronomeConfigInfo> for MetronomeServerConfig {
    fn into(self) -> MetronomeConfigInfo {
        let batch_info = match self.batch_config {
            BatchConfig::Individual => BatchInfo::Individual,
            BatchConfig::Every(n) => BatchInfo::Every(n),
            BatchConfig::Opportunistic => BatchInfo::Opportunistic,
        };
        let persist_info = match self.persist_config {
            PersistConfig::NoPersist => PersistInfo::NoPersist,
            PersistConfig::File(data_size) => PersistInfo::File(data_size),
        };
        MetronomeConfigInfo {
            cluster_size: self.nodes.len(),
            metronome_info: self.metronome_config,
            metronome_quorum_size: self.metronome_quorum_size,
            batch_info,
            persist_info,
            instrumented: self.instrumentation,
        }
    }
}
