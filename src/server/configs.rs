use std::env;

use config::{Config, ConfigError, Environment, File};
use metronome::common::messages::{BatchInfo, MetronomeConfigInfo, PersistInfo};
use omnipaxos::{
    util::NodeId, BatchSetting, ClusterConfig as OmnipaxosClusterConfig, MetronomeSetting,
    OmniPaxosConfig, ServerConfig as OmnipaxosServerConfig,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ClusterConfig {
    pub nodes: Vec<NodeId>,
    pub node_addrs: Vec<String>,
    pub initial_leader: Option<NodeId>,
    pub metronome_quorum_size: Option<usize>,
    pub metronome_config: MetronomeSetting,
    pub batch_config: BatchConfig,
    pub persist_config: PersistConfig,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ServerConfig {
    pub location: Option<String>,
    pub server_id: NodeId,
    pub listen_address: String,
    pub listen_port: u16,
    pub instrumentation: bool,
    pub debug_filename: String,
    pub persist_log_filepath: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MetronomeConfig {
    #[serde(flatten)]
    pub server: ServerConfig,
    #[serde(flatten)]
    pub cluster: ClusterConfig,
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

impl Into<OmniPaxosConfig> for MetronomeConfig {
    fn into(self) -> OmniPaxosConfig {
        let batch_setting = match self.cluster.batch_config {
            BatchConfig::Individual => BatchSetting::Individual,
            BatchConfig::Every(n) => BatchSetting::Every(n),
            BatchConfig::Opportunistic => BatchSetting::Opportunistic,
        };
        let cluster_config = OmnipaxosClusterConfig {
            configuration_id: 1,
            nodes: self.cluster.nodes,
            flexible_quorum: None,
            metronome_setting: self.cluster.metronome_config,
            batch_setting,
            metronome_quorum_size: self.cluster.metronome_quorum_size,
        };
        let server_config = OmnipaxosServerConfig {
            pid: self.server.server_id,
            ..Default::default()
        };
        OmniPaxosConfig {
            cluster_config,
            server_config,
        }
    }
}

impl Into<MetronomeConfigInfo> for MetronomeConfig {
    fn into(self) -> MetronomeConfigInfo {
        let batch_info = match self.cluster.batch_config {
            BatchConfig::Individual => BatchInfo::Individual,
            BatchConfig::Every(n) => BatchInfo::Every(n),
            BatchConfig::Opportunistic => BatchInfo::Opportunistic,
        };
        let persist_info = match self.cluster.persist_config {
            PersistConfig::NoPersist => PersistInfo::NoPersist,
            PersistConfig::File(data_size) => PersistInfo::File(data_size),
        };
        MetronomeConfigInfo {
            cluster_size: self.cluster.nodes.len(),
            metronome_info: self.cluster.metronome_config,
            metronome_quorum_size: self.cluster.metronome_quorum_size,
            batch_info,
            persist_info,
            instrumented: self.server.instrumentation,
        }
    }
}

impl MetronomeConfig {
    pub fn new() -> Result<Self, ConfigError> {
        let local_config_file = match env::var("SERVER_CONFIG_FILE") {
            Ok(file_path) => file_path,
            Err(_) => panic!("Requires SERVER_CONFIG_FILE environment variable to be set"),
        };
        let cluster_config_file = match env::var("CLUSTER_CONFIG_FILE") {
            Ok(file_path) => file_path,
            Err(_) => panic!("Requires CLUSTER_CONFIG_FILE environment variable to be set"),
        };
        let config = Config::builder()
            .add_source(File::with_name(&local_config_file))
            .add_source(File::with_name(&cluster_config_file))
            // Add-in/overwrite settings with environment variables (with a prefix of METRONOME)
            .add_source(
                Environment::with_prefix("METRONOME")
                    .try_parsing(true)
                    .list_separator(",")
                    .with_list_parse_key("node_addrs"),
            )
            .build()?;
        config.try_deserialize()
    }
}
