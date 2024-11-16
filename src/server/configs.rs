use metronome::common::messages::{DelayInfo, MetronomeConfigInfo, PersistInfo};
use omnipaxos::{
    util::NodeId, BatchSetting, ClusterConfig, MetronomeSetting, OmniPaxosConfig, ServerConfig,
};
use serde::{Deserialize, Serialize};
use std::{fs::File, time::Duration};
use tempfile::tempfile;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MetronomeServerConfig {
    pub location: String,
    pub local_deployment: Option<bool>,
    pub server_id: NodeId,
    pub instrumentation: bool,
    pub debug_filename: String,
    // Cluster-wide settings
    pub cluster_name: String,
    pub nodes: Vec<NodeId>,
    pub metronome_config: MetronomeSetting,
    pub persist_config: PersistConfig,
    pub delay_config: DelayConfig,
    pub initial_leader: Option<NodeId>,
    pub metronome_quorum_size: Option<usize>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
#[serde(tag = "delay_type", content = "delay_value")]
pub enum DelayConfig {
    Sleep(u64),
    File(usize),
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
#[serde(tag = "persist_type", content = "persist_value")]
pub enum PersistConfig {
    Individual,
    Every(usize),
    Opportunistic,
}

impl Into<OmniPaxosConfig> for MetronomeServerConfig {
    fn into(self) -> OmniPaxosConfig {
        let delay_strat = self.delay_config.into();
        let batch_setting = match delay_strat {
            DelayStrategy::NoDelay => BatchSetting::Opportunistic,
            _ => match self.persist_config {
                PersistConfig::Individual => BatchSetting::Individual,
                PersistConfig::Every(n) => BatchSetting::Every(n),
                PersistConfig::Opportunistic => BatchSetting::Opportunistic,
            },
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
        let persist_info = match self.persist_config {
            PersistConfig::Individual => PersistInfo::Individual,
            PersistConfig::Every(n) => PersistInfo::Every(n),
            PersistConfig::Opportunistic => PersistInfo::Opportunistic,
        };
        let delay_info = match self.delay_config {
            DelayConfig::Sleep(μs) => DelayInfo::Sleep(μs),
            DelayConfig::File(d) => DelayInfo::File(d),
        };
        MetronomeConfigInfo {
            cluster_size: self.nodes.len(),
            metronome_info: self.metronome_config,
            metronome_quorum_size: self.metronome_quorum_size,
            persist_info,
            delay_info,
            instrumented: self.instrumentation,
        }
    }
}

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
