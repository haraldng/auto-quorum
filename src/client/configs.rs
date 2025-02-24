use config::{Config, ConfigError, Environment, File};
use metronome::common::kv::NodeId;
use serde::{Deserialize, Serialize};
use std::{env, time::Duration};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ClientConfig {
    pub location: String,
    pub server_id: NodeId,
    pub server_address: String,
    pub request_mode_config: RequestModeConfig,
    pub end_condition: EndConditionConfig,
    pub summary_filename: String,
    pub summary_only: bool,
    pub output_filename: String,
}

impl ClientConfig {
    pub fn new() -> Result<Self, ConfigError> {
        let config_file = match env::var("CONFIG_FILE") {
            Ok(file_path) => file_path,
            Err(_) => panic!("Requires CONFIG_FILE environment variable to be set"),
        };
        let config = Config::builder()
            .add_source(File::with_name(&config_file))
            // Add-in/overwrite settings with environment variables (with a prefix of METRONOME)
            .add_source(Environment::with_prefix("METRONOME").try_parsing(true))
            .build()?;
        config.try_deserialize()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
#[serde(
    tag = "request_mode_config_type",
    content = "request_mode_config_value"
)]
pub enum RequestModeConfig {
    // New request on reply.
    // (usize) = number of parallel requests
    ClosedLoop(usize),
    // New requests every duration.
    // (u64, usize) = (request interval ms, requests to send perinterval)
    OpenLoop(u64, usize),
}

impl RequestModeConfig {
    pub fn to_closed_loop_params(self) -> Option<usize> {
        match self {
            RequestModeConfig::ClosedLoop(parallel_requests) => Some(parallel_requests),
            RequestModeConfig::OpenLoop(_, _) => None,
        }
    }

    pub fn to_open_loop_params(self) -> Option<(Duration, usize)> {
        match self {
            RequestModeConfig::OpenLoop(req_interval, num_req) => {
                Some((Duration::from_millis(req_interval), num_req))
            }
            RequestModeConfig::ClosedLoop(_) => None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
#[serde(tag = "end_condition_type", content = "end_condition_value")]
pub enum EndConditionConfig {
    ResponsesCollected(usize),
    SecondsPassed(u64),
}

#[derive(Debug, Clone, Copy)]
pub enum EndCondition {
    ResponsesCollected(usize),
    TimePassed(Duration),
}

impl From<EndConditionConfig> for EndCondition {
    fn from(config: EndConditionConfig) -> Self {
        match config {
            EndConditionConfig::ResponsesCollected(n) => EndCondition::ResponsesCollected(n),
            EndConditionConfig::SecondsPassed(secs) => {
                EndCondition::TimePassed(Duration::from_secs(secs))
            }
        }
    }
}
