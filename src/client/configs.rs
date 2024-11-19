use metronome::common::kv::NodeId;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ClientConfig {
    pub cluster_name: String,
    pub location: String,
    pub server_id: NodeId,
    pub local_deployment: Option<bool>,
    pub request_mode_config: RequestModeConfig,
    pub end_condition: EndConditionConfig,
    pub summary_filename: String,
    pub output_filename: String,
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
