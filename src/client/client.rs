use std::time::Duration;

use crate::network::Network;
use auto_quorum::common::{kv::*, messages::*};
use futures::StreamExt;
use log::*;
use omnipaxos::util::FlexibleQuorum;
use serde::{Deserialize, Serialize};
use tokio::time::Instant;

// TODO: Server/Cluster config parameters like flexible_quorum should be taken from server, since
// there may be a mismatch between the client's config and the servers' configs.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ClientConfig {
    pub cluster_name: String,
    pub location: String,
    pub server_id: NodeId,
    pub local_deployment: Option<bool>,
    pub total_requests: Option<usize>,
    pub num_parallel_requests: Option<usize>,
    // Cluster Config for debugging
    pub use_metronome: Option<usize>,
    pub metronome_quorum_size: Option<usize>,
    pub flexible_quorum: Option<FlexibleQuorum>,
    pub nodes: Option<Vec<usize>>,
    pub storage_duration_micros: Option<usize>,
    pub data_size: Option<usize>,
}

pub struct Client {
    total_requests: usize,
    num_parallel_requests: usize,
    network: Network,
    request_data: Vec<(CommandId, Instant)>,
    response_data: Vec<(CommandId, Instant)>,
    config: ClientConfig,
}

impl Client {
    pub async fn new(config: ClientConfig) -> Self {
        let local_deployment = config.local_deployment.unwrap_or(false);
        let network = Network::new(
            config.cluster_name.clone(),
            config.server_id,
            local_deployment,
        )
        .await;
        let total_requests = config
            .total_requests
            .expect("Config field total_requests must be set");
        let num_parallel_requests = config
            .num_parallel_requests
            .expect("Config field num_parallel_requests must be set");
        Client {
            total_requests,
            num_parallel_requests,
            network,
            request_data: Vec::with_capacity(total_requests),
            response_data: Vec::with_capacity(total_requests),
            config,
        }
    }

    pub async fn run(&mut self) {
        // Wait until server is ready
        let first_msg = self.network.next().await;
        match first_msg {
            Some(ServerMessage::Ready) => (),
            Some(m) => panic!("Recieved unexpected message during handshake: {m:?}"),
            None => panic!("Lost connection to server"),
        }

        // Send initial requests
        for request_id in 0..self.num_parallel_requests {
            self.send_request(request_id);
        }

        // Send new requests on reponse until total requests reached
        loop {
            match self.network.next().await {
                Some(message) => match message {
                    ServerMessage::Ready => error!("Unexpected ready message"),
                    msg => {
                        debug!("Received {msg:?}");
                        let response_id = msg.command_id();
                        self.response_data.push((response_id, Instant::now()));
                        let next_request_id = response_id + self.num_parallel_requests;
                        if next_request_id < self.total_requests {
                            self.send_request(next_request_id);
                        } else if self.response_data.len() >= self.total_requests {
                            eprintln!("Client finished collecting responses");
                            break;
                        }
                    }
                },
                None => {
                    error!("Server connection lost");
                    break;
                }
            }
        }

        // Shutdown cluster and display latency data
        self.network.send(ClientMessage::Done);
        assert_eq!(self.request_data.len(), self.response_data.len());
        self.request_data.sort_by(|a, b| a.0.cmp(&b.0));
        self.response_data.sort_by(|a, b| a.0.cmp(&b.0));
        self.print_results();
    }

    fn send_request(&mut self, request_id: usize) {
        let key = request_id.to_string();
        let cmd = KVCommand::Put(key.clone(), key);
        let request = ClientMessage::Append(request_id, cmd);
        debug!("Sending request {request:?}");
        self.network.send(request);
        self.request_data.push((request_id, Instant::now()));
    }

    fn print_results(&self) {
        let response_latencies: Vec<u128> = self
            .request_data
            .iter()
            .zip(self.response_data.iter())
            .map(|((_, req_time), (_, resp_time))| (*resp_time - *req_time).as_micros())
            .collect();
        let total_time = self.calc_total_time();
        let throughput = self.calc_request_throughput();
        let (request_latency_average, request_latency_std_dev) =
            calc_avg_response_latency(&response_latencies);
        let (client_latencies_average, client_latencies_std_dev) =
            calc_avg_client_latencies(&response_latencies, self.num_parallel_requests);
        let output = ClientOutput {
            client_config: self.config.clone(),
            throughput,
            missed_responses: 0,
            total_time: format!("{total_time:?}"),
            request_latency_average,
            request_latency_std_dev,
            client_latencies_average,
            client_latencies_std_dev,
        };
        let json_output = serde_json::to_string_pretty(&output).unwrap();
        println!("{json_output}\n");
        eprintln!("{json_output}");

        // // Individual response times
        // for (cmd_idx, request) in self.request_data.iter().enumerate() {
        //     match &request.response {
        //         Some(response) => println!(
        //             "request: {:?}, latency: {:?}, sent: {:?}",
        //             response.message.command_id(),
        //             request.response_time().unwrap(),
        //             request.time_sent_utc
        //         ),
        //         None => println!(
        //             "request: {:?}, latency: NO RESPONSE, sent: {:?}",
        //             cmd_idx, request.time_sent_utc
        //         ),
        //     }
        // }
    }

    fn calc_total_time(&self) -> Duration {
        let first_request_time = self.request_data[0].1;
        let last_response_time = self.response_data[self.response_data.len() - 1].1;
        last_response_time - first_request_time
    }

    fn calc_request_throughput(&self) -> f64 {
        let num_requests = self.request_data.len();
        let first_request_time = self.request_data[0].1;
        let last_request_time = self.request_data[num_requests - 1].1;
        let messaging_duration = last_request_time - first_request_time;
        num_requests as f64 / (messaging_duration.as_millis() as f64 / 1000.)
    }
}

// The (average, std dev) response latency of all requests in milliseconds
fn calc_avg_response_latency(latencies: &Vec<u128>) -> (f64, f64) {
    let num_responses = latencies.len();
    let avg_latency = latencies.iter().sum::<u128>() / num_responses as u128;
    let variance = latencies
        .iter()
        .map(|&value| {
            let diff = value - avg_latency;
            diff * diff
        })
        .sum::<u128>() as f64
        / num_responses as f64;
    let std_dev = variance.sqrt();
    (avg_latency as f64 / 1000., std_dev / 1000.)
}

#[derive(Debug, Serialize)]
struct ClientOutput {
    client_config: ClientConfig,
    throughput: f64,
    missed_responses: usize,
    total_time: String,
    request_latency_average: f64,
    request_latency_std_dev: f64,
    client_latencies_average: Vec<f64>,
    client_latencies_std_dev: Vec<f64>,
}

// The (average, std dev) response latencies for each parallel pseudo-client in milliseconds
fn calc_avg_client_latencies(
    latencies: &Vec<u128>,
    parallel_requests: usize,
) -> (Vec<f64>, Vec<f64>) {
    let requests_per_client = (latencies.len() / parallel_requests) as u128;
    let mut sums = vec![0u128; parallel_requests];
    for batch in latencies.chunks(parallel_requests) {
        for i in 0..parallel_requests {
            sums[i] += batch[i];
        }
    }
    let avg_latencies: Vec<u128> = sums.iter().map(|&sum| sum / requests_per_client).collect();
    let mut variances = vec![0u128; parallel_requests];
    for batch in latencies.chunks(parallel_requests) {
        for i in 0..parallel_requests {
            let diff = batch[i] - avg_latencies[i];
            variances[i] += diff * diff;
        }
    }
    let std_devs_ms: Vec<f64> = variances
        .into_iter()
        .map(|v| ((v / requests_per_client) as f64 / 1000.).sqrt())
        .collect();
    let avg_latencies_ms = avg_latencies
        .into_iter()
        .map(|l| l as f64 / 1000.)
        .collect();
    (avg_latencies_ms, std_devs_ms)
}

// // Function to serialize client data to JSON and compress with gzip
// fn compress_raw_output(data: &RawOutput) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
//     // Step 1: Serialize the entire struct to JSON
//     let json_representation = serde_json::to_string(data)?;
//
//     // Step 2: Compress the JSON representation using Gzip
//     let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
//     encoder.write_all(json_representation.as_bytes())?;
//     let compressed_data = encoder.finish()?;
//
//     Ok(compressed_data)
// }
