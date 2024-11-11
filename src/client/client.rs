use crate::network::Network;
use auto_quorum::common::{kv::*, messages::*};
use chrono::Utc;
use csv::Writer;
use futures::StreamExt;
use log::*;
use serde::{Deserialize, Serialize};
use std::{fs::File, io::Write, time::Duration};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ClientConfig {
    pub cluster_name: String,
    pub location: String,
    pub server_id: NodeId,
    pub local_deployment: Option<bool>,
    pub end_condition: EndConditionConfig,
    pub num_parallel_requests: usize,
    pub summary_filepath: String,
    pub debug_filepath: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
#[serde(tag = "end_condition_type", content = "end_condition_value")]
pub enum EndConditionConfig {
    ResponsesCollected(usize),
    SecondsPassed(u64),
}

#[derive(Debug, Clone, Copy)]
enum EndCondition {
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

type Timestamp = i64;

pub struct Client {
    end_condition: EndCondition,
    num_parallel_requests: usize,
    network: Network,
    response_data: Vec<ResponseData>,
    config: ClientConfig,
    leaders_config: Option<MetronomeConfigInfo>,
    client_start_time: Option<Timestamp>,
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
        let num_estimated_responses = match config.end_condition {
            EndConditionConfig::ResponsesCollected(n) => n,
            EndConditionConfig::SecondsPassed(_secs) => 10_000,
        };
        Client {
            end_condition: config.end_condition.into(),
            num_parallel_requests: config.num_parallel_requests,
            network,
            response_data: Vec::with_capacity(num_estimated_responses),
            config,
            leaders_config: None,
            client_start_time: None,
        }
    }

    pub async fn run(&mut self) {
        // Wait until server is ready
        let first_msg = self.network.next().await;
        match first_msg {
            Some(ServerMessage::Ready(server_config)) => {
                self.leaders_config = Some(server_config);
            }
            Some(m) => panic!("Recieved unexpected message during handshake: {m:?}"),
            None => panic!("Lost connection to server during handshake"),
        }

        // Send initial requests
        self.client_start_time = Some(Utc::now().timestamp_micros());
        for request_id in 0..self.num_parallel_requests {
            self.send_request(request_id);
        }

        // Send requests and collect responses
        match self.end_condition {
            EndCondition::ResponsesCollected(n) => self.run_until_response_limit(n).await,
            EndCondition::TimePassed(t) => self.run_until_duration_limit(t).await,
        }
        info!("Client finished collecting responses");

        // Shutdown cluster and collect results
        self.network.send(ClientMessage::Done);
        self.print_results();
    }

    // Send new requests on reponse until response_limit responses are recheived
    async fn run_until_response_limit(&mut self, response_limit: usize) {
        let mut response_count = 0;
        loop {
            match self.network.next().await {
                Some(message) => match message {
                    ServerMessage::Ready(_) => error!("Unexpected ready message"),
                    msg => {
                        debug!("Received {msg:?}");
                        let response_id = msg.command_id();
                        let response_time = Utc::now().timestamp_micros();
                        self.response_data[response_id].response_time = Some(response_time);
                        response_count += 1;
                        if response_count >= response_limit {
                            break;
                        }
                        let next_request_id = response_id + self.num_parallel_requests;
                        self.send_request(next_request_id);
                    }
                },
                None => panic!("Connection to server lost before end condition"),
            }
        }
    }

    // Send new requests on reponse until duration_limit time has passed
    async fn run_until_duration_limit(&mut self, duration_limit: Duration) {
        let mut end_duration = tokio::time::interval(duration_limit);
        end_duration.tick().await; // First tick resolves immediately
        loop {
            tokio::select! {
                biased;
                incoming_message = self.network.next() => {
                    match incoming_message {
                        Some(message) => match message {
                            ServerMessage::Ready(_) => error!("Unexpected ready message"),
                            msg => {
                                debug!("Received {msg:?}");
                                let response_id = msg.command_id();
                                let response_time = Utc::now().timestamp_micros();
                                self.response_data[response_id].response_time = Some(response_time);
                                let next_request_id = response_id + self.num_parallel_requests;
                                self.send_request(next_request_id);
                            }
                        },
                        None => panic!("Connection to server lost before end condition"),
                    }
                },
                _ = end_duration.tick() => break,
            }
        }
    }

    fn send_request(&mut self, request_id: usize) {
        let key = request_id.to_string();
        let cmd = KVCommand::Put(key.clone(), key);
        let request = ClientMessage::Append(request_id, cmd);
        debug!("Sending request {request:?}");
        self.network.send(request);
        let request_time = Utc::now().timestamp_micros();
        if request_id < self.response_data.len() {
            self.response_data[request_id].request_time = Some(request_time);
        } else {
            for i in self.response_data.len()..request_id {
                self.response_data.push(ResponseData::new_placeholder(i));
            }
            self.response_data
                .push(ResponseData::new(request_id, request_time));
        }
    }

    fn print_results(&self) {
        // Persist summary
        let total_time = self.calc_total_time();
        let (throughput, total_responses, unanswered_requests) =
            self.calc_request_throughput_and_missed();
        let (request_latency_average, request_latency_std_dev) = self.calc_avg_response_latency();
        let output = ClientOutput {
            client_config: self.config.clone(),
            server_info: self.leaders_config.unwrap(),
            client_start_time: self.client_start_time.unwrap(),
            throughput,
            total_responses,
            unanswered_requests,
            total_time,
            request_latency_average,
            request_latency_std_dev,
        };
        let json_output = serde_json::to_string_pretty(&output).unwrap();
        eprintln!("{json_output}");
        let mut summary_file = File::create(self.config.summary_filepath.clone()).unwrap();
        summary_file.write_all(json_output.as_bytes()).unwrap();
        summary_file.flush().unwrap();

        // Persist individual response times
        if let Some(file_path) = self.config.debug_filepath.clone() {
            let file = File::create(file_path).unwrap();
            let mut writer = Writer::from_writer(file);
            for data in &self.response_data {
                writer.serialize(data).unwrap();
            }
            writer.flush().unwrap();
        }
    }

    fn calc_total_time(&self) -> f64 {
        let first_request_time = self.response_data[0]
            .request_time
            .expect("First request was a placeholder");
        let last_response_time = self
            .response_data
            .iter()
            .rev()
            .filter_map(|d| d.response_time)
            .next()
            .expect("No responses exist");
        (last_response_time - first_request_time) as f64 / 1000.
    }

    fn calc_request_throughput_and_missed(&self) -> (f64, usize, usize) {
        let mut request_count = 0;
        let mut response_count = 0;
        for data in &self.response_data {
            if data.request_time.is_some() {
                request_count += 1;
            }
            if data.response_time.is_some() {
                response_count += 1;
            }
        }
        let first_request_time = self.response_data[0]
            .request_time
            .expect("First request was a placeholder");
        let last_request_time = self.response_data[self.response_data.len() - 1]
            .request_time
            .expect("Last request was a placeholder");
        let messaging_duration = last_request_time - first_request_time;
        let throughput = (request_count as f64 / messaging_duration as f64) * 1_000_000.;
        let unanswered_requests = request_count - response_count;
        return (throughput, response_count, unanswered_requests);
    }

    // The (average, std dev) response latency of all requests in milliseconds
    fn calc_avg_response_latency(&self) -> (f64, f64) {
        let latencies: Vec<i64> = self
            .response_data
            .iter()
            .filter_map(|d| {
                d.response_time.map(|resp_time| {
                    resp_time - d.request_time.expect("Got a response without a request")
                })
            })
            .collect();
        let num_responses = latencies.len();
        let avg_latency = latencies.iter().sum::<Timestamp>() / num_responses as i64;
        let variance = latencies
            .iter()
            .map(|&value| {
                let diff = value - avg_latency;
                diff * diff
            })
            .sum::<Timestamp>() as f64
            / num_responses as f64;
        let std_dev = variance.sqrt();
        (avg_latency as f64 / 1000., std_dev / 1000.)
    }
}

#[derive(Debug, Serialize)]
struct ClientOutput {
    client_config: ClientConfig,
    server_info: MetronomeConfigInfo,
    client_start_time: Timestamp,
    throughput: f64,
    total_responses: usize,
    unanswered_requests: usize,
    total_time: f64,
    request_latency_average: f64,
    request_latency_std_dev: f64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct ResponseData {
    command_id: CommandId,
    request_time: Option<Timestamp>,
    response_time: Option<Timestamp>,
}

impl ResponseData {
    fn new(command_id: CommandId, request_time: Timestamp) -> Self {
        Self {
            command_id,
            request_time: Some(request_time),
            response_time: None,
        }
    }

    fn new_placeholder(command_id: CommandId) -> Self {
        Self {
            command_id,
            request_time: None,
            response_time: None,
        }
    }
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
