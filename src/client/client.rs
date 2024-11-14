use crate::network::Network;
use chrono::Utc;
use csv::Writer;
use futures::StreamExt;
use log::*;
use metronome::common::{kv::*, messages::*};
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
    pub output_filepath: String,
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
    client_data: ClientData,
    config: ClientConfig,
    leaders_config: Option<MetronomeConfigInfo>,
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
            client_data: ClientData::new(num_estimated_responses),
            config,
            leaders_config: None,
        }
    }

    pub async fn run(&mut self) {
        // Wait until server is ready
        info!("Waiting for ready signal from server");
        let first_msg = self.network.next().await;
        match first_msg {
            Some(ServerMessage::Ready(server_config)) => {
                self.leaders_config = Some(server_config);
            }
            Some(m) => panic!("Recieved unexpected message during handshake: {m:?}"),
            None => panic!("Lost connection to server during handshake"),
        }

        // Send initial requests
        info!("Starting requests");
        self.client_data.experiment_start();
        for request_id in 0..self.num_parallel_requests {
            self.send_request(request_id);
        }

        // Send requests and collect responses
        match self.end_condition {
            EndCondition::ResponsesCollected(n) => self.run_until_response_limit(n).await,
            EndCondition::TimePassed(t) => self.run_until_duration_limit(t).await,
        }
        self.client_data.experiment_end();
        info!(
            "Client finished: collected {} responses",
            self.client_data.total_responses()
        );

        // Shutdown cluster and collect results
        self.network.send(ClientMessage::Done);
        self.client_data
            .save_summary(self.config.clone(), self.leaders_config.unwrap())
            .expect("Failed to write summary file");
        self.client_data
            .to_csv(self.config.output_filepath.clone())
            .expect("Failed to write output file");
    }

    // Send new requests on reponse until response limit is reached
    async fn run_until_response_limit(&mut self, response_limit: usize) {
        let mut response_count = 0;
        loop {
            match self.network.next().await {
                Some(message) => match message {
                    ServerMessage::Ready(_) => error!("Unexpected ready message"),
                    msg => {
                        debug!("Received {msg:?}");
                        let response_id = msg.command_id();
                        self.client_data.new_response(response_id);
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
                                self.client_data.new_response(response_id);
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

    fn send_request(&mut self, request_id: CommandId) {
        let key = request_id.to_string();
        let cmd = KVCommand::Put(key.clone(), key);
        let request = ClientMessage::Append(request_id, cmd);
        debug!("Sending request {request:?}");
        self.network.send(request);
        self.client_data.new_request(request_id);
    }
}

#[derive(Debug, Serialize)]
struct ClientSummary {
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

struct ClientData {
    response_data: Vec<ResponseData>,
    request_count: usize,
    response_count: usize,
    client_start_time: Option<Timestamp>,
    client_end_time: Option<Timestamp>,
}

#[derive(Debug, Serialize)]
struct ResponseData {
    command_id: CommandId,
    request_time: Option<Timestamp>,
    response_time: Option<Timestamp>,
}

impl ClientData {
    fn new(num_estimated_responses: usize) -> Self {
        Self {
            response_data: Vec::with_capacity(num_estimated_responses),
            request_count: 0,
            response_count: 0,
            client_start_time: None,
            client_end_time: None,
        }
    }

    fn experiment_start(&mut self) {
        self.client_start_time = Some(Utc::now().timestamp_micros());
    }
    fn experiment_end(&mut self) {
        self.client_end_time = Some(Utc::now().timestamp_micros());
    }

    // A Client can send requests with out-of-order command ids due to parallel requests.
    // Use a vec with placeholder values to fill in the gaps.
    #[inline]
    fn new_request(&mut self, request_id: CommandId) {
        let request_time = Utc::now().timestamp_micros();
        if request_id < self.response_data.len() {
            self.response_data[request_id].request_time = Some(request_time);
        } else {
            for placeholder_id in self.response_data.len()..request_id {
                let placeholder = ResponseData {
                    command_id: placeholder_id,
                    request_time: None,
                    response_time: None,
                };
                self.response_data.push(placeholder);
            }
            self.response_data.push(ResponseData {
                command_id: request_id,
                request_time: Some(request_time),
                response_time: None,
            });
        }
        self.request_count += 1;
    }

    #[inline]
    fn new_response(&mut self, response_id: CommandId) {
        let response_time = Utc::now().timestamp_micros();
        // Placeholders will ensure indexing works
        self.response_data[response_id].response_time = Some(response_time);
        self.response_count += 1;
    }

    fn save_summary(
        &self,
        client_config: ClientConfig,
        server_info: MetronomeConfigInfo,
    ) -> Result<(), std::io::Error> {
        let summary_filepath = client_config.summary_filepath.clone();
        let (request_latency_average, request_latency_std_dev) = self.avg_response_latency();
        let summary = ClientSummary {
            client_config,
            server_info,
            client_start_time: self
                .client_start_time
                .expect("Should create summary after experiment start"),
            throughput: self.throughput(),
            total_responses: self.total_responses(),
            unanswered_requests: self.unanswered_requests(),
            total_time: self.total_time(),
            request_latency_average,
            request_latency_std_dev,
        };
        let json_summary = serde_json::to_string_pretty(&summary)?;
        eprintln!("{json_summary}");
        let mut summary_file = File::create(summary_filepath)?;
        summary_file.write_all(json_summary.as_bytes())?;
        summary_file.flush()?;
        Ok(())
    }

    fn to_csv(&self, file_path: String) -> Result<(), std::io::Error> {
        let file = File::create(file_path)?;
        let mut writer = Writer::from_writer(file);
        for data in &self.response_data {
            writer.serialize(data)?;
        }
        writer.flush()?;
        Ok(())
    }

    // The (average, std dev) response latency of all requests in milliseconds
    fn avg_response_latency(&self) -> (f64, f64) {
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

    // The request throughput in responses/sec
    fn throughput(&self) -> f64 {
        let experiment_duration = self
            .client_end_time
            .expect("Should calculate throughput after experiment end")
            - self
                .client_start_time
                .expect("Should calculate throughput after experiment start");
        let throughput = (self.request_count as f64 / experiment_duration as f64) * 1_000_000.;
        return throughput;
    }

    // The total duration of request time in milliseconds
    fn total_time(&self) -> f64 {
        let first_request_time = self.response_data[0]
            .request_time
            .expect("First request shouldn't be a placeholder");
        let last_response_time = self
            .response_data
            .iter()
            .rev()
            .filter_map(|d| d.response_time)
            .next()
            .expect("There should be responses");
        (last_response_time - first_request_time) as f64 / 1000.
    }

    fn total_responses(&self) -> usize {
        self.response_count
    }

    fn unanswered_requests(&self) -> usize {
        self.request_count - self.response_count
    }
}
