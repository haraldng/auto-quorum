use chrono::Utc;
use futures::SinkExt;
use log::*;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::time::Duration;

use tokio::net::TcpStream;
use tokio::time::interval;
use tokio_stream::StreamExt;

use common::util::{
    frame_clients_connection, frame_registration_connection, get_node_addr, ServerConnection,
};
use common::{kv::*, messages::*};

#[derive(Debug, Serialize)]
struct RequestData {
    time_sent_utc: i64,
    response: Option<Response>,
}

impl RequestData {
    fn response_time(&self) -> Option<i64> {
        self.response
            .as_ref()
            .map(|r| r.time_recieved_utc - self.time_sent_utc)
    }
}

#[derive(Debug, Serialize)]
struct Response {
    time_recieved_utc: i64,
    message: ServerMessage,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ClientConfig {
    pub cluster_name: String,
    pub location: String,
    pub server_id: u64,
    pub local_deployment: Option<bool>,
    pub scheduled_start_utc_ms: Option<i64>,
    pub use_metronome: Option<usize>,
    pub req_batch_size: Option<usize>,
    pub interval_ms: Option<u64>,
    pub iterations: Option<usize>,
    pub storage_duration_micros: Option<usize>,
    pub nodes: Option<Vec<usize>>,
}

#[derive(Debug, Serialize)]
struct ClientOutput {
    client_config: ClientConfig,
    throughput: f64,
    num_requests: usize,
    missed_responses: usize,
    request_latency_average: f64,
    request_latency_std_dev: f64,
    batch_latency_average: f64,
    batch_latency_std_dev: f64,
    batch_position_latency: Vec<f64>,
}

/*
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct RequestInterval {
    duration_sec: u64,
    requests_per_sec: u64,
    read_ratio: f64,
}

impl RequestInterval {
    fn get_interval_duration(self) -> Duration {
        Duration::from_secs(self.duration_sec)
    }

    fn get_request_delay(self) -> (Duration, usize) {
        if self.requests_per_sec == 0 {
            return (Duration::from_secs(999999), 0);
        }
        if self.requests_per_sec > 1000 {
            let ops_per_ms = self.requests_per_sec / 1000;
            (Duration::from_millis(1), ops_per_ms as usize)
        } else {
            let delay_ms = 1000 / self.requests_per_sec;
            (Duration::from_millis(2000), 10)
        }
    }
}
*/

pub struct Client {
    id: ClientId,
    server: ServerConnection,
    command_id: CommandId,
    request_data: Vec<RequestData>,
    req_batch_size: usize,
    batch_interval: Duration,
    iterations: usize,
    current_iteration: usize,
    num_responses: usize,
    config: ClientConfig,
}

async fn get_server_connection(server_address: SocketAddr) -> (ServerConnection, ClientId) {
    let mut retry_connection = interval(Duration::from_secs(1));
    loop {
        retry_connection.tick().await;
        match TcpStream::connect(server_address).await {
            Ok(stream) => {
                stream.set_nodelay(true).unwrap();
                let mut registration_connection = frame_registration_connection(stream);
                registration_connection
                    .send(RegistrationMessage::ClientRegister)
                    .await
                    .expect("Couldn't send message to server");
                let first_msg = registration_connection.next().await.unwrap();
                let assigned_id = match first_msg.unwrap() {
                    RegistrationMessage::AssignedId(id) => id,
                    _ => panic!("Recieved unexpected message during handshake"),
                };
                let underlying_stream = registration_connection.into_inner().into_inner();
                break (frame_clients_connection(underlying_stream), assigned_id);
            }
            Err(e) => eprintln!("Unable to connect to server: {e}"),
        }
    }
}

impl Client {
    pub async fn with(config: ClientConfig) -> Self {
        let is_local = config.local_deployment.unwrap_or(false);
        let server_address = get_node_addr(&config.cluster_name, config.server_id, is_local)
            .expect("Couldn't resolve server IP");
        let (server, assigned_id) = get_server_connection(server_address).await;
        Self {
            id: assigned_id,
            server,
            command_id: 0,
            request_data: Vec::with_capacity(8000),
            req_batch_size: config.req_batch_size.expect("Batch size not set"),
            batch_interval: Duration::from_millis(config.interval_ms.expect("Interval not set")),
            iterations: config.iterations.expect("Iterations not set"),
            current_iteration: 0,
            num_responses: 0,
            config,
        }
    }

    pub async fn put(&mut self, key: String, value: String) {
        self.send_command(KVCommand::Put(key, value)).await;
    }

    #[allow(dead_code)]
    pub async fn delete(&mut self, key: String) {
        self.send_command(KVCommand::Delete(key)).await;
    }

    #[allow(dead_code)]
    pub async fn get(&mut self, key: String) {
        self.send_command(KVCommand::Get(key)).await;
    }

    pub async fn run(&mut self) {
        let first_msg = self.server.next().await.unwrap();
        match first_msg.unwrap() {
            ServerMessage::Ready => (),
            _ => panic!("Recieved unexpected message during handshake"),
        }

        let mut batch_interval = interval(self.batch_interval);
        batch_interval.tick().await;

        loop {
            tokio::select! {
                biased;
                // Handle the next server message when it arrives
                Some(msg) = self.server.next() => self.handle_response(msg.unwrap()),
                // Send request according to rate of current request interval setting. (defined in
                // TOML config)
                _ = batch_interval.tick() => {
                    if self.current_iteration == self.iterations {
                        if self.num_responses == self.request_data.len() {
                            break;
                        }
                    } else {
                        for _ in 0..self.req_batch_size {
                            let key = self.command_id.to_string();
                            self.put(key.clone(), key).await;
                        }
                        self.current_iteration += 1;
                    }
                },
            }
        }
        self.print_results();
    }

    async fn send_command(&mut self, command: KVCommand) {
        let request = ClientMessage::Append(self.id, self.command_id, command);
        let data = RequestData {
            time_sent_utc: Utc::now().timestamp_micros(),
            response: None,
        };
        self.request_data.push(data);
        self.command_id += 1;
        if let Err(e) = self.server.send(request).await {
            error!("Couldn't send command to server: {e}");
        }
    }

    fn handle_response(&mut self, msg: ServerMessage) {
        match msg {
            ServerMessage::Ready => panic!("Recieved unexpected message: {msg:?}"),
            response => {
                let cmd_id = response.command_id();
                let response_time = Utc::now().timestamp_micros();
                self.request_data[cmd_id].response = Some(Response {
                    time_recieved_utc: response_time,
                    message: response,
                });
                self.num_responses += 1;
            }
        }
    }

    fn print_results(&self) {
        let (request_latency_average, request_latency_std_dev) = self.calc_avg_response_latency();
        let throughput = self.calc_throughput();
        let num_requests = self.request_data.len();
        let missed_responses = self
            .request_data
            .iter()
            .filter(|r| r.response.is_none())
            .count();
        let (batch_latency_average, batch_latency_std_dev) = self.calc_avg_batch_latency();
        let batch_position_latency = self.calc_avg_batch_latency_by_position();
        let output = ClientOutput {
            client_config: self.config.clone(),
            throughput,
            num_requests,
            missed_responses,
            request_latency_average,
            request_latency_std_dev,
            batch_latency_average,
            batch_latency_std_dev,
            batch_position_latency,
        };
        let json_output = serde_json::to_string_pretty(&output).unwrap();
        println!("{json_output}\n");
        eprintln!("{json_output}");

        // Individual response times
        for (cmd_idx, request) in self.request_data.iter().enumerate() {
            match &request.response {
                Some(response) => println!(
                    "request: {:?}, latency: {:?}, sent: {:?}",
                    response.message.command_id(),
                    request.response_time().unwrap(),
                    request.time_sent_utc
                ),
                None => println!(
                    "request: {:?}, latency: NO RESPONSE, sent: {:?}",
                    cmd_idx, request.time_sent_utc
                ),
            }
        }
    }

    fn calc_throughput(&self) -> f64 {
        let num_responses = self
            .request_data
            .iter()
            .filter_map(|data| data.response_time())
            .count();
        let duration_sec = self.iterations as f64 * self.batch_interval.as_secs_f64();
        num_responses as f64 / duration_sec
    }

    // The (average, std dev) response latency of all requests
    fn calc_avg_response_latency(&self) -> (f64, f64) {
        let latencies: Vec<i64> = self
            .request_data
            .iter()
            .filter_map(|data| data.response_time())
            .collect();
        let num_responses = latencies.len();
        let avg_latency = latencies.iter().sum::<i64>() as f64 / num_responses as f64;
        let variance = latencies
            .into_iter()
            .map(|value| {
                let diff = (value as f64) - avg_latency;
                diff * diff
            })
            .sum::<f64>()
            / num_responses as f64;
        let std_dev = variance.sqrt();
        // Convert from microseconds to ms
        (avg_latency / 1000., std_dev / 1000.)
    }

    // The (average, std dev) respose latency for each batch of requests
    fn calc_avg_batch_latency(&self) -> (f64, f64) {
        let latencies: Vec<i64> = self
            .request_data
            .iter()
            .filter_map(|data| data.response_time())
            .collect();
        let batch_latencies: Vec<i64> = latencies
            .chunks(self.req_batch_size)
            .map(|batch| batch.into_iter().sum())
            .collect();
        let num_batches = batch_latencies.len();
        let avg_batch_latency = batch_latencies.iter().sum::<i64>() as f64 / num_batches as f64;
        let variance = batch_latencies
            .into_iter()
            .map(|value| {
                let diff = (value as f64) - avg_batch_latency;
                diff * diff
            })
            .sum::<f64>()
            / num_batches as f64;
        let std_dev = variance.sqrt();
        // Convert from microseconds to ms
        (avg_batch_latency / 1000., std_dev / 1000.)
    }

    // The average latency for each response by position in request batch
    fn calc_avg_batch_latency_by_position(&self) -> Vec<f64> {
        let latencies: Vec<i64> = self
            .request_data
            .iter()
            .filter_map(|data| data.response_time())
            .collect();
        let batch_count = latencies.len() / self.req_batch_size;
        let mut sums = vec![0i64; self.req_batch_size];
        for batch in latencies.chunks(self.req_batch_size) {
            for i in 0..self.req_batch_size {
                sums[i] += batch[i];
            }
        }
        let avg_latencies: Vec<f64> = sums
            .iter()
            .map(|&sum| (sum as f64 / 1000.) / batch_count as f64)
            .collect();
        avg_latencies
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
