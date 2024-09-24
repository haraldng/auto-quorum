use chrono::Utc;
use futures::SinkExt;
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
use histogram::Histogram;

const PERCENTILES: [f64; 6] = [50.0, 70.0, 80.0, 90.0, 95.0, 99.0];

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

#[derive(Debug, Serialize, Deserialize)]
pub struct ClientConfig {
    cluster_name: String,
    location: String,
    pub server_id: u64,
    local_deployment: Option<bool>,
    kill_signal_sec: Option<u64>,
    pub scheduled_start_utc_ms: Option<i64>,
    pub use_metronome: Option<usize>,
    pub req_batch_size: Option<usize>,
    pub(crate) interval_ms: Option<u64>,
    pub(crate) iterations: Option<usize>,
    pub storage_duration_micros: Option<usize>,
    pub nodes: Option<Vec<usize>>,
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
    server: ServerConnection,
    command_id: CommandId,
    request_data: Vec<RequestData>,
    kill_signal_sec: Option<u64>,
    req_batch_size: usize,
    batch_interval: Duration,
    iterations: usize,
    current_iteration: usize,
    num_responses: usize,
}

async fn get_server_connection(server_address: SocketAddr) -> ServerConnection {
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
                let underlying_stream = registration_connection.into_inner().into_inner();
                break frame_clients_connection(underlying_stream);
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
        let server = get_server_connection(server_address).await;
        Self {
            server,
            command_id: 0,
            request_data: Vec::with_capacity(8000),
            kill_signal_sec: config.kill_signal_sec,
            req_batch_size: config.req_batch_size.expect("Batch size not set"),
            batch_interval: Duration::from_millis(config.interval_ms.expect("Interval not set")),
            iterations: config.iterations.expect("Iterations not set"),
            current_iteration: 0,
            num_responses: 0,
        }
    }

    pub async fn put(&mut self, key: String, value: String) {
        self.send_command(KVCommand::Put(key, value)).await;
    }

    pub async fn delete(&mut self, key: String) {
        self.send_command(KVCommand::Delete(key)).await;
    }

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
        // TODO: Get client ID from handshake
        let request = ClientMessage::Append(0, self.command_id, command);
        let data = RequestData {
            time_sent_utc: Utc::now().timestamp_micros(),
            response: None,
        };
        self.request_data.push(data);
        self.command_id += 1;
        if let Err(e) = self.server.send(request).await {
            log::error!("Couldn't send command to server: {e}");
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
        let num_requests = self.request_data.len();
        let mut missed_responses = 0;
        let mut dropped_sequence: usize = 0;
        let mut histo = Histogram::new(7, 32).unwrap();
        let mut latency_sum = 0;
        let mut num_responses = 0;
        for request_data in &self.request_data {
            let response_time = request_data.response_time();
            match response_time {
                Some(latency) => {
                    histo.increment(latency as u64).unwrap();
                    latency_sum += latency;
                    num_responses += 1;
                    // if dropped_sequence > 0 {
                    //     println!("dropped requests: {dropped_sequence}");
                    //     dropped_sequence = 0;
                    // }
                    // if latency > 10 {
                    //     println!("SLOW REQUEST {request_data:?}");
                    // }
                    println!(
                        "request: {:?}, latency: {:?}, sent: {:?}",
                        request_data.response.as_ref().unwrap().message.command_id(),
                        latency,
                        request_data.time_sent_utc
                    );
                }
                None => {
                    missed_responses += 1;
                    dropped_sequence += 1;
                }
            }
        }
        let avg_latency = (latency_sum as f64 / 1000.) / num_responses as f64;
        let variance = self
            .request_data
            .iter()
            .filter_map(|data| data.response_time())
            .map(|value| {
                let diff = (value as f64 / 1000.) - avg_latency;
                diff * diff
            })
            .sum::<f64>()
            / num_responses as f64;
        let std_dev = variance.sqrt();
        if dropped_sequence > 0 {
            println!("dropped requests: {dropped_sequence}");
        }
        let duration_s = self.iterations as f64 * self.batch_interval.as_secs_f64();
        let throughput = if num_responses <= missed_responses {
            eprintln!(
                "More dropped requests ({num_responses}) than completed requests ({num_responses})"
            );
            0.0
        } else {
            (num_responses - missed_responses) as f64 / duration_s
        };
        let res_str = format!(
            "Avg latency: {avg_latency} ms, Std dev: {std_dev} ms Throughput: {throughput} ops/s, num requests: {num_requests}, missed: {missed_responses}"
        );
        println!("{res_str}");
        eprintln!("{res_str}");

        let mut p_str = String::from("Latency percentiles:");
        for (percentile, bucket) in histo.percentiles(&PERCENTILES).unwrap().unwrap() {
            p_str.push_str(&format!("\np{percentile}: {:?},", bucket));
        }
        println!("{p_str}");
        eprintln!("{p_str}");

        // Batch latency
        //
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
        let res_str = format!(
            "Avg batch latency: {} ms, Batch std dev: {} ms",
            avg_batch_latency / 1000.,
            std_dev / 1000.,
        );
        println!("{res_str}");
        eprintln!("{res_str}");
    }
}
