use chrono::Utc;
use futures::SinkExt;
use serde::{Deserialize, Serialize};
use std::time::Duration;

use tokio::net::TcpStream;
use tokio::time::{interval};
use tokio_stream::StreamExt;

use common::util::{get_node_addr, wrap_stream, Connection as ServerConnection};
use common::{kv::*, messages::*};

#[derive(Debug, Serialize)]
struct RequestData {
    time_sent_utc: i64,
    response: Option<Response>,
}

impl RequestData {
    fn response_time(&self) -> Option<i64> {
        self.response.as_ref().map(|r| r.time_recieved_utc - self.time_sent_utc)
    }
}

#[derive(Debug, Serialize)]
struct Response {
    time_recieved_utc: i64,
    message: ServerMessage,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ClientConfig {
    location: String,
    pub server_id: u64,
    pub request_rate_intervals: Vec<RequestInterval>,
    local_deployment: Option<bool>,
    kill_signal_sec: Option<u64>,
    pub scheduled_start_utc_ms: Option<i64>,
    pub use_metronome: Option<bool>,
}

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

pub struct Client {
    server: ServerConnection,
    command_id: CommandId,
    request_data: Vec<RequestData>,
    request_rate_intervals: Vec<RequestInterval>,
    kill_signal_sec: Option<u64>,
    ops_per_interval: usize,
}

impl Client {
    pub async fn with(config: ClientConfig) -> Self {
        let is_local = config.local_deployment.unwrap_or(false);
        let server_address =
            get_node_addr(config.server_id, is_local).expect("Couldn't resolve server IP");
        let server_stream = TcpStream::connect(server_address)
            .await
            .expect("Couldn't connect to server {server_id}");
        server_stream.set_nodelay(true).unwrap();
        let mut client = Self {
            server: wrap_stream(server_stream),
            command_id: 0,
            request_data: Vec::with_capacity(8000),
            request_rate_intervals: config.request_rate_intervals,
            kill_signal_sec: config.kill_signal_sec,
            ops_per_interval: 1
        };
        client.send_registration().await;
        client
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
        if self.request_rate_intervals.is_empty() {
            return;
        }
        let intervals = self.request_rate_intervals.clone();
        assert_eq!(intervals.len(), 1, "Should only use one interval in metronome");
        let mut intervals = intervals.iter();
        let first_interval = intervals.next().unwrap();
        let (req_interval, ops_per_interval) = first_interval.get_request_delay();
        self.ops_per_interval = ops_per_interval;
        let mut request_interval = interval(req_interval);
        let mut next_interval = interval(first_interval.get_interval_duration());
        request_interval.tick().await;
        next_interval.tick().await;

        loop {
            tokio::select! {
                biased;
                // Handle the next server message when it arrives
                Some(msg) = self.server.next() => self.handle_response(msg.unwrap()),
                // Send request according to rate of current request interval setting. (defined in
                    // TOML config)
                _ = request_interval.tick() => {
                    if self.command_id >= 50 {
                        continue;
                    }
                    for _ in 0..self.ops_per_interval {
                        let key = self.command_id.to_string();
                        self.put(key.clone(), key).await;
                    }
                },
                // Go to the next request interval setting. (defined in TOML config)
                _ = next_interval.tick() => {
                    break;
                },
            }
        }
        self.print_results();
    }

    async fn send_command(&mut self, command: KVCommand) {
        let request = ClientMessage::Append(self.command_id, command);
        let data = RequestData {
            time_sent_utc: Utc::now().timestamp_millis(),
            response: None,
        };
        self.request_data.push(data);
        self.command_id += 1;
        if let Err(e) = self
            .server
            .send(NetworkMessage::ClientMessage(request))
            .await
        {
            log::error!("Couldn't send command to server: {e}");
        }
    }

    fn handle_response(&mut self, msg: NetworkMessage) {
        match msg {
            NetworkMessage::ServerMessage(response) => {
                let cmd_id = response.command_id();
                let response_time = Utc::now().timestamp_millis();
                self.request_data[cmd_id].response = Some(Response {
                    time_recieved_utc: response_time,
                    message: response,
                });
            }
            _ => panic!("Recieved unexpected message: {msg:?}"),
        }
    }

    async fn send_registration(&mut self) {
        self.server
            .send(NetworkMessage::ClientRegister)
            .await
            .expect("Couldn't send message to server");
    }

    async fn send_kill_signal(&mut self) {
        self.server
            .send(NetworkMessage::KillServer)
            .await
            .expect("Couldn't send message to server")
    }

    fn print_results(&self) {
        let num_requests = self.request_data.len();
        let mut all_latencies = Vec::with_capacity(self.request_data.len());
        let mut missed_responses = 0;
        let mut dropped_sequence: usize = 0;
        for request_data in &self.request_data {
            let response_time = request_data.response_time();
            match response_time {
                Some(latency) => {
                    all_latencies.push(latency);
                    if dropped_sequence > 0 {
                        println!("dropped requests: {dropped_sequence}");
                        dropped_sequence = 0;
                    }
                    println!("request: {:?}, latency: {:?}", request_data.response.as_ref().unwrap().message, latency);
                },
                None => {
                    missed_responses += 1;
                    dropped_sequence += 1;
                },
            }
        }
        if dropped_sequence > 0 {
            println!("dropped requests: {dropped_sequence}");
        }
        let avg_latency = all_latencies.iter().sum::<i64>() as f64 / all_latencies.len() as f64;
        let duration_s = self.request_rate_intervals.first().unwrap().duration_sec as f64;
        let num_responses = all_latencies.len();
        let throughput = if num_responses <= missed_responses {
            eprintln!("More dropped requests ({missed_responses}) than completed requests ({num_responses})");
            0.0
        } else {
            (num_responses - missed_responses) as f64 / duration_s
        };
        println!("Avg latency: {avg_latency} ms, Throughput: {throughput} ops/s, num requests: {num_requests}, missed: {missed_responses}");
        eprintln!("Avg latency: {avg_latency} ms, Throughput: {throughput} ops/s, num requests: {num_requests}, missed: {missed_responses}");
    }
}
