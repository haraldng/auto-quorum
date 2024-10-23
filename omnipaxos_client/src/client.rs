use chrono::{DateTime, Utc};
use futures::{SinkExt, StreamExt};
use log::*;
use omnipaxos::util::FlexibleQuorum;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::time::Duration;

use tokio::net::TcpStream;
use tokio::time::interval;

use common::util::{
    frame_clients_connection, frame_registration_connection, get_node_addr, FromServerConnection,
    ToServerConnection,
};
use common::{kv::*, messages::*};

// TODO: Server config parameters like flexible_quorum should be taken from server, since
// there may be a mismatch between the client's config and the servers' configs.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ClientConfig {
    pub cluster_name: String,
    pub location: String,
    pub server_id: u64,
    pub local_deployment: Option<bool>,
    pub scheduled_start_utc_ms: Option<i64>,
    pub use_metronome: Option<usize>,
    pub metronome_quorum_size: Option<usize>,
    pub flexible_quorum: Option<FlexibleQuorum>,
    pub req_batch_size: Option<usize>,
    pub interval_ms: Option<u64>,
    pub iterations: Option<usize>,
    pub storage_duration_micros: Option<usize>,
    pub data_size: Option<usize>,
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
    batch_position_std_dev: Vec<f64>,
}

pub struct Client {
    // id: ClientId,
    // server: ServerConnection,
    // command_id: CommandId,
    // request_data: Vec<RequestData>,
    // req_batch_size: usize,
    // batch_interval: Duration,
    // iterations: usize,
    // current_iteration: usize,
    // num_responses: usize,
    // config: ClientConfig,
}

async fn get_server_connection(
    server_address: SocketAddr,
) -> ((FromServerConnection, ToServerConnection), ClientId) {
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
                break ((frame_clients_connection(underlying_stream)), assigned_id);
            }
            Err(e) => eprintln!("Unable to connect to server: {e}"),
        }
    }
}

impl Client {
    pub async fn run(config: ClientConfig) {
        // Get connection to server
        let is_local = config.local_deployment.unwrap_or(false);
        let server_address = get_node_addr(&config.cluster_name, config.server_id, is_local)
            .expect("Couldn't resolve server IP");
        let ((mut from_server_conn, to_server_conn), assigned_id) =
            get_server_connection(server_address).await;
        // Complete handshake with server
        let first_msg = from_server_conn.next().await.unwrap();
        match first_msg.unwrap() {
            ServerMessage::Ready => (),
            _ => panic!("Recieved unexpected message during handshake"),
        }
        // Spawn reader and writer actors
        let batch_size = config.req_batch_size.expect("Batch size not configured");
        let batch_delay =
            Duration::from_millis(config.interval_ms.expect("Interval not configured"));
        let num_iterations = config.iterations.expect("Iterations not configured");
        let num_of_requests = batch_size * num_iterations;
        let reader_task = tokio::spawn(Self::reader_actor(from_server_conn, num_of_requests));
        let writer_task = tokio::spawn(Self::writer_actor(
            to_server_conn,
            assigned_id,
            batch_size,
            batch_delay,
            num_iterations,
        ));
        // Collect request data
        let (request_data, response_data) = tokio::join!(writer_task, reader_task);
        let request_data = request_data.expect("Error collecting requests");
        let response_data = response_data.expect("Error collecting responses");
        Self::print_results(config, request_data, response_data);
    }

    async fn reader_actor(
        from_server_conn: FromServerConnection,
        num_responses: usize,
    ) -> Vec<(CommandId, DateTime<Utc>)> {
        let mut response_data = Vec::with_capacity(num_responses);
        let mut buf_reader = from_server_conn.ready_chunks(100);
        while let Some(messages) = buf_reader.next().await {
            for msg in messages {
                match msg {
                    Ok(ServerMessage::Ready) => panic!("Recieved unexpected message: {msg:?}"),
                    Ok(response) => {
                        response_data.push((response.command_id(), Utc::now()));
                    }
                    Err(err) => panic!("Error deserializing message: {err:?}"),
                }
            }
            if response_data.len() >= num_responses {
                if response_data.len() > num_responses {
                    warn!(
                        "Expected {} responses from server, but got {} responses",
                        num_responses,
                        response_data.len()
                    );
                }
                return response_data;
            }
        }
        eprintln!("Finished collecting responses");
        return response_data;
    }

    async fn writer_actor(
        mut to_server_conn: ToServerConnection,
        client_id: ClientId,
        batch_size: usize,
        batch_delay: Duration,
        num_iterations: usize,
    ) -> Vec<DateTime<Utc>> {
        let mut request_id = 0;
        let mut request_data = Vec::with_capacity(batch_size * num_iterations);
        let mut batch_interval = interval(batch_delay);
        for _ in 0..num_iterations {
            let _ = batch_interval.tick().await;
            for _ in 0..batch_size {
                let key = request_id.to_string();
                let cmd = KVCommand::Put(key.clone(), key);
                let request = ClientMessage::Append(client_id, request_id, cmd);
                request_id += 1;
                if let Err(e) = to_server_conn.send(request).await {
                    error!("Couldn't send command to server: {e}");
                }
                request_data.push(Utc::now());
            }
        }
        to_server_conn.send(ClientMessage::Done).await.unwrap();
        eprintln!("Finished sending requests");
        return request_data;
    }

    fn print_results(
        config: ClientConfig,
        request_data: Vec<DateTime<Utc>>,
        response_data: Vec<(CommandId, DateTime<Utc>)>,
    ) {
        let response_latencies_μs = response_data
            .into_iter()
            .map(|(cmd_id, response_time)| {
                (response_time - request_data[cmd_id as usize])
                    .num_microseconds()
                    .expect("UTC overflow")
            })
            .collect::<Vec<i64>>();
        let batch_size = config.req_batch_size.unwrap();
        let num_requests = config.iterations.unwrap() * batch_size;

        let throughput = calc_request_throughput(&config);
        let (request_latency_average, request_latency_std_dev) =
            calc_avg_response_latency(&response_latencies_μs);
        let (batch_latency_average, batch_latency_std_dev) =
            calc_avg_batch_latency(&response_latencies_μs, batch_size);
        let (batch_position_latency, batch_position_std_dev) =
            calc_avg_batch_latency_by_position(&response_latencies_μs, batch_size);
        let output = ClientOutput {
            client_config: config,
            throughput,
            num_requests,
            missed_responses: 0,
            request_latency_average,
            request_latency_std_dev,
            batch_latency_average,
            batch_latency_std_dev,
            batch_position_latency,
            batch_position_std_dev,
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
}

fn calc_request_throughput(config: &ClientConfig) -> f64 {
    let request_duration_sec =
        config.iterations.unwrap() as f64 * (config.interval_ms.unwrap() as f64 / 1000.);
    let num_requests = config.req_batch_size.unwrap() * config.iterations.unwrap();
    num_requests as f64 / request_duration_sec
}

// The (average, std dev) response latency of all requests
fn calc_avg_response_latency(latencies: &Vec<i64>) -> (f64, f64) {
    let num_responses = latencies.len() as f64;
    let avg_latency = latencies.iter().sum::<i64>() as f64 / num_responses;
    let variance = latencies
        .iter()
        .map(|&value| {
            let diff = (value as f64) - avg_latency;
            diff * diff
        })
        .sum::<f64>()
        / num_responses;
    let std_dev = variance.sqrt();
    (avg_latency / 1000., std_dev / 1000.)
}

// The (average, std dev) respose latency for each batch of requests
fn calc_avg_batch_latency(latencies: &Vec<i64>, batch_size: usize) -> (f64, f64) {
    let batch_latencies: Vec<i64> = latencies
        .chunks(batch_size)
        .map(|batch| *batch.into_iter().max().unwrap())
        .collect();
    let num_batches = batch_latencies.len() as f64;
    let avg_batch_latency = batch_latencies.iter().sum::<i64>() as f64 / num_batches;
    let variance = batch_latencies
        .into_iter()
        .map(|value| {
            let diff = (value as f64) - avg_batch_latency;
            diff * diff
        })
        .sum::<f64>()
        / num_batches;
    let std_dev = variance.sqrt();
    (avg_batch_latency / 1000., std_dev / 1000.)
}

// The average latency for each response by position in request batch
fn calc_avg_batch_latency_by_position(
    latencies: &Vec<i64>,
    batch_size: usize,
) -> (Vec<f64>, Vec<f64>) {
    let batch_count = (latencies.len() / batch_size) as f64;
    let mut sums = vec![0i64; batch_size];
    for batch in latencies.chunks(batch_size) {
        for i in 0..batch_size {
            sums[i] += batch[i];
        }
    }
    let avg_latencies: Vec<f64> = sums
        .iter()
        .map(|&sum| (sum as f64 / 1000.) / batch_count)
        .collect();
    let mut variances = vec![0f64; batch_size];
    for batch in latencies.chunks(batch_size) {
        for i in 0..batch_size {
            let diff = (batch[i] as f64 / 1000.) - avg_latencies[i];
            variances[i] += diff * diff;
        }
    }
    let std_devs = variances
        .into_iter()
        .map(|v| (v / batch_count).sqrt())
        .collect();
    (avg_latencies, std_devs)
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
