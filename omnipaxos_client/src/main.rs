use chrono::Utc;
use client::{Client, ClientConfig};
use std::{env, fs};
use tokio::time::Duration;
use toml;
use itertools::Itertools;

mod client;

// Wait until the scheduled start time to synchronize client starts.
// If start time has already passed, start immediately.
async fn wait_until_sync_time(scheduled_start_utc_ms: Option<i64>) {
    if let Some(scheduled_start) = scheduled_start_utc_ms {
        let now = Utc::now();
        let milliseconds_until_sync = scheduled_start - now.timestamp_millis();
        println!("{{ \"sync_delay\": {milliseconds_until_sync} }}");
        // let sync_time = Utc::now() + chrono::Duration::milliseconds(milliseconds_until_sync);
        if milliseconds_until_sync > 0 {
            tokio::time::sleep(Duration::from_millis(milliseconds_until_sync as u64)).await;
        }
    }
}

#[tokio::main]
pub async fn main() {
    let config_file = match env::var("CONFIG_FILE") {
        Ok(file_path) => file_path,
        Err(_) => panic!("Requires CONFIG_FILE environment variable"),
    };
    let config_string = fs::read_to_string(config_file).unwrap();
    let mut client_config: ClientConfig = toml::from_str(&config_string).unwrap();
    let num_nodes = client_config.nodes.as_ref().unwrap().len();
    let majority = num_nodes / 2 + 1;
    let batch_size = match client_config.req_batch_size {
        Some(size) => size,
        None => (1..=num_nodes).combinations(majority).count(),
    };
    client_config.req_batch_size = Some(batch_size);
    println!(
        "Client: {}, Metronome: {}, storage_duration_micros: {}, req_batch_size: {}, interval_ms: {}, iterations: {}, N={}",
        client_config.server_id,
        client_config.use_metronome.unwrap(),
        client_config.storage_duration_micros.unwrap(),
        client_config.req_batch_size.unwrap(),
        client_config.interval_ms.unwrap(),
        client_config.iterations.unwrap(),
        client_config.nodes.as_ref().unwrap().len(),
    );
    // wait_until_sync_time(client_config.scheduled_start_utc_ms).await;
    let mut client = Client::with(client_config).await;
    client.run().await;
}
