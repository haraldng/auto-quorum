use chrono::Utc;
use client::ClosedLoopClient;
use configs::{ClientConfig, RequestModeConfig};
use open_loop_client::OpenLoopClient;
use tokio::time::Duration;

mod client;
mod configs;
mod data_collection;
mod network;
mod open_loop_client;

// Wait until the scheduled start time to synchronize client starts.
// If start time has already passed, start immediately.
#[allow(dead_code)]
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
    env_logger::init();
    let client_config = match ClientConfig::new() {
        Ok(parsed_config) => parsed_config,
        Err(e) => panic!("{e}"),
    };
    match &client_config.request_mode_config {
        RequestModeConfig::ClosedLoop(_) => {
            let mut client = ClosedLoopClient::new(client_config).await;
            client.run().await;
        }
        RequestModeConfig::OpenLoop(_, _) => {
            let mut client = OpenLoopClient::new(client_config).await;
            client.run().await;
        }
    }
}
