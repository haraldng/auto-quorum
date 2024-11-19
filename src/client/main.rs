use chrono::Utc;
use client::ClosedLoopClient;
use configs::{ClientConfig, RequestModeConfig};
use open_loop_client::OpenLoopClient;
use std::{env, fs};
use tokio::time::Duration;
use toml;

mod client;
mod configs;
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
    let config_file = match env::var("CONFIG_FILE") {
        Ok(file_path) => file_path,
        Err(_) => panic!("Requires CONFIG_FILE environment variable"),
    };
    let config_string = match fs::read_to_string(config_file.clone()) {
        Ok(string) => string,
        Err(e) => panic!("Couldn't read config file {config_file}: {e}"),
    };
    let client_config: ClientConfig = match toml::from_str(&config_string) {
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
