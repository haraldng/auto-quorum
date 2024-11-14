use crate::server::OmniPaxosServer;
use chrono::Local;
use env_logger::Builder;
use log::LevelFilter;
use metronome::common::configs::OmniPaxosServerConfig;
use omnipaxos::{errors::ConfigError, OmniPaxosConfig};
use std::{env, fs, io::Write};
use toml;

mod database;
mod network;
mod server;

#[tokio::main]
pub async fn main() {
    let log_arg = env::var("RUST_LOG").unwrap_or("warn".to_string());
    let log_level = match log_arg.as_str() {
        "error" => LevelFilter::Error,
        "warn" => LevelFilter::Warn,
        "info" => LevelFilter::Info,
        "debug" => LevelFilter::Debug,
        "trace" => LevelFilter::Trace,
        _ => unreachable!("Incorrect RUST_LOG value"),
    };
    match log_level {
        LevelFilter::Debug | LevelFilter::Trace => {
            Builder::new()
                .format(|buf, record| {
                    // Use chrono to get the current time with milliseconds
                    let timestamp = Local::now().timestamp_subsec_micros();
                    writeln!(
                        buf,
                        "[{}] {} - {}",
                        timestamp as f64 / 1000.,
                        record.level(),
                        record.args()
                    )
                })
                .filter(None, log_level)
                .init();
        }
        _ => env_logger::init(),
    }
    let config_file = match env::var("CONFIG_FILE") {
        Ok(file_path) => file_path,
        Err(_) => panic!("Requires CONFIG_FILE environment variable"),
    };
    let omnipaxos_config = match OmniPaxosConfig::with_toml(&config_file) {
        Ok(parsed_config) => parsed_config,
        Err(ConfigError::Parse(e)) => panic!("{e}",),
        Err(e) => panic!("{e}"),
    };
    let config_string = fs::read_to_string(config_file).unwrap();
    let server_config: OmniPaxosServerConfig = match toml::from_str(&config_string) {
        Ok(parsed_config) => parsed_config,
        Err(e) => panic!("{e}"),
    };
    let max_node_pid = omnipaxos_config.cluster_config.nodes.iter().max().unwrap();
    let initial_leader = server_config.initial_leader.unwrap_or(*max_node_pid);
    // println!("{}", serde_json::to_string(&server_config).unwrap());
    let mut server = OmniPaxosServer::new(server_config, omnipaxos_config, initial_leader).await;
    server.run().await;
}
