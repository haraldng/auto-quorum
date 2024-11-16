use crate::{configs::MetronomeServerConfig, server::MetronomeServer};
use chrono::Local;
use env_logger::Builder;
use log::LevelFilter;
use std::{env, fs, io::Write};
use toml;

mod configs;
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
    let config_string = fs::read_to_string(config_file).unwrap();
    let metronome_config: MetronomeServerConfig = match toml::from_str(&config_string) {
        Ok(parsed_config) => parsed_config,
        Err(e) => panic!("{e}"),
    };
    let mut server = MetronomeServer::new(metronome_config).await;
    server.run().await;
}
