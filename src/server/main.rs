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
    env_logger::init();
    let config_file = match env::var("CONFIG_FILE") {
        Ok(file_path) => file_path,
        Err(_) => panic!("Requires CONFIG_FILE environment variable"),
    };
    let config_string = fs::read_to_string(config_file).unwrap();
    let metronome_config: MetronomeServerConfig = match toml::from_str(&config_string) {
        Ok(parsed_config) => parsed_config,
        Err(e) => panic!("{e}"),
    };
    log::info!("Starting server");
    let mut server = MetronomeServer::new(metronome_config).await;
    server.run().await;
}
