use crate::server::{OmniPaxosServer, OmniPaxosServerConfig};
use env_logger;
use omnipaxos::OmniPaxosConfig;
use std::{env, fs};
use toml;

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
    let omnipaxos_config = OmniPaxosConfig::with_toml(&config_file).unwrap();
    let config_string = fs::read_to_string(config_file).unwrap();
    let server_config: OmniPaxosServerConfig = toml::from_str(&config_string).unwrap();
    let max_node_pid = omnipaxos_config.cluster_config.nodes.iter().max().unwrap();
    let initial_leader = server_config.initial_leader.unwrap_or(*max_node_pid);
    println!("{}", serde_json::to_string(&server_config).unwrap());
    let mut server = OmniPaxosServer::new(server_config, omnipaxos_config, initial_leader).await;
    server.run().await;
}
