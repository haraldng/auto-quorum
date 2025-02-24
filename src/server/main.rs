use crate::{configs::MetronomeConfig, server::MetronomeServer};

mod configs;
mod database;
mod network;
mod server;

#[tokio::main]
pub async fn main() {
    env_logger::init();
    let server_config = match MetronomeConfig::new() {
        Ok(parsed_config) => parsed_config,
        Err(e) => panic!("{e}"),
    };
    log::info!("Starting server");
    let mut server = MetronomeServer::new(server_config).await;
    server.run().await;
}
