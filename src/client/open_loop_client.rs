use crate::{
    client::ClientData,
    configs::{ClientConfig, EndCondition, EndConditionConfig},
    network::Network,
};
use futures::StreamExt;
use log::*;
use metronome::common::{kv::*, messages::*};
use std::{time::Duration, usize};

pub struct OpenLoopClient {
    end_condition: EndCondition,
    request_delay: Duration,
    requests_per_interval: usize,
    next_request_id: usize,
    network: Network,
    client_data: ClientData,
    config: ClientConfig,
    leaders_config: Option<MetronomeConfigInfo>,
}

impl OpenLoopClient {
    pub async fn new(config: ClientConfig) -> Self {
        let local_deployment = config.local_deployment.unwrap_or(false);
        let network = Network::new(
            config.cluster_name.clone(),
            config.server_id,
            local_deployment,
        )
        .await;
        let num_estimated_responses = match config.end_condition {
            EndConditionConfig::ResponsesCollected(n) => n,
            EndConditionConfig::SecondsPassed(_secs) => 10_000,
        };
        let (request_delay, requests_per_interval) =
            config.request_mode_config.to_open_loop_params().unwrap();
        OpenLoopClient {
            end_condition: config.end_condition.into(),
            request_delay,
            requests_per_interval,
            next_request_id: 0,
            network,
            client_data: ClientData::new(num_estimated_responses),
            config,
            leaders_config: None,
        }
    }

    pub async fn run(&mut self) {
        // Wait until server is ready
        info!("Waiting for ready signal from server");
        let first_msg = self.network.next().await;
        match first_msg {
            Some(ServerMessage::Ready(server_config)) => {
                self.leaders_config = Some(server_config);
            }
            Some(m) => panic!("Recieved unexpected message during handshake: {m:?}"),
            None => panic!("Lost connection to server during handshake"),
        }

        // Send requests and collect responses
        info!("Starting requests");
        self.client_data.experiment_start();
        match self.end_condition {
            EndCondition::ResponsesCollected(n) => self.run_until_response_limit(n).await,
            EndCondition::TimePassed(t) => self.run_until_duration_limit(t).await,
        }
        self.client_data.experiment_end();
        info!(
            "Client finished: collected {} responses",
            self.client_data.total_responses()
        );

        // Shutdown cluster and collect results
        self.network.send(ClientMessage::Done);
        self.client_data
            .save_summary(self.config.clone(), self.leaders_config.unwrap())
            .expect("Failed to write summary file");
        self.client_data
            .to_csv(self.config.output_filename.clone())
            .expect("Failed to write output file");
    }

    // Send new requests every request interval until response limit is reached
    async fn run_until_response_limit(&mut self, response_limit: usize) {
        let mut response_count = 0;
        let mut request_interval = tokio::time::interval(self.request_delay);
        loop {
            tokio::select! {
                biased;
                network_recv = self.network.next() => {
                    match network_recv {
                        Some(message) => match message {
                            ServerMessage::Ready(_) => error!("Unexpected ready message"),
                            msg => {
                                debug!("Received {msg:?}");
                                let response_id = msg.command_id();
                                self.client_data.new_response(response_id);
                                response_count += 1;
                                if response_count >= response_limit {
                                    break;
                                }
                            }
                        },
                        None => panic!("Connection to server lost before end condition"),
                    }
                },
                _ = request_interval.tick() => self.send_request(self.requests_per_interval),
            }
        }
    }

    // Send new requests every request interval until duration_limit time has passed
    async fn run_until_duration_limit(&mut self, duration_limit: Duration) {
        let mut end_duration = tokio::time::interval(duration_limit);
        end_duration.tick().await; // First tick resolves immediately
        let mut request_interval = tokio::time::interval(self.request_delay);
        loop {
            tokio::select! {
                biased;
                incoming_message = self.network.next() => {
                    match incoming_message {
                        Some(message) => match message {
                            ServerMessage::Ready(_) => error!("Unexpected ready message"),
                            msg => {
                                debug!("Received {msg:?}");
                                let response_id = msg.command_id();
                                self.client_data.new_response(response_id);
                            }
                        },
                        None => panic!("Connection to server lost before end condition"),
                    }
                },
                _ = request_interval.tick() => self.send_request(self.requests_per_interval),
                _ = end_duration.tick() => break,
            }
        }
    }

    fn send_request(&mut self, batch_size: usize) {
        let mut requests = Vec::with_capacity(batch_size);
        let start_id = self.next_request_id;
        for _ in 0..batch_size {
            let key = self.next_request_id % 10000;
            let cmd = KVCommand::Put(key, self.next_request_id);
            requests.push((self.next_request_id, cmd));
            self.next_request_id += 1;
        }
        let batch_request = ClientMessage::BatchAppend(requests);
        debug!("Sending request {batch_request:?}");
        self.client_data.new_batch_request(start_id, batch_size);
        self.network.send(batch_request);
    }
}
