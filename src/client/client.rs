use crate::{
    configs::{ClientConfig, EndCondition},
    data_collection::ClientData,
    network::Network,
};
use futures::StreamExt;
use log::*;
use metronome::common::{kv::*, messages::*};
use std::time::Duration;

pub struct ClosedLoopClient {
    end_condition: EndCondition,
    num_parallel_requests: usize,
    network: Network,
    client_data: ClientData,
    config: ClientConfig,
    leaders_config: Option<MetronomeConfigInfo>,
}

impl ClosedLoopClient {
    pub async fn new(config: ClientConfig) -> Self {
        let local_deployment = config.local_deployment.unwrap_or(false);
        let network = Network::new(
            config.cluster_name.clone(),
            config.server_id,
            local_deployment,
        )
        .await;
        let num_parallel_requests = config.request_mode_config.to_closed_loop_params().unwrap();
        ClosedLoopClient {
            end_condition: config.end_condition.into(),
            num_parallel_requests,
            network,
            client_data: ClientData::new(&config),
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

        // Send initial requests
        info!("Starting requests");
        self.client_data.experiment_start();
        for request_id in 0..self.num_parallel_requests {
            self.send_request(request_id);
        }

        // Send requests and collect responses
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

    // Send new requests on reponse until response limit is reached
    async fn run_until_response_limit(&mut self, response_limit: usize) {
        let mut response_count = 0;
        loop {
            match self.network.next().await {
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
                        let next_request_id = response_id + self.num_parallel_requests;
                        self.send_request(next_request_id);
                    }
                },
                None => panic!("Connection to server lost before end condition"),
            }
        }
    }

    // Send new requests on reponse until duration_limit time has passed
    async fn run_until_duration_limit(&mut self, duration_limit: Duration) {
        let mut end_duration = tokio::time::interval(duration_limit);
        end_duration.tick().await; // First tick resolves immediately
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
                                let next_request_id = response_id + self.num_parallel_requests;
                                self.send_request(next_request_id);
                            }
                        },
                        None => panic!("Connection to server lost before end condition"),
                    }
                },
                _ = end_duration.tick() => break,
            }
        }
    }

    fn send_request(&mut self, request_id: CommandId) {
        let key = request_id % self.num_parallel_requests;
        let cmd = KVCommand::Put(key, request_id);
        let request = ClientMessage::Append(request_id, cmd);
        debug!("Sending request {request:?}");
        self.network.send(request);
        self.client_data.new_request(request_id);
    }
}
