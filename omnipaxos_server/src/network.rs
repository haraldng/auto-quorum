use anyhow::Error;
use futures::{SinkExt, StreamExt};
use log::*;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio::time::Instant;

use common::{
    kv::{ClientId, NodeId},
    messages::*,
    util::*,
};

enum NewEgressConnection {
    ToNode(NodeId, ToNodeConnection),
    ToClient(ClientId, ToClientConnection),
}
enum NewIngressConnection {
    FromNode(NodeId, FromNodeConnection),
    FromClient(ClientId, FromClientConnection),
}

pub struct Network {
    cluster_name: String,
    id: NodeId,
    peers: Vec<NodeId>,
    is_local: bool,
    cluster_connections: Vec<Option<ToNodeConnection>>,
    client_connections: HashMap<ClientId, ToClientConnection>,
    max_client_id: Arc<Mutex<ClientId>>,
    client_message_sender: Sender<(ClientMessage, Instant)>,
    cluster_message_sender: Sender<ClusterMessage>,
}

impl Network {
    pub async fn new(
        cluster_name: String,
        id: NodeId,
        peers: Vec<NodeId>,
        num_clients: usize,
        local_deployment: bool,
        client_message_sender: Sender<(ClientMessage, Instant)>,
        cluster_message_sender: Sender<ClusterMessage>,
    ) -> Result<Self, Error> {
        let mut cluster_connections = vec![];
        cluster_connections.resize_with(peers.len(), Default::default);
        let mut network = Self {
            cluster_name,
            id,
            peers,
            cluster_connections,
            client_connections: HashMap::new(),
            max_client_id: Arc::new(Mutex::new(0)),
            client_message_sender,
            cluster_message_sender,
            is_local: local_deployment,
        };
        network.initialize_connections(num_clients).await;
        Ok(network)
    }

    async fn initialize_connections(&mut self, num_clients: usize) {
        let (connection_sink, mut connection_source) = mpsc::channel(30);
        let listener_handle = self.spawn_connection_listener(connection_sink.clone());
        self.spawn_peer_connectors(connection_sink.clone());
        while let Some(new_connection) = connection_source.recv().await {
            match new_connection {
                NewEgressConnection::ToNode(id, conn) => match self.cluster_id_to_idx(id) {
                    Some(idx) => self.cluster_connections[idx] = Some(conn),
                    None => error!("New connection from unexpected node {id}"),
                },
                NewEgressConnection::ToClient(id, conn) => {
                    _ = self.client_connections.insert(id, conn)
                }
            }
            let all_clients_connected = self.client_connections.len() >= num_clients;
            let all_cluster_connected = self.cluster_connections.iter().all(|c| c.is_some());
            if all_clients_connected && all_cluster_connected {
                listener_handle.abort();
                break;
            }
        }
    }

    fn spawn_connection_listener(
        &self,
        connection_sender: Sender<NewEgressConnection>,
    ) -> tokio::task::JoinHandle<()> {
        let port = 8000 + self.id as u16;
        let listening_address = SocketAddr::from(([0, 0, 0, 0], port));
        let client_sender = self.client_message_sender.clone();
        let cluster_sender = self.cluster_message_sender.clone();
        let max_client_id_handle = self.max_client_id.clone();
        tokio::spawn(async move {
            let listener = TcpListener::bind(listening_address).await.unwrap();
            loop {
                match listener.accept().await {
                    Ok((tcp_stream, socket_addr)) => {
                        debug!("New connection from {socket_addr}");
                        tokio::spawn(Self::handle_incoming_connection(
                            tcp_stream,
                            client_sender.clone(),
                            cluster_sender.clone(),
                            connection_sender.clone(),
                            max_client_id_handle.clone(),
                        ));
                    }
                    Err(e) => error!("Error listening for new connection: {:?}", e),
                }
            }
        })
    }

    fn spawn_peer_connectors(&self, connection_sender: Sender<NewEgressConnection>) {
        let my_id = self.id;
        let peers_to_contact: Vec<NodeId> =
            self.peers.iter().cloned().filter(|&p| p > my_id).collect();
        for peer in peers_to_contact {
            let cluster_sender = self.cluster_message_sender.clone();
            let connection_sender = connection_sender.clone();
            let to_address = match get_node_addr(&self.cluster_name, peer, self.is_local) {
                Ok(addr) => addr,
                Err(e) => {
                    log::error!("Error resolving DNS name of node {peer}: {e}");
                    return;
                }
            };
            let reconnect_delay = Duration::from_secs(1);
            let mut reconnect_interval = tokio::time::interval(reconnect_delay);
            tokio::spawn(async move {
                let peer_connection = loop {
                    reconnect_interval.tick().await;
                    match TcpStream::connect(to_address).await {
                        Ok(connection) => {
                            debug!("New connection to node {peer}");
                            break connection;
                        }
                        Err(err) => {
                            error!("Establishing connection to node {peer} failed: {err}")
                        }
                    }
                };
                Self::handle_connection_to_node(
                    peer_connection,
                    cluster_sender,
                    connection_sender,
                    my_id,
                    peer,
                )
                .await;
            });
        }
    }

    async fn handle_incoming_connection(
        connection: TcpStream,
        client_message_sender: Sender<(ClientMessage, Instant)>,
        cluster_message_sender: Sender<ClusterMessage>,
        connection_sender: Sender<NewEgressConnection>,
        max_client_id_handle: Arc<Mutex<ClientId>>,
    ) {
        connection.set_nodelay(true).unwrap();
        let mut registration_connection = frame_registration_connection(connection);

        // Identify connector's ID by handshake
        let registration_message = registration_connection.next().await;
        let identified_connection = match registration_message {
            Some(Ok(RegistrationMessage::NodeRegister(node_id))) => {
                info!("Identified connection from node {node_id}");
                let underlying_stream = registration_connection.into_inner().into_inner();
                let (from_node, to_node) = frame_cluster_connection(underlying_stream);
                connection_sender
                    .send(NewEgressConnection::ToNode(node_id, to_node))
                    .await
                    .unwrap();
                NewIngressConnection::FromNode(node_id, from_node)
            }
            Some(Ok(RegistrationMessage::ClientRegister)) => {
                let next_client_id = {
                    let mut max_client_id = max_client_id_handle.lock().unwrap();
                    *max_client_id += 1;
                    *max_client_id
                };
                debug!("Identified connection from client {next_client_id}");
                registration_connection
                    .send(RegistrationMessage::AssignedId(next_client_id))
                    .await
                    .unwrap();
                let underlying_stream = registration_connection.into_inner().into_inner();
                let (from_client, to_client) = frame_servers_connection(underlying_stream);
                connection_sender
                    .send(NewEgressConnection::ToClient(next_client_id, to_client))
                    .await
                    .unwrap();
                NewIngressConnection::FromClient(next_client_id, from_client)
            }
            Some(Ok(RegistrationMessage::AssignedId(_))) => {
                error!("Handshake failed");
                return;
            }
            Some(Err(err)) => {
                error!("Error deserializing handshake: {:?}", err);
                return;
            }
            None => {
                debug!("Connection to unidentified source dropped");
                return;
            }
        };

        // Receive messages
        match identified_connection {
            NewIngressConnection::FromClient(client_id, mut reader) => {
                while let Some(msg) = reader.next().await {
                    debug!("Network: Request from client {client_id}: {msg:?}");
                    match msg {
                        Ok(m) => client_message_sender
                            .send((m, Instant::now()))
                            .await
                            .unwrap(),
                        Err(err) => {
                            error!("Error deserializing message: {:?}", err);
                            break;
                        }
                    }
                }
            }
            NewIngressConnection::FromNode(_node_id, mut reader) => {
                while let Some(msg) = reader.next().await {
                    match msg {
                        Ok(m) => {
                            trace!("Received: {m:?}");
                            cluster_message_sender.send(m).await.unwrap();
                        }
                        Err(err) => {
                            error!("Error deserializing message: {:?}", err);
                            break;
                        }
                    }
                }
            }
        }
    }

    async fn handle_connection_to_node(
        connection: TcpStream,
        cluster_message_sender: Sender<ClusterMessage>,
        connection_sender: Sender<NewEgressConnection>,
        my_id: NodeId,
        to: NodeId,
    ) {
        connection.set_nodelay(true).unwrap();
        let mut registration_connection = frame_registration_connection(connection);

        // Send handshake
        let handshake = RegistrationMessage::NodeRegister(my_id);
        if let Err(err) = registration_connection.send(handshake).await {
            error!("Error sending handshake to {to}: {err}");
            return;
        }
        let underlying_stream = registration_connection.into_inner().into_inner();
        let (mut from_node, to_node) = frame_cluster_connection(underlying_stream);
        let new_connection = NewEgressConnection::ToNode(to, to_node);
        connection_sender.send(new_connection).await.unwrap();

        // Collect incoming messages
        while let Some(msg) = from_node.next().await {
            match msg {
                Ok(m) => {
                    trace!("Received: {m:?}");
                    cluster_message_sender.send(m).await.unwrap();
                }
                Err(err) => {
                    error!("Error deserializing message: {:?}", err);
                    break;
                }
            }
        }
    }

    pub async fn send_to_cluster(&mut self, to: NodeId, msg: ClusterMessage) {
        match self.cluster_id_to_idx(to) {
            Some(idx) => match &mut self.cluster_connections[idx] {
                Some(ref mut writer) => {
                    let now = Instant::now();
                    if let Err(err) = writer.send(msg).await {
                        warn!("Couldn't send message to node {to}: {err}");
                        warn!("Removing connection to node {to}");
                        self.cluster_connections[idx] = None;
                    }
                    // let send_time = now.elapsed();
                    // if send_time > Duration::from_micros(100) {
                    //     eprintln!("Send cluster message {send_time:?}");
                    // }
                }
                None => warn!("Not connected to node {to}: couldn't send {msg:?}"),
            },
            None => error!("Sending to unexpected node {to}"),
        }
    }

    pub async fn send_to_client(&mut self, to: ClientId, msg: ServerMessage) {
        if let Some(writer) = self.client_connections.get_mut(&to) {
            debug!("Responding to client {to}: {msg:?}");
            let now = Instant::now();
            if let Err(err) = writer.send(msg).await {
                warn!("Couldn't send message to client {to}: {err}");
                warn!("Removing connection to client {to}");
                self.client_connections.remove(&to);
            }
            // let send_time = now.elapsed();
            // if send_time > Duration::from_micros(100) {
            //     eprintln!("Send client message {send_time:?}");
            // }
        } else {
            warn!("Not connected to client {to}");
        }
    }

    #[inline]
    fn cluster_id_to_idx(&self, id: NodeId) -> Option<usize> {
        self.peers.iter().position(|&p| p == id)
    }
}
