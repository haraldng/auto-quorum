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
    client_message_sender: Sender<ClientMessage>,
    cluster_message_sender: Sender<ClusterMessage>,
}

impl Network {
    pub async fn new(
        cluster_name: String,
        id: NodeId,
        nodes: Vec<NodeId>,
        num_clients: usize,
        local_deployment: bool,
        client_message_sender: Sender<ClientMessage>,
        cluster_message_sender: Sender<ClusterMessage>,
    ) -> Result<Self, Error> {
        let peers: Vec<u64> = nodes.into_iter().filter(|node| *node != id).collect();
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
        let (connection_sink, mut connection_source) = mpsc::channel(10);
        let listen_handle = tokio::spawn(Self::listen_for_connections(
            self.id,
            self.client_message_sender.clone(),
            self.cluster_message_sender.clone(),
            connection_sink.clone(),
            self.max_client_id.clone(),
        ));
        let abort_listen_handle = listen_handle.abort_handle();
        let reconnect_delay = Duration::from_secs(1);
        let mut reconnect_interval = tokio::time::interval(reconnect_delay);
        loop {
            tokio::select! {
                _ = reconnect_interval.tick() => self.connect_to_peers(connection_sink.clone()),
                Some(new_connection) = connection_source.recv() => {
                    match new_connection {
                        NewEgressConnection::ToNode(id, conn) => {
                            match self.cluster_id_to_idx(id) {
                                Some(idx) => self.cluster_connections[idx] = Some(conn),
                                None => error!("New connection from unexpected node {id}"),
                            }
                        },
                        NewEgressConnection::ToClient(id, conn) => _ = self.client_connections.insert(id, conn),
                    }
                    let all_clients_connected = self.client_connections.len() >= num_clients;
                    let all_cluster_connected = self.cluster_connections.iter().all(|c| c.is_some());
                    if all_clients_connected && all_cluster_connected {
                        abort_listen_handle.abort();
                        break;
                    }
                },
            }
        }
    }

    #[inline]
    fn cluster_id_to_idx(&self, id: NodeId) -> Option<usize> {
        self.peers.iter().position(|&p| p == id)
    }

    async fn listen_for_connections(
        id: NodeId,
        client_message_sender: Sender<ClientMessage>,
        cluster_message_sender: Sender<ClusterMessage>,
        connection_sender: Sender<NewEgressConnection>,
        max_client_id_handle: Arc<Mutex<ClientId>>,
    ) {
        let port = 8000 + id as u16;
        let listening_address = SocketAddr::from(([0, 0, 0, 0], port));
        let listener = TcpListener::bind(listening_address).await.unwrap();

        loop {
            match listener.accept().await {
                Ok((tcp_stream, socket_addr)) => {
                    debug!("New connection from {socket_addr}");
                    tokio::spawn(Self::handle_incoming_connection(
                        tcp_stream,
                        client_message_sender.clone(),
                        cluster_message_sender.clone(),
                        connection_sender.clone(),
                        max_client_id_handle.clone(),
                    ));
                }
                Err(e) => error!("Error listening for new connection: {:?}", e),
            }
        }
    }

    fn connect_to_peers(&self, connection_sender: Sender<NewEgressConnection>) {
        let pending_peer_connections: Vec<u64> = self
            .peers
            .iter()
            .cloned()
            .filter(|&p| {
                self.cluster_connections[self.cluster_id_to_idx(p).unwrap()].is_none()
                    && self.id < p
            })
            .collect();
        if !pending_peer_connections.is_empty() {
            debug!(
                "{}: Trying to connect to nodes {pending_peer_connections:?}",
                self.id
            );
        }
        for pending_node in pending_peer_connections {
            let cluster_message_sender = self.cluster_message_sender.clone();
            let conn_sender = connection_sender.clone();
            let from = self.id;
            let to_address = match get_node_addr(&self.cluster_name, pending_node, self.is_local) {
                Ok(addr) => addr,
                Err(e) => {
                    log::error!("Error resolving DNS name of node {pending_node}: {e}");
                    return;
                }
            };
            tokio::spawn(async move {
                match TcpStream::connect(to_address).await {
                    Ok(connection) => {
                        debug!("New connection to node {pending_node}");
                        Self::handle_connection_to_node(
                            connection,
                            cluster_message_sender,
                            conn_sender,
                            from,
                            pending_node,
                        )
                        .await;
                    }
                    Err(err) => {
                        error!("Establishing connection to node {pending_node} failed: {err}")
                    }
                }
            });
        }
    }

    async fn handle_incoming_connection(
        connection: TcpStream,
        client_message_sender: Sender<ClientMessage>,
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
                    match msg {
                        Ok(m) => {
                            debug!("Received request from client {client_id}: {m:?}");
                            client_message_sender.send(m).await.unwrap();
                        }
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
                    if let Err(err) = writer.send(msg).await {
                        warn!("Couldn't send message to node {to}: {err}");
                        warn!("Removing connection to node {to}");
                        self.cluster_connections[idx] = None;
                    }
                }
                None => warn!("Not connected to node {to}: couldn't send {msg:?}"),
            },
            None => error!("Sending to unexpected node {to}"),
        }
    }

    pub async fn send_to_client(&mut self, to: ClientId, msg: ServerMessage) {
        if let Some(writer) = self.client_connections.get_mut(&to) {
            debug!("Responding to client {to}: {msg:?}");
            if let Err(err) = writer.send(msg).await {
                warn!("Couldn't send message to client {to}: {err}");
                warn!("Removing connection to client {to}");
                self.client_connections.remove(&to);
            }
        } else {
            warn!("Not connected to client {to}");
        }
    }
}
