use chrono::Utc;
use futures::{SinkExt, StreamExt};
use log::*;
use metronome::common::{
    kv::{ClientId, NodeId},
    messages::*,
    utils::*,
};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{self, UnboundedReceiver};
use tokio::sync::mpsc::{Sender, UnboundedSender};

pub struct Network {
    cluster_name: String,
    id: NodeId,
    peers: Vec<NodeId>,
    is_local: bool,
    peer_connections: Vec<Option<PeerConnection>>,
    client_connections: HashMap<ClientId, ClientConnection>,
    max_client_id: Arc<Mutex<ClientId>>,
    client_message_sender: Sender<(ClientId, ClientMessage, i64)>,
    cluster_message_sender: Sender<(ClusterMessage, i64)>,
}

impl Network {
    pub async fn new(
        cluster_name: String,
        id: NodeId,
        peers: Vec<NodeId>,
        num_clients: usize,
        local_deployment: bool,
        client_message_sender: Sender<(ClientId, ClientMessage, i64)>,
        cluster_message_sender: Sender<(ClusterMessage, i64)>,
    ) -> Self {
        let mut cluster_connections = vec![];
        cluster_connections.resize_with(peers.len(), Default::default);
        let mut network = Self {
            cluster_name,
            id,
            peers,
            peer_connections: cluster_connections,
            client_connections: HashMap::new(),
            max_client_id: Arc::new(Mutex::new(0)),
            client_message_sender,
            cluster_message_sender,
            is_local: local_deployment,
        };
        network.initialize_connections(num_clients).await;
        network
    }

    async fn initialize_connections(&mut self, num_clients: usize) {
        let (connection_sink, mut connection_source) = mpsc::channel(30);
        let listener_handle = self.spawn_connection_listener(connection_sink.clone());
        self.spawn_peer_connectors(connection_sink.clone());
        while let Some(new_connection) = connection_source.recv().await {
            match new_connection {
                NewConnection::ToPeer(connection) => {
                    let peer_idx = self.cluster_id_to_idx(connection.peer_id).unwrap();
                    self.peer_connections[peer_idx] = Some(connection);
                }
                NewConnection::ToClient(connection) => {
                    let _ = self
                        .client_connections
                        .insert(connection.client_id, connection);
                }
            }
            let all_clients_connected = self.client_connections.len() >= num_clients;
            let all_cluster_connected = self.peer_connections.iter().all(|c| c.is_some());
            if all_clients_connected && all_cluster_connected {
                listener_handle.abort();
                break;
            }
        }
    }

    fn spawn_connection_listener(
        &self,
        connection_sender: Sender<NewConnection>,
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
                        tcp_stream.set_nodelay(true).unwrap();
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

    async fn handle_incoming_connection(
        connection: TcpStream,
        client_message_sender: Sender<(ClientId, ClientMessage, i64)>,
        cluster_message_sender: Sender<(ClusterMessage, i64)>,
        connection_sender: Sender<NewConnection>,
        max_client_id_handle: Arc<Mutex<ClientId>>,
    ) {
        // Identify connector's ID and type by handshake
        let mut registration_connection = frame_registration_connection(connection);
        let registration_message = registration_connection.next().await;
        let new_connection = match registration_message {
            Some(Ok(RegistrationMessage::NodeRegister(node_id))) => {
                info!("Identified connection from node {node_id}");
                let underlying_stream = registration_connection.into_inner().into_inner();
                NewConnection::ToPeer(PeerConnection::new(
                    node_id,
                    underlying_stream,
                    cluster_message_sender,
                ))
            }
            Some(Ok(RegistrationMessage::ClientRegister)) => {
                let next_client_id = {
                    let mut max_client_id = max_client_id_handle.lock().unwrap();
                    *max_client_id += 1;
                    *max_client_id
                };
                info!("Identified connection from client {next_client_id}");
                let underlying_stream = registration_connection.into_inner().into_inner();
                NewConnection::ToClient(ClientConnection::new(
                    next_client_id,
                    underlying_stream,
                    client_message_sender,
                ))
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
        connection_sender.send(new_connection).await.unwrap();
    }

    fn spawn_peer_connectors(&self, connection_sender: Sender<NewConnection>) {
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
                // Establish connection
                let peer_connection = loop {
                    reconnect_interval.tick().await;
                    match TcpStream::connect(to_address).await {
                        Ok(connection) => {
                            info!("New connection to node {peer}");
                            connection.set_nodelay(true).unwrap();
                            break connection;
                        }
                        Err(err) => {
                            error!("Establishing connection to node {peer} failed: {err}")
                        }
                    }
                };
                // Send handshake
                let mut registration_connection = frame_registration_connection(peer_connection);
                let handshake = RegistrationMessage::NodeRegister(my_id);
                if let Err(err) = registration_connection.send(handshake).await {
                    error!("Error sending handshake to {peer}: {err}");
                    return;
                }
                let underlying_stream = registration_connection.into_inner().into_inner();
                // Create connection actor
                let peer_actor = PeerConnection::new(peer, underlying_stream, cluster_sender);
                let new_connection = NewConnection::ToPeer(peer_actor);
                connection_sender.send(new_connection).await.unwrap();
            });
        }
    }

    pub fn send_to_cluster(&mut self, to: NodeId, msg: ClusterMessage) {
        match self.cluster_id_to_idx(to) {
            Some(idx) => match &mut self.peer_connections[idx] {
                Some(ref mut connection) => {
                    // TODO: remove connection if it closes
                    connection.send(msg);
                }
                None => warn!("Not connected to node {to}: couldn't send {msg:?}"),
            },
            None => error!("Sending to unexpected node {to}"),
        }
    }

    pub fn send_to_client(&mut self, to: ClientId, msg: ServerMessage) {
        if let Some(connection) = self.client_connections.get_mut(&to) {
            // debug!("Responding to client {to}: {msg:?}");
            // TODO: remove connection if it closes
            connection.send(msg);
        } else {
            warn!("Not connected to client {to}");
        }
    }

    #[inline]
    fn cluster_id_to_idx(&self, id: NodeId) -> Option<usize> {
        self.peers.iter().position(|&p| p == id)
    }
}

enum NewConnection {
    ToPeer(PeerConnection),
    ToClient(ClientConnection),
}

const SOCKET_BUFFER_SIZE: usize = 1000;
struct PeerConnection {
    peer_id: NodeId,
    outgoing_messages: UnboundedSender<ClusterMessage>,
}

impl PeerConnection {
    pub fn new(
        peer_id: NodeId,
        connection: TcpStream,
        incoming_messages: Sender<(ClusterMessage, i64)>,
    ) -> Self {
        let (reader, mut writer) = frame_cluster_connection(connection);
        // Reader Actor
        let _reader_task = tokio::spawn(async move {
            let mut buf_reader = reader.ready_chunks(SOCKET_BUFFER_SIZE);
            while let Some(messages) = buf_reader.next().await {
                let receive_time = Utc::now().timestamp_micros();
                for msg in messages {
                    match msg {
                        Ok(m) => {
                            trace!("Received: {m:?}");
                            if let Err(_) = incoming_messages.send((m, receive_time)).await {
                                break;
                            };
                        }
                        Err(err) => {
                            error!("Error deserializing message: {:?}", err);
                        }
                    }
                }
            }
        });
        // Writer Actor
        let (message_tx, mut message_rx) = mpsc::unbounded_channel();
        let _writer_task = tokio::spawn(async move {
            let mut buffer = Vec::with_capacity(SOCKET_BUFFER_SIZE);
            while message_rx.recv_many(&mut buffer, SOCKET_BUFFER_SIZE).await != 0 {
                for msg in buffer.drain(..) {
                    if let Err(err) = writer.feed(msg).await {
                        warn!("Couldn't feed message to node {peer_id}: {err}");
                        warn!("Cant Remove connection to node {peer_id}");
                    }
                }
                if let Err(err) = writer.flush().await {
                    warn!("Couldn't flush message to node {peer_id}: {err}");
                    warn!("Cant Remove connection to node {peer_id}");
                };
            }
        });
        PeerConnection {
            peer_id,
            outgoing_messages: message_tx,
        }
    }

    pub fn send(&mut self, msg: ClusterMessage) {
        self.outgoing_messages
            .send(msg)
            .expect("Tried to send on a closed channel");
    }
}

struct ClientConnection {
    client_id: ClientId,
    outgoing_messages: UnboundedSender<ServerMessage>,
}

impl ClientConnection {
    pub fn new(
        client_id: ClientId,
        connection: TcpStream,
        incoming_messages: Sender<(ClientId, ClientMessage, i64)>,
    ) -> Self {
        let (reader, mut writer) = frame_servers_connection(connection);
        // Reader Actor
        let _reader_task = tokio::spawn(async move {
            let mut buf_reader = reader.ready_chunks(SOCKET_BUFFER_SIZE);
            while let Some(messages) = buf_reader.next().await {
                let receive_time = Utc::now().timestamp_micros();
                for msg in messages {
                    // debug!("Network: Request from client {client_id}: {msg:?}");
                    match msg {
                        Ok(m) => {
                            incoming_messages
                                .send((client_id, m, receive_time))
                                .await
                                .unwrap();
                        }
                        Err(err) => {
                            error!("Error deserializing message: {:?}", err);
                        }
                    }
                }
            }
        });
        // Writer Actor
        let (message_tx, mut message_rx): (
            UnboundedSender<ServerMessage>,
            UnboundedReceiver<ServerMessage>,
        ) = mpsc::unbounded_channel();
        let _writer_task = tokio::spawn(async move {
            let mut buffer = Vec::with_capacity(SOCKET_BUFFER_SIZE);
            while message_rx.recv_many(&mut buffer, SOCKET_BUFFER_SIZE).await != 0 {
                for msg in buffer.drain(..) {
                    if let Err(err) = writer.feed(msg).await {
                        warn!("Couldn't send message to client {client_id}: {err}");
                        warn!("Cant Remove connection to client {client_id}");
                    }
                }
                writer.flush().await.unwrap();
            }
        });
        ClientConnection {
            client_id,
            outgoing_messages: message_tx,
        }
    }

    pub fn send(&mut self, msg: ServerMessage) {
        self.outgoing_messages
            .send(msg)
            .expect("Tried to send on a closed channel");
    }
}
