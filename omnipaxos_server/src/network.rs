use anyhow::Error;
use futures::{SinkExt, Stream, StreamExt};
use log::*;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};

use common::{
    kv::{ClientId, NodeId},
    messages::*,
    util::*,
};

enum ConnectionId {
    ClientConnection(ClientId),
    ServerConnection(NodeId),
}

enum NewConnection {
    NodeConnection(NodeId, NetworkSink),
    ClientConnection(ClientId, NetworkSink),
}

pub struct Network {
    cluster_name: String,
    id: NodeId,
    peers: Vec<NodeId>,
    pub cluster_connections: Vec<Option<NetworkSink>>,
    pub client_connections: HashMap<ClientId, NetworkSink>,
    max_client_id: Arc<Mutex<ClientId>>,
    message_sink: Sender<Incoming>,
    message_source: Receiver<Incoming>,
    is_local: bool,
}

impl Network {
    pub async fn new(
        cluster_name: String,
        id: NodeId,
        nodes: Vec<NodeId>,
        num_clients: usize,
        local_deployment: bool,
    ) -> Result<Self, Error> {
        let (message_sink, message_source) = mpsc::channel(100);
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
            message_sink,
            message_source,
            is_local: local_deployment,
        };
        network.initialize_connections(num_clients).await;
        Ok(network)
    }

    async fn initialize_connections(&mut self, num_clients: usize) {
        let (connection_sink, mut connection_source) = mpsc::channel(10);
        let listen_handle = tokio::spawn(Self::listen_for_connections(
            self.id,
            self.message_sink.clone(),
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
                        NewConnection::NodeConnection(id, conn) => {
                            match self.cluster_id_to_idx(id) {
                                Some(idx) => self.cluster_connections[idx] = Some(conn),
                                None => error!("New connection from unexpected node {id}"),
                            }
                        },
                        NewConnection::ClientConnection(id, conn) => _ = {
                            debug!("{}: Adding connection from client {id}", self.id);
                            self.client_connections.insert(id, conn);
                        },
                    }
                    let all_clients_connected = self.client_connections.len() >= num_clients;
                    let a: Vec<&str> = self.cluster_connections.iter().map(|c| match c {Some(_) => "Some(conn)", None => "None"}).collect();
                    debug!("{}: Connections remaining {:?}", self.id, a);
                    let all_cluster_connected = self.cluster_connections.iter().all(|c| c.is_some());
                    if all_clients_connected && all_cluster_connected {
                        abort_listen_handle.abort();
                        break;
                    }
                },
            }
        }
        debug!("{}: Network initialized", self.id);
    }

    #[inline]
    fn cluster_id_to_idx(&self, id: NodeId) -> Option<usize> {
        self.peers.iter().position(|&p| p == id)
    }

    async fn listen_for_connections(
        id: NodeId,
        message_sink: Sender<Incoming>,
        connection_sink: Sender<NewConnection>,
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
                        message_sink.clone(),
                        connection_sink.clone(),
                        max_client_id_handle.clone(),
                    ));
                }
                Err(e) => error!("Error listening for new connection: {:?}", e),
            }
        }
    }

    fn connect_to_peers(&self, connection_sink: Sender<NewConnection>) {
        let pending_peer_connections: Vec<u64> = self
            .peers
            .iter()
            .cloned()
            .filter(|&p| {
                self.cluster_connections[self.cluster_id_to_idx(p).unwrap()].is_none()
                    && self.id < p
            })
            .collect();
        debug!("{}: Reconnecting to {pending_peer_connections:?}", self.id);
        for pending_node in pending_peer_connections {
            let message_sink = self.message_sink.clone();
            let conn_sink = connection_sink.clone();
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
                            message_sink,
                            conn_sink,
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
        message_sink: Sender<Incoming>,
        connection_sink: Sender<NewConnection>,
        max_client_id_handle: Arc<Mutex<ClientId>>,
    ) {
        connection.set_nodelay(true).unwrap();
        let (mut reader, writer) = wrap_split_stream(connection);

        // Identify connector's ID by handshake
        let first_message = reader.next().await;
        let connection_id = match first_message {
            Some(Ok(NetworkMessage::NodeRegister(node_id))) => {
                info!("Identified connection from node {node_id}");
                let identified_connection = NewConnection::NodeConnection(node_id, writer);
                connection_sink.send(identified_connection).await.unwrap();
                ConnectionId::ServerConnection(node_id)
            }
            Some(Ok(NetworkMessage::ClientRegister)) => {
                let next_client_id = {
                    let mut max_client_id = max_client_id_handle.lock().unwrap();
                    *max_client_id += 1;
                    *max_client_id
                };
                debug!("Identified connection from client {next_client_id}");
                // let handshake_finish =
                //     NetworkMessage::ClientToMsg(ClientToMsg::AssignedID(next_client_id));
                // debug!("Assigning id to client {next_client_id}");
                // if let Err(err) = writer.send(handshake_finish).await {
                //     error!("Error sending ID to client {next_client_id}: {err}");
                //     return;
                // }
                let identified_connection = NewConnection::ClientConnection(next_client_id, writer);
                connection_sink.send(identified_connection).await.unwrap();
                ConnectionId::ClientConnection(next_client_id)
            }
            Some(Ok(msg)) => {
                warn!("Received unknown message during handshake: {:?}", msg);
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

        match connection_id {
            ConnectionId::ClientConnection(id) => {
                while let Some(msg) = reader.next().await {
                    match msg {
                        Ok(NetworkMessage::ClientMessage(m)) => {
                            debug!("Received request from client {id}: {m:?}");
                            let request = Incoming::ClientMessage(id, m);
                            message_sink.send(request).await.unwrap();
                        }
                        Ok(NetworkMessage::KillServer) => {
                            log::error!("Received kill signal");
                            std::process::abort();
                        }
                        Ok(m) => warn!("Received unexpected message: {m:?}"),
                        Err(err) => {
                            error!("Error deserializing message: {:?}", err);
                            break;
                        }
                    }
                }
            }
            ConnectionId::ServerConnection(id) => {
                while let Some(msg) = reader.next().await {
                    match msg {
                        Ok(NetworkMessage::ClusterMessage(m)) => {
                            trace!("Received: {m:?}");
                            let request = Incoming::ClusterMessage(id, m);
                            message_sink.send(request).await.unwrap();
                        }
                        Ok(m) => warn!("Received unexpected message: {m:?}"),
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
        message_sink: Sender<Incoming>,
        connection_sink: Sender<NewConnection>,
        my_id: NodeId,
        to: NodeId,
    ) {
        connection.set_nodelay(true).unwrap();
        let (mut reader, mut writer) = wrap_split_stream(connection);

        // Send handshake
        let handshake = NetworkMessage::NodeRegister(my_id);
        if let Err(err) = writer.send(handshake).await {
            error!("Error sending handshake to {to}: {err}");
            return;
        }
        let new_connection = NewConnection::NodeConnection(to, writer);
        connection_sink.send(new_connection).await.unwrap();

        // Collect incoming messages
        while let Some(msg) = reader.next().await {
            match msg {
                Ok(NetworkMessage::ClusterMessage(m)) => {
                    trace!("Received: {m:?}");
                    let request = Incoming::ClusterMessage(to, m);
                    message_sink.send(request).await.unwrap();
                }
                Ok(m) => warn!("Received unexpected message: {m:?}"),
                Err(err) => {
                    error!("Error deserializing message: {:?}", err);
                    break;
                }
            }
        }
    }

    pub async fn send(&mut self, message: Outgoing) {
        match message {
            Outgoing::ServerMessage(client_id, msg) => self.send_to_client(client_id, msg).await,
            Outgoing::ClusterMessage(server_id, msg) => self.send_to_cluster(server_id, msg).await,
        }
    }

    async fn send_to_cluster(&mut self, to: NodeId, msg: ClusterMessage) {
        match self.cluster_id_to_idx(to) {
            Some(idx) => match &mut self.cluster_connections[idx] {
                Some(ref mut writer) => {
                    if let Err(err) = writer.send(NetworkMessage::ClusterMessage(msg)).await {
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

    async fn send_to_client(&mut self, to: ClientId, msg: ServerMessage) {
        if let Some(writer) = self.client_connections.get_mut(&to) {
            debug!("Responding to client {to}: {msg:?}");
            let net_msg = NetworkMessage::ServerMessage(msg);
            if let Err(err) = writer.send(net_msg).await {
                warn!("Couldn't send message to client {to}: {err}");
                warn!("Removing connection to client {to}");
                self.client_connections.remove(&to);
            }
        } else {
            warn!("Not connected to client {to}");
        }
    }
}

#[derive(Debug, Clone)]
pub enum NetworkError {
    SocketListenerFailure,
    InternalChannelFailure,
}

impl Stream for Network {
    type Item = Incoming;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.message_source.poll_recv(cx)
    }
}
