use anyhow::Error;
use futures::{SinkExt, Stream, StreamExt};
use log::*;
use omnipaxos::messages::ballot_leader_election::BLEMsg;
use omnipaxos::messages::Message as OmniPaxosMessage;
use omnipaxos::util::NodeId;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};

use common::{kv::ClientId, messages::*, util::*};

enum ConnectionId {
    ClientConnection(ClientId),
    ServerConnection(NodeId),
}

enum NewConnection {
    NodeConnection(NodeId, NetworkSink),
    ClientConnection(ClientId, NetworkSink),
}

pub struct Network {
    id: NodeId,
    listener: TcpListener,
    cluster_connections: HashMap<NodeId, NetworkSink>,
    client_connections: HashMap<ClientId, NetworkSink>,
    max_client_id: Arc<Mutex<ClientId>>,
    connection_sink: Sender<NewConnection>,
    connection_source: Receiver<NewConnection>,
    message_sink: Sender<Incoming>,
    message_source: Receiver<Incoming>,
    is_local: bool,
}

impl Network {
    pub async fn new(
        id: NodeId,
        peers: Vec<NodeId>,
        local_deployment: bool,
    ) -> Result<Self, Error> {
        let (connection_sink, connection_source) = mpsc::channel(100);
        let (message_sink, message_source) = mpsc::channel(100);
        let port = 8000 + id as u16;
        let listening_address = SocketAddr::from(([127, 0, 0, 1], port));
        let listener = TcpListener::bind(listening_address).await?;
        let mut network = Self {
            id,
            listener,
            cluster_connections: HashMap::new(),
            client_connections: HashMap::new(),
            max_client_id: Arc::new(Mutex::new(0)),
            connection_sink,
            connection_source,
            message_sink,
            message_source,
            is_local: local_deployment,
        };
        // Create connections to other servers
        for peer in peers.into_iter().filter(|p| *p < id) {
            network.connect_to_node(peer);
        }
        Ok(network)
    }

    fn connect_to_node(&mut self, to: NodeId) {
        debug!("Trying to connect to node {to}");
        let message_sink = self.message_sink.clone();
        let connection_sink = self.connection_sink.clone();
        let from = self.id;
        let to_address = match get_node_addr(to, self.is_local) {
            Ok(addr) => addr,
            Err(e) => {
                log::error!("Error resolving DNS name of node {to}: {e}");
                return;
            }
        };
        tokio::spawn(async move {
            match TcpStream::connect(to_address).await {
                Ok(connection) => {
                    debug!("New connection to node {to}");
                    Self::handle_connection_to_node(
                        connection,
                        message_sink,
                        connection_sink,
                        from,
                        to,
                    )
                    .await;
                }
                Err(err) => error!("Establishing connection to node {to} failed: {err}"),
            }
        });
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
        debug!("Sending handshake to {to}");
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

    fn handle_identified_connection(&mut self, connection: NewConnection) {
        match connection {
            NewConnection::NodeConnection(node_id, conn) => {
                self.cluster_connections.insert(node_id, conn)
            }
            NewConnection::ClientConnection(client_id, conn) => {
                self.client_connections.insert(client_id, conn)
            }
        };
    }

    pub async fn send(&mut self, message: Outgoing) {
        match message {
            Outgoing::ServerMessage(client_id, msg) => self.send_to_client(client_id, msg).await,
            Outgoing::ClusterMessage(server_id, msg) => self.send_to_cluster(server_id, msg).await,
        }
    }

    async fn send_to_cluster(&mut self, to: NodeId, msg: ClusterMessage) {
        if let Some(writer) = self.cluster_connections.get_mut(&to) {
            trace!("Sending to node {to}: {msg:?}");
            if let Err(err) = writer.send(NetworkMessage::ClusterMessage(msg)).await {
                warn!("Couldn't send message to node {to}: {err}");
                warn!("Removing connection to node {to}");
                self.cluster_connections.remove(&to);
            }
        } else {
            warn!("Not connected to node {to}");
            // If HeartbeatRequest msg is what failed, try to reconnect to node.
            if let ClusterMessage::OmniPaxosMessage(OmniPaxosMessage::BLE(m)) = msg {
                if let BLEMsg::HeartbeatRequest(_) = m.msg {
                    if m.to == to {
                        self.connect_to_node(to);
                    }
                }
            }
        }
    }

    async fn send_to_client(&mut self, to: ClientId, msg: ServerMessage) {
        if let Some(writer) = self.client_connections.get_mut(&to) {
            debug!("Responding to client {to}: {msg:?}");
            let net_msg = NetworkMessage::ServerMessage(msg);
            if let Err(err) = writer.send(net_msg).await {
                warn!("Couldn't send message to client {to}: {err}");
                warn!("Removing connection to client {to}");
                self.cluster_connections.remove(&to);
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
    type Item = Result<Incoming, NetworkError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Poll new incoming connection
        if let Poll::Ready(val) = Pin::new(&mut self.as_mut().listener).poll_accept(cx) {
            match val {
                Ok((tcp_stream, socket_addr)) => {
                    debug!("New connection from {socket_addr}");
                    tokio::spawn(Self::handle_incoming_connection(
                        tcp_stream,
                        self.message_sink.clone(),
                        self.connection_sink.clone(),
                        self.max_client_id.clone(),
                    ));
                }
                Err(err) => {
                    error!("Error checking for new requests: {:?}", err);
                    return Poll::Ready(Some(Err(NetworkError::SocketListenerFailure)));
                }
            }
        }
        // Poll new identified connection
        if let Poll::Ready(val) = self.connection_source.poll_recv(cx) {
            match val {
                Some(new_conn) => self.handle_identified_connection(new_conn),
                None => return Poll::Ready(Some(Err(NetworkError::InternalChannelFailure))),
            }
        }
        // Poll new incoming message
        if let Poll::Ready(val) = self.message_source.poll_recv(cx) {
            match val {
                Some(msg) => return Poll::Ready(Some(Ok(msg))),
                None => return Poll::Ready(Some(Err(NetworkError::InternalChannelFailure))),
            }
        }
        // Nothing to yield yet
        // Note: don't need to call waker here because previous poll_recv and poll_accept will
        // handle scheduling the wake up.
        return Poll::Pending;
    }
}
