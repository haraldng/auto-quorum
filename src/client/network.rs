use futures::{SinkExt, Stream, StreamExt};
use log::*;
use metronome::common::{kv::NodeId, messages::*, utils::*};
use std::{
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    net::TcpStream,
    sync::mpsc::{Receiver, Sender, UnboundedReceiver, UnboundedSender},
    task::JoinHandle,
    time::interval,
};

pub struct Network {
    reader_handle: JoinHandle<()>,
    writer_handle: JoinHandle<()>,
    incoming_messages: Receiver<ServerMessage>,
    outgoing_messages: UnboundedSender<ClientMessage>,
}
const SOCKET_BUFFER_SIZE: usize = 10_000;

impl Network {
    pub async fn new(cluster_name: String, server_id: NodeId, local_deployment: bool) -> Self {
        // Get connection to server
        let server_address = Self::get_server_address(&cluster_name, server_id, local_deployment);
        let (from_server_conn, to_server_conn) = Self::get_server_connection(server_address).await;
        // Spawn reader and writer actors
        let (incoming_message_tx, incoming_message_rx) = tokio::sync::mpsc::channel(1000);
        let (outgoing_message_tx, outgoing_message_rx) = tokio::sync::mpsc::unbounded_channel();
        let reader_handle = tokio::spawn(Self::reader_actor(from_server_conn, incoming_message_tx));
        let writer_handle = tokio::spawn(Self::writer_actor(to_server_conn, outgoing_message_rx));
        Self {
            reader_handle,
            writer_handle,
            incoming_messages: incoming_message_rx,
            outgoing_messages: outgoing_message_tx,
        }
    }

    fn get_server_address(
        cluster_name: &String,
        server_id: NodeId,
        local_deployment: bool,
    ) -> SocketAddr {
        match get_node_addr(cluster_name, server_id, local_deployment) {
            Ok(address) => address,
            Err(e) => panic!("Couldn't resolve server's DNS name: {e}"),
        }
    }

    async fn get_server_connection(
        server_address: SocketAddr,
    ) -> (FromServerConnection, ToServerConnection) {
        let mut retry_connection = interval(Duration::from_secs(1));
        loop {
            retry_connection.tick().await;
            debug!("Connecting to server at {server_address:?}...");
            match TcpStream::connect(server_address).await {
                Ok(stream) => {
                    debug!("Connection to server established");
                    debug!("Sending registration message");
                    stream.set_nodelay(true).unwrap();
                    let mut registration_connection = frame_registration_connection(stream);
                    registration_connection
                        .send(RegistrationMessage::ClientRegister)
                        .await
                        .expect("Couldn't send message to server");
                    let underlying_stream = registration_connection.into_inner().into_inner();
                    break frame_clients_connection(underlying_stream);
                }
                Err(e) => error!("Unable to connect to server: {e}"),
            }
        }
    }

    async fn reader_actor(reader: FromServerConnection, incoming_messages: Sender<ServerMessage>) {
        let mut buf_reader = reader.ready_chunks(SOCKET_BUFFER_SIZE);
        while let Some(messages) = buf_reader.next().await {
            for msg in messages {
                match msg {
                    Ok(m) => incoming_messages.send(m).await.unwrap(),
                    Err(err) => error!("Error deserializing message: {:?}", err),
                }
            }
        }
        error!("Connection from server lost");
    }

    async fn writer_actor(
        mut writer: ToServerConnection,
        mut outgoing_messages: UnboundedReceiver<ClientMessage>,
    ) {
        let mut buffer = Vec::with_capacity(SOCKET_BUFFER_SIZE);
        while outgoing_messages
            .recv_many(&mut buffer, SOCKET_BUFFER_SIZE)
            .await
            != 0
        {
            for msg in buffer.drain(..) {
                if let Err(err) = writer.feed(msg).await {
                    warn!("Couldn't send message to server: {err}");
                }
            }
            writer.flush().await.unwrap();
        }
        error!("Connection to server lost");
    }

    pub fn send(&mut self, message: ClientMessage) {
        if let Err(e) = self.outgoing_messages.send(message) {
            error!("Couldn't send message to server: {e}");
        }
    }
}

impl Stream for Network {
    type Item = ServerMessage;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.incoming_messages.poll_recv(cx)
    }
}

impl Drop for Network {
    // Shutdown reader/writer tasks on destruction
    fn drop(&mut self) {
        self.reader_handle.abort();
        self.writer_handle.abort();
    }
}
