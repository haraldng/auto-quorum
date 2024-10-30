use common::{kv::NodeId, messages::*, util::*};
use futures::{SinkExt, Stream, StreamExt};
use log::*;
use std::{
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    net::TcpStream,
    sync::mpsc::{Receiver, Sender, UnboundedReceiver, UnboundedSender},
    time::interval,
};

pub struct Network {
    // cluster_name: String,
    // server_id: NodeId,
    // is_local: bool,
    // reader_handle: JoinHandle<()>,
    // writer_handle: JoinHandle<()>,
    incoming_messages: Receiver<ServerMessage>,
    outgoing_messages: UnboundedSender<ClientMessage>,
}

impl Network {
    pub async fn new(cluster_name: String, server_id: NodeId, local_deployment: bool) -> Self {
        // Get connection to server
        let server_address = Self::get_server_address(&cluster_name, server_id, local_deployment);
        let (from_server_conn, to_server_conn) = Self::get_server_connection(server_address).await;
        // Spawn reader and writer actors
        let (incoming_message_tx, incoming_message_rx) = tokio::sync::mpsc::channel(1000);
        let (outgoing_message_tx, outgoing_message_rx) = tokio::sync::mpsc::unbounded_channel();
        let _reader_handle =
            tokio::spawn(Self::reader_actor(from_server_conn, incoming_message_tx));
        let _writer_handle = tokio::spawn(Self::writer_actor(to_server_conn, outgoing_message_rx));
        Self {
            // cluster_name,
            // server_id,
            // is_local: local_deployment,
            // reader_handle,
            // writer_handle,
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
            match TcpStream::connect(server_address).await {
                Ok(stream) => {
                    stream.set_nodelay(true).unwrap();
                    let mut registration_connection = frame_registration_connection(stream);
                    registration_connection
                        .send(RegistrationMessage::ClientRegister)
                        .await
                        .expect("Couldn't send message to server");
                    // let first_msg = registration_connection.next().await.unwrap();
                    // let assigned_id = match first_msg.unwrap() {
                    //     RegistrationMessage::AssignedId(id) => id,
                    //     _ => panic!("Recieved unexpected message during handshake"),
                    // };
                    let underlying_stream = registration_connection.into_inner().into_inner();
                    break frame_clients_connection(underlying_stream);
                }
                Err(e) => error!("Unable to connect to server: {e}"),
            }
        }
    }

    async fn reader_actor(reader: FromServerConnection, incoming_messages: Sender<ServerMessage>) {
        let mut buf_reader = reader.ready_chunks(100);
        while let Some(messages) = buf_reader.next().await {
            for msg in messages {
                match msg {
                    Ok(m) => incoming_messages.send(m).await.unwrap(),
                    Err(err) => error!("Error deserializing message: {:?}", err),
                }
            }
        }
    }

    async fn writer_actor(
        mut writer: ToServerConnection,
        mut outgoing_messages: UnboundedReceiver<ClientMessage>,
    ) {
        let mut buffer = Vec::with_capacity(100);
        while outgoing_messages.recv_many(&mut buffer, 100).await != 0 {
            for msg in buffer.drain(..) {
                if let Err(err) = writer.feed(msg).await {
                    warn!("Couldn't send message to server: {err}");
                }
            }
            writer.flush().await.unwrap();
        }
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
