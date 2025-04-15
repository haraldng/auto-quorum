use futures::{SinkExt, Stream, StreamExt};
use log::*;
use metronome::common::{messages::*, utils::*};
use std::{
    net::{SocketAddr, ToSocketAddrs},
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    net::TcpStream,
    sync::{
        mpsc::{Receiver, Sender, UnboundedReceiver, UnboundedSender},
        oneshot,
    },
    task::JoinHandle,
    time::interval,
};

use crate::configs::ClientConfig;

pub struct Network {
    // reader_handle: JoinHandle<()>,
    writer_handle: Option<JoinHandle<()>>,
    writer_stop_signal: Option<oneshot::Sender<()>>,
    incoming_messages: Receiver<ServerMessage>,
    outgoing_messages: UnboundedSender<ClientMessage>,
}
const SOCKET_BUFFER_SIZE: usize = 20_000;

impl Network {
    pub async fn new(config: ClientConfig) -> Self {
        // Get connection to server
        let server_address = config
            .server_address
            .to_socket_addrs()
            .expect("Unable to resolve server IP")
            .next()
            .unwrap();
        let (from_server_conn, to_server_conn) = Self::get_server_connection(server_address).await;
        // Spawn reader and writer actors
        let (incoming_message_tx, incoming_message_rx) = tokio::sync::mpsc::channel(1000);
        let (outgoing_message_tx, outgoing_message_rx) = tokio::sync::mpsc::unbounded_channel();
        let (stop_signal_tx, stop_signal_rx) = tokio::sync::oneshot::channel();
        let _reader_handle =
            tokio::spawn(Self::reader_actor(from_server_conn, incoming_message_tx));
        let writer_handle = tokio::spawn(Self::writer_actor(
            to_server_conn,
            outgoing_message_rx,
            stop_signal_rx,
        ));
        Self {
            // reader_handle,
            writer_handle: Some(writer_handle),
            writer_stop_signal: Some(stop_signal_tx),
            incoming_messages: incoming_message_rx,
            outgoing_messages: outgoing_message_tx,
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
        mut stop_signal: oneshot::Receiver<()>, // Add oneshot receiver
    ) {
        let mut buffer = Vec::with_capacity(SOCKET_BUFFER_SIZE);

        loop {
            tokio::select! {
                // Normal behavior: receive messages from outgoing_messages
                result = outgoing_messages.recv_many(&mut buffer, SOCKET_BUFFER_SIZE) => {
                    if result == 0 {
                        break;
                    }
                    for msg in buffer.drain(..) {
                        if let Err(err) = writer.feed(msg).await {
                            warn!("Couldn't send message to server: {err}");
                        }
                    }
                    writer.flush().await.unwrap();
                }

                // Stop signal received: exit the loop
                _ = &mut stop_signal => {
                    // Try to empty outgoing message queue before exiting
                    while let Ok(msg) = outgoing_messages.try_recv() {
                        let _ = writer.feed(msg).await;
                    }
                    let _ = writer.flush().await;
                    break;
                }
            }
        }
        info!("Connection to server closed");
    }

    pub fn send(&mut self, message: ClientMessage) {
        if let Err(e) = self.outgoing_messages.send(message) {
            error!("Couldn't send message to server: {e}");
        }
    }

    pub async fn shutdown(&mut self) {
        let stop_sender = self.writer_stop_signal.take().unwrap();
        let writer = self.writer_handle.take().unwrap();
        let _ = stop_sender.send(()).unwrap();
        let _ = writer.await;
    }
}

impl Stream for Network {
    type Item = ServerMessage;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.incoming_messages.poll_recv(cx)
    }
}

// impl Drop for Network {
//     // Shutdown reader/writer tasks on destruction
//     fn drop(&mut self) {
//         self.reader_handle.abort();
//         self.writer_handle.abort();
//     }
// }
