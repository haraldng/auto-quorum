use std::net::{SocketAddr, ToSocketAddrs};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio_serde::{formats::Bincode, Framed};
use tokio_util::codec::{Framed as CodecFramed, FramedRead, FramedWrite, LengthDelimitedCodec};

use crate::{kv::NodeId, messages::*};

pub fn get_node_addr(
    cluster_name: &String,
    node: NodeId,
    is_local: bool,
) -> Result<SocketAddr, std::io::Error> {
    let dns_name = if is_local {
        // format!("s{node}:800{node}")
        format!("localhost:800{node}")
    } else {
        format!("{cluster_name}-server-{node}.internal.zone.:800{node}")
    };
    let address = dns_name.to_socket_addrs()?.next().unwrap();
    Ok(address)
}

pub type RegistrationConnection = Framed<
    CodecFramed<TcpStream, LengthDelimitedCodec>,
    RegistrationMessage,
    RegistrationMessage,
    Bincode<RegistrationMessage, RegistrationMessage>,
>;

pub fn frame_registration_connection(stream: TcpStream) -> RegistrationConnection {
    let length_delimited = CodecFramed::new(stream, LengthDelimitedCodec::new());
    Framed::new(length_delimited, Bincode::default())
}

pub type FromNodeConnection = Framed<
    FramedRead<OwnedReadHalf, LengthDelimitedCodec>,
    ClusterMessage,
    (),
    Bincode<ClusterMessage, ()>,
>;
pub type ToNodeConnection = Framed<
    FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>,
    (),
    ClusterMessage,
    Bincode<(), ClusterMessage>,
>;

pub fn frame_cluster_connection(stream: TcpStream) -> (FromNodeConnection, ToNodeConnection) {
    let (reader, writer) = stream.into_split();
    let stream = FramedRead::new(reader, LengthDelimitedCodec::new());
    let sink = FramedWrite::new(writer, LengthDelimitedCodec::new());
    (
        FromNodeConnection::new(stream, Bincode::default()),
        ToNodeConnection::new(sink, Bincode::default()),
    )
}

pub type ServerConnection = Framed<
    CodecFramed<TcpStream, LengthDelimitedCodec>,
    ServerMessage,
    ClientMessage,
    Bincode<ServerMessage, ClientMessage>,
>;
pub type FromServerConnection = Framed<
    FramedRead<OwnedReadHalf, LengthDelimitedCodec>,
    ServerMessage,
    (),
    Bincode<ServerMessage, ()>,
>;
pub type ToServerConnection = Framed<
    FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>,
    (),
    ClientMessage,
    Bincode<(), ClientMessage>,
>;
pub type FromClientConnection = Framed<
    FramedRead<OwnedReadHalf, LengthDelimitedCodec>,
    ClientMessage,
    (),
    Bincode<ClientMessage, ()>,
>;
pub type ToClientConnection = Framed<
    FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>,
    (),
    ServerMessage,
    Bincode<(), ServerMessage>,
>;

pub fn frame_clients_connection(stream: TcpStream) -> (FromServerConnection, ToServerConnection) {
    let (reader, writer) = stream.into_split();
    let stream = FramedRead::new(reader, LengthDelimitedCodec::new());
    let sink = FramedWrite::new(writer, LengthDelimitedCodec::new());
    (
        FromServerConnection::new(stream, Bincode::default()),
        ToServerConnection::new(sink, Bincode::default()),
    )
}
// pub fn frame_clients_connection(stream: TcpStream) -> ServerConnection {
//     let length_delimited = CodecFramed::new(stream, LengthDelimitedCodec::new());
//     Framed::new(length_delimited, Bincode::default())
// }

pub fn frame_servers_connection(stream: TcpStream) -> (FromClientConnection, ToClientConnection) {
    let (reader, writer) = stream.into_split();
    let stream = FramedRead::new(reader, LengthDelimitedCodec::new());
    let sink = FramedWrite::new(writer, LengthDelimitedCodec::new());
    (
        FromClientConnection::new(stream, Bincode::default()),
        ToClientConnection::new(sink, Bincode::default()),
    )
}
