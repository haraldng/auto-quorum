use std::net::{SocketAddr, ToSocketAddrs};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio_serde::{formats::Bincode, Framed};
use tokio_util::codec::{Framed as CodecFramed, FramedRead, FramedWrite, LengthDelimitedCodec};

use crate::{kv::NodeId, messages::NetworkMessage};

pub type Connection = Framed<
    CodecFramed<TcpStream, LengthDelimitedCodec>,
    NetworkMessage,
    NetworkMessage,
    Bincode<NetworkMessage, NetworkMessage>,
>;

pub fn wrap_stream(stream: TcpStream) -> Connection {
    let length_delimited = CodecFramed::new(stream, LengthDelimitedCodec::new());
    Framed::new(length_delimited, Bincode::default())
}

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

pub type NetworkSource = Framed<
    FramedRead<OwnedReadHalf, LengthDelimitedCodec>,
    NetworkMessage,
    (),
    Bincode<NetworkMessage, ()>,
>;
pub type NetworkSink = Framed<
    FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>,
    (),
    NetworkMessage,
    Bincode<(), NetworkMessage>,
>;

/// Turns tcp stream into a framed read and write sink/source
pub fn wrap_split_stream(stream: TcpStream) -> (NetworkSource, NetworkSink) {
    let (reader, writer) = stream.into_split();
    let stream = FramedRead::new(reader, LengthDelimitedCodec::new());
    let sink = FramedWrite::new(writer, LengthDelimitedCodec::new());
    (
        NetworkSource::new(stream, Bincode::default()),
        NetworkSink::new(sink, Bincode::default()),
    )
}
