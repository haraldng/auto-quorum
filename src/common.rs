pub mod messages {
    use super::{
        configs::{DelayConfig, OmniPaxosServerConfig, PersistConfig},
        kv::*,
    };
    use omnipaxos::{messages::Message as OmniPaxosMessage, util::NodeId};
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub enum RegistrationMessage {
        NodeRegister(NodeId),
        ClientRegister,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub enum ClusterMessage {
        OmniPaxosMessage(OmniPaxosMessage<Command>),
        Done,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub enum ServerMessage {
        Ready(MetronomeConfigInfo),
        Write(CommandId),
        Read(CommandId, Option<String>),
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub enum ClientMessage {
        Append(CommandId, KVCommand),
        Done,
    }

    impl ServerMessage {
        pub fn command_id(&self) -> CommandId {
            match self {
                ServerMessage::Write(id) => *id,
                ServerMessage::Read(id, _) => *id,
                ServerMessage::Ready(_) => unimplemented!(),
            }
        }
    }

    #[derive(Debug, Serialize, Deserialize, Clone, Copy)]
    pub struct MetronomeConfigInfo {
        pub cluster_size: usize,
        pub use_metronome: usize,
        pub metronome_quorum_size: Option<usize>,
        pub persist_info: PersistInfo,
        pub delay_info: DelayInfo,
        pub instrumented: bool,
    }

    impl From<OmniPaxosServerConfig> for MetronomeConfigInfo {
        fn from(value: OmniPaxosServerConfig) -> Self {
            let persist_info = match value.persist_config {
                PersistConfig::Individual => PersistInfo::Individual,
                PersistConfig::Every(n) => PersistInfo::Every(n),
                PersistConfig::Opportunistic => PersistInfo::Opportunistic,
            };
            let delay_info = match value.delay_config {
                DelayConfig::Sleep(μs) => DelayInfo::Sleep(μs),
                DelayConfig::File(d) => DelayInfo::File(d),
            };
            MetronomeConfigInfo {
                cluster_size: value.cluster_config.nodes.len(),
                use_metronome: value.cluster_config.use_metronome,
                metronome_quorum_size: value.cluster_config.metronome_quorum_size,
                persist_info,
                delay_info,
                instrumented: value.instrumentation,
            }
        }
    }

    #[derive(Debug, Serialize, Deserialize, Clone, Copy)]
    pub enum PersistInfo {
        Individual,
        Every(usize),
        Opportunistic,
    }

    #[derive(Debug, Serialize, Deserialize, Clone, Copy)]
    pub enum DelayInfo {
        Sleep(u64),
        File(usize),
    }
}

pub mod configs {
    use omnipaxos::{util::NodeId, ClusterConfig, ServerConfig};
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize, Clone)]
    pub struct OmniPaxosServerConfig {
        pub cluster_name: String,
        pub location: String,
        pub initial_leader: Option<NodeId>,
        pub local_deployment: Option<bool>,
        pub persist_config: PersistConfig,
        pub delay_config: DelayConfig,
        pub instrumentation: bool,
        pub debug_filepath: String,
        pub server_config: ServerConfig,
        pub cluster_config: ClusterConfig,
    }

    #[derive(Debug, Serialize, Deserialize, Clone, Copy)]
    #[serde(tag = "delay_type", content = "delay_value")]
    pub enum DelayConfig {
        Sleep(u64),
        File(usize),
    }

    #[derive(Debug, Serialize, Deserialize, Clone, Copy)]
    #[serde(tag = "persist_type", content = "persist_value")]
    pub enum PersistConfig {
        Individual,
        Every(usize),
        Opportunistic,
    }
}

pub mod utils {
    use super::{kv::NodeId, messages::*};
    use std::net::{SocketAddr, ToSocketAddrs};
    use tokio::net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpStream,
    };
    use tokio_serde::{formats::Bincode, Framed};
    use tokio_util::codec::{Framed as CodecFramed, FramedRead, FramedWrite, LengthDelimitedCodec};

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

    // pub type ServerConnection = Framed<
    //     CodecFramed<TcpStream, LengthDelimitedCodec>,
    //     ServerMessage,
    //     ClientMessage,
    //     Bincode<ServerMessage, ClientMessage>,
    // >;
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

    pub fn frame_clients_connection(
        stream: TcpStream,
    ) -> (FromServerConnection, ToServerConnection) {
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

    pub fn frame_servers_connection(
        stream: TcpStream,
    ) -> (FromClientConnection, ToClientConnection) {
        let (reader, writer) = stream.into_split();
        let stream = FramedRead::new(reader, LengthDelimitedCodec::new());
        let sink = FramedWrite::new(writer, LengthDelimitedCodec::new());
        (
            FromClientConnection::new(stream, Bincode::default()),
            ToClientConnection::new(sink, Bincode::default()),
        )
    }
}

pub mod kv {
    use omnipaxos::{macros::Entry, storage::Snapshot};
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;

    pub type CommandId = usize;
    pub type ClientId = u64;
    pub type NodeId = omnipaxos::util::NodeId;

    #[derive(Debug, Clone, Entry, Serialize, Deserialize)]
    pub struct Command {
        pub client_id: ClientId,
        pub coordinator_id: NodeId,
        pub id: CommandId,
        pub kv_cmd: KVCommand,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub enum KVCommand {
        Put(String, String),
        Delete(String),
        Get(String),
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct KVSnapshot {
        snapshotted: HashMap<String, String>,
        deleted_keys: Vec<String>,
    }

    impl Snapshot<Command> for KVSnapshot {
        fn create(entries: &[Command]) -> Self {
            let mut snapshotted = HashMap::new();
            let mut deleted_keys: Vec<String> = Vec::new();
            for e in entries {
                match &e.kv_cmd {
                    KVCommand::Put(key, value) => {
                        snapshotted.insert(key.clone(), value.clone());
                    }
                    KVCommand::Delete(key) => {
                        if snapshotted.remove(key).is_none() {
                            // key was not in the snapshot
                            deleted_keys.push(key.clone());
                        }
                    }
                    KVCommand::Get(_) => (),
                }
            }
            // remove keys that were put back
            deleted_keys.retain(|k| !snapshotted.contains_key(k));
            Self {
                snapshotted,
                deleted_keys,
            }
        }

        fn merge(&mut self, delta: Self) {
            for (k, v) in delta.snapshotted {
                self.snapshotted.insert(k, v);
            }
            for k in delta.deleted_keys {
                self.snapshotted.remove(&k);
            }
            self.deleted_keys.clear();
        }

        fn use_snapshots() -> bool {
            true
        }
    }
}
