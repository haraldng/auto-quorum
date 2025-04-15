pub mod messages {
    use super::kv::*;
    use omnipaxos::{messages::Message as OmniPaxosMessage, util::NodeId, MetronomeSetting};
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub enum RegistrationMessage {
        NodeRegister(NodeId),
        ClientRegister,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub enum ClusterMessage {
        OmniPaxosMessage(OmniPaxosMessage<Command>),
        Disable(NodeId, String),
        Done,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub enum ServerMessage {
        Ready(MetronomeConfigInfo),
        Write(CommandId),
        Read(CommandId, Option<usize>),
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub enum ClientMessage {
        Append(CommandId, KVCommand),
        BatchAppend(Vec<(CommandId, KVCommand)>),
        Disable(NodeId, String),
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
        pub metronome_info: MetronomeSetting,
        pub metronome_quorum_size: Option<usize>,
        pub batch_info: BatchInfo,
        pub persist_info: PersistInfo,
        pub worksteal_ms: Option<u64>,
        pub straggler: Option<NodeId>,
        pub instrumented: bool,
    }

    #[derive(Debug, Serialize, Deserialize, Clone, Copy)]
    pub enum BatchInfo {
        Individual,
        Every(usize),
        Opportunistic,
    }

    #[derive(Debug, Serialize, Deserialize, Clone, Copy)]
    pub enum PersistInfo {
        NoPersist,
        File(usize),
    }
}

pub mod utils {
    use super::messages::*;
    use tokio::net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpStream,
    };
    use tokio_serde::{formats::Bincode, Framed};
    use tokio_util::codec::{Framed as CodecFramed, FramedRead, FramedWrite, LengthDelimitedCodec};

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

    #[derive(Debug, Clone, Copy, Serialize, Deserialize)]
    pub enum KVCommand {
        Put(usize, usize),
        Delete(usize),
        Get(usize),
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct KVSnapshot {
        snapshotted: HashMap<usize, usize>,
        deleted_keys: Vec<usize>,
    }

    impl Snapshot<Command> for KVSnapshot {
        fn create(entries: &[Command]) -> Self {
            let mut snapshotted = HashMap::new();
            let mut deleted_keys: Vec<usize> = Vec::new();
            for e in entries {
                match e.kv_cmd {
                    KVCommand::Put(key, value) => {
                        snapshotted.insert(key, value);
                    }
                    KVCommand::Delete(key) => {
                        if snapshotted.remove(&key).is_none() {
                            // key was not in the snapshot
                            deleted_keys.push(key);
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
