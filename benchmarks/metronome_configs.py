from __future__ import annotations

from dataclasses import asdict, dataclass, replace

import toml

from gcp_cluster import InstanceConfig


@dataclass(frozen=True)
class ClusterConfig:
    metronome_cluster_config: MetronomeClusterConfig
    server_configs: dict[int, ServerConfig]
    client_configs: dict[int, ClientConfig]
    client_image: str
    server_image: str

    @dataclass(frozen=True)
    class MetronomeClusterConfig:
        nodes: list[int]
        node_addrs: list[str]
        metronome_config: str
        batch_config: BatchConfig
        persist_config: PersistConfig
        worksteal_ms: int | None = None
        straggler: int | None = None
        initial_leader: int | None = None
        metronome_quorum_size: int | None = None

    def __post_init__(self):
        self.validate()

    # TODO: Validate that config won't cause deadlock due to parallel requests not reaching server batch io size
    def validate(self):
        met_config = self.metronome_cluster_config
        if met_config.metronome_config not in [
            "Off",
            "RoundRobin",
            "RoundRobin2",
            "FastestFollower",
        ]:
            raise ValueError(
                f"Invalid metronome_config: {met_config.metronome_config}. Expected one of ['Off', 'RoundRobin', 'RoundRobin2', 'FastestFollower']"
            )

        for client_id in self.client_configs.keys():
            if client_id not in self.server_configs.keys():
                raise ValueError(f"Client {client_id} has no server to connect to")

        if met_config.initial_leader:
            if met_config.initial_leader not in self.server_configs.keys():
                raise ValueError(
                    f"Initial leader {met_config.initial_leader} must be one of the server nodes"
                )

        if met_config.metronome_quorum_size is not None:
            majority = len(self.server_configs) // 2 + 1
            if met_config.metronome_quorum_size < majority:
                raise ValueError(
                    f"Metronome quorum size is {met_config.metronome_quorum_size}, but it can't be smaller than the majority ({majority})"
                )

        server_ids = sorted(self.server_configs.keys())
        if met_config.nodes != server_ids:
            raise ValueError(
                f"Cluster nodes {met_config.nodes} must match defined server ids {server_ids}"
            )

    def update_metronome_config(self, **kwargs) -> ClusterConfig:
        new_op_config = replace(self.metronome_cluster_config, **kwargs)
        new_config = replace(self, metronome_cluster_config=new_op_config)
        new_config.validate()
        return new_config

    def generate_cluster_toml(self) -> str:
        cluster_toml_str = toml.dumps(asdict(self.metronome_cluster_config))
        return cluster_toml_str


@dataclass(frozen=True)
class ServerConfig:
    instance_config: InstanceConfig
    metronome_server_config: MetronomeServerConfig
    rust_log: str
    server_address: str

    @dataclass(frozen=True)
    class MetronomeServerConfig:
        location: str
        server_id: int
        listen_address: str
        listen_port: int
        instrumentation: bool
        debug_filename: str
        persist_log_filepath: str

    def __post_init__(self):
        self.validate()

    def validate(self):
        met_config = self.metronome_server_config
        if met_config.server_id <= 0:
            raise ValueError(
                f"Invalid server_id: {met_config.server_id}. It must be greater than 0."
            )

        valid_rust_log_levels = ["error", "debug", "trace", "info", "warn"]
        if self.rust_log not in valid_rust_log_levels:
            raise ValueError(
                f"Invalid rust_log level: {self.rust_log}. Expected one of {valid_rust_log_levels}."
            )

    def update_metronome_config(self, **kwargs) -> ServerConfig:
        new_op_config = replace(self.metronome_server_config, **kwargs)
        new_config = replace(self, metronome_server_config=new_op_config)
        new_config.validate()
        return new_config

    def generate_server_toml(self) -> str:
        server_toml_str = toml.dumps(asdict(self.metronome_server_config))
        return server_toml_str


@dataclass(frozen=True)
class ClientConfig:
    instance_config: InstanceConfig
    metronome_client_config: MetronomeClientConfig
    rust_log: str = "info"

    @dataclass(frozen=True)
    class MetronomeClientConfig:
        location: str
        server_id: int
        server_address: str
        request_mode_config: RequestModeConfig
        end_condition: EndConditionConfig
        send_disable_config: int | None
        send_disable_command: str | None
        summary_filename: str
        summary_only: bool
        output_filename: str

    def __post_init__(self):
        self.validate()

    def validate(self):
        met_config = self.metronome_client_config
        if met_config.server_id <= 0:
            raise ValueError(
                f"Invalid server_id: {met_config.server_id}. It must be greater than 0."
            )

        valid_rust_log_levels = ["error", "debug", "trace", "info", "warn"]
        if self.rust_log not in valid_rust_log_levels:
            raise ValueError(
                f"Invalid rust_log level: {self.rust_log}. Expected one of {valid_rust_log_levels}."
            )

    def update_metronome_config(self, **kwargs) -> ClientConfig:
        new_op_config = replace(self.metronome_client_config, **kwargs)
        new_config = replace(self, metronome_client_config=new_op_config)
        new_config.validate()
        return new_config

    def generate_client_toml(self) -> str:
        client_toml_str = toml.dumps(asdict(self.metronome_client_config))
        return client_toml_str


@dataclass(frozen=True)
class EndConditionConfig:
    end_condition_type: str
    end_condition_value: int

    @staticmethod
    def ResponsesCollected(response_limit: int):
        return EndConditionConfig(
            end_condition_type="ResponsesCollected", end_condition_value=response_limit
        )

    @staticmethod
    def SecondsPassed(seconds: int):
        return EndConditionConfig(
            end_condition_type="SecondsPassed", end_condition_value=seconds
        )


@dataclass(frozen=True)
class PersistConfig:
    persist_type: str
    persist_value: int | None

    @staticmethod
    def NoPersist():
        return PersistConfig(persist_type="NoPersist", persist_value=None)

    @staticmethod
    def File(data_size: int):
        return PersistConfig(persist_type="File", persist_value=data_size)

    def __post_init__(self):
        self.validate()

    def validate(self):
        if self.persist_value is not None:
            assert self.persist_value >= 0

    def to_label(self) -> str:
        return f"{self.persist_type}{self.persist_value}"


@dataclass(frozen=True)
class BatchConfig:
    batch_type: str
    batch_value: int | None

    @staticmethod
    def Individual():
        return BatchConfig(batch_type="Individual", batch_value=None)

    @staticmethod
    def Every(interval: int):
        return BatchConfig(batch_type="Every", batch_value=interval)

    @staticmethod
    def Opportunistic():
        return BatchConfig(batch_type="Opportunistic", batch_value=None)

    def to_label(self) -> str:
        if self.batch_type == "Every":
            return f"Every{self.batch_value}"
        else:
            return f"{self.batch_type}"


@dataclass(frozen=True)
class RequestModeConfig:
    request_mode_config_type: str
    request_mode_config_value: int | list[int]

    @staticmethod
    def ClosedLoop(num_parallel_requests: int):
        return RequestModeConfig("ClosedLoop", num_parallel_requests)

    @staticmethod
    def OpenLoop(request_interval_ms: int, requests_per_interval: int):
        return RequestModeConfig(
            "OpenLoop", [request_interval_ms, requests_per_interval]
        )

    def to_label(self) -> str:
        if self.request_mode_config_value == "ClosedLoop":
            return f"ClosedLoop{self.request_mode_config_value}"
        else:
            assert isinstance(self.request_mode_config_value, list)
            return f"OpenLoop{self.request_mode_config_value[0]}-{self.request_mode_config_value[1]}"
