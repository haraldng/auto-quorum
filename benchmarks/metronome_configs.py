from __future__ import annotations

from dataclasses import asdict, dataclass, fields, replace

import toml

from gcp_cluster import InstanceConfig


@dataclass(frozen=True)
class ClusterConfig:
    cluster_name: str
    nodes: list[int]
    metronome_config: str
    batch_config: BatchConfig
    persist_config: PersistConfig
    server_configs: dict[int, ServerConfig]
    client_configs: dict[int, ClientConfig]
    initial_leader: int | None = None
    metronome_quorum_size: int | None = None

    def __post_init__(self):
        self.validate()

    # TODO: Validate that config won't cause deadlock due to parallel requests not reaching server batch io size
    def validate(self):
        if self.metronome_config not in [
            "Off",
            "RoundRobin",
            "RoundRobin2",
            "FastestFollower",
        ]:
            raise ValueError(
                f"Invalid metronome_config: {self.metronome_config}. Expected one of ['Off', 'RoundRobin', 'RoundRobin2', 'FastestFollower']"
            )

        for client_id in self.client_configs.keys():
            if client_id not in self.server_configs.keys():
                raise ValueError(f"Client {client_id} has no server to connect to")

        if self.initial_leader:
            if self.initial_leader not in self.server_configs.keys():
                raise ValueError(
                    f"Initial leader {self.initial_leader} must be one of the server nodes"
                )

        if self.metronome_quorum_size is not None:
            majority = len(self.server_configs) // 2 + 1
            if self.metronome_quorum_size < majority:
                raise ValueError(
                    f"Metronome quorum size is {self.metronome_quorum_size}, but it can't be smaller than the majority ({majority})"
                )

        server_ids = sorted(self.server_configs.keys())
        if self.nodes != server_ids:
            raise ValueError(
                f"Cluster nodes {self.nodes} must match defined server ids {server_ids}"
            )

    def with_updated(self, **kwargs) -> ClusterConfig:
        new_config = replace(self, **kwargs)
        new_config.validate()
        return new_config


@dataclass(frozen=True)
class ServerConfig:
    instance_config: InstanceConfig
    server_id: int
    instrumentation: bool
    debug_filename: str
    persist_log_filepath: str
    rust_log: str = "info"

    @dataclass(frozen=True)
    class MetronomeServerToml:
        location: str
        server_id: int
        instrumentation: bool
        debug_filename: str
        persist_log_filepath: str
        # Cluster-wide config
        cluster_name: str
        nodes: list[int]
        metronome_config: str
        batch_config: BatchConfig
        persist_config: PersistConfig
        initial_leader: int | None = None
        metronome_quorum_size: int | None = None

    def __post_init__(self):
        self.validate()

    def validate(self):
        if self.server_id <= 0:
            raise ValueError(
                f"Invalid server_id: {self.server_id}. It must be greater than 0."
            )

        valid_rust_log_levels = ["error", "debug", "trace", "info", "warn"]
        if self.rust_log not in valid_rust_log_levels:
            raise ValueError(
                f"Invalid rust_log level: {self.rust_log}. Expected one of {valid_rust_log_levels}."
            )

    def with_updated(self, **kwargs) -> ServerConfig:
        new_config = replace(self, **kwargs)
        return new_config

    def generate_server_toml(self, cluster_config: ClusterConfig) -> str:
        server_toml = ServerConfig.MetronomeServerToml(
            location=self.instance_config.zone,
            server_id=self.server_id,
            instrumentation=self.instrumentation,
            debug_filename=self.debug_filename,
            persist_log_filepath=self.persist_log_filepath,
            cluster_name=cluster_config.cluster_name,
            nodes=cluster_config.nodes,
            metronome_config=cluster_config.metronome_config,
            batch_config=cluster_config.batch_config,
            persist_config=cluster_config.persist_config,
            initial_leader=cluster_config.initial_leader,
            metronome_quorum_size=cluster_config.metronome_quorum_size,
        )
        server_toml_str = toml.dumps(asdict(server_toml))
        return server_toml_str


@dataclass(frozen=True)
class ClientConfig:
    instance_config: InstanceConfig
    server_id: int
    request_mode_config: RequestModeConfig
    end_condition: EndConditionConfig
    summary_filename: str
    summary_only: bool
    output_filename: str
    rust_log: str = "info"

    @dataclass(frozen=True)
    class MetronomeClientToml:
        cluster_name: str
        location: str
        server_id: int
        request_mode_config: RequestModeConfig
        end_condition: EndConditionConfig
        summary_filename: str
        summary_only: bool
        output_filename: str

    def __post_init__(self):
        self.validate()

    def validate(self):
        if self.server_id <= 0:
            raise ValueError(
                f"Invalid server_id: {self.server_id}. It must be greater than 0."
            )

        valid_rust_log_levels = ["error", "debug", "trace", "info", "warn"]
        if self.rust_log not in valid_rust_log_levels:
            raise ValueError(
                f"Invalid rust_log level: {self.rust_log}. Expected one of {valid_rust_log_levels}."
            )

    def with_updated(self, **kwargs) -> ClientConfig:
        new_config = replace(self, **kwargs)
        return new_config

    def generate_client_toml(self, cluster_config: ClusterConfig) -> str:
        toml_fields = {f.name for f in fields(ClientConfig.MetronomeClientToml)}
        shared_fields = {k: v for k, v in asdict(self).items() if k in toml_fields}
        client_toml = ClientConfig.MetronomeClientToml(
            cluster_name=cluster_config.cluster_name,
            location=self.instance_config.zone,
            **shared_fields,
        )
        client_toml_str = toml.dumps(asdict(client_toml))
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
