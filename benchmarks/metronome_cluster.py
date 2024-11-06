import subprocess
from dataclasses import dataclass
from typing import Optional
from pathlib import Path

from gcp_cluster import GcpCluster, InstanceConfig

def create_directories(directory: Path):
    subprocess.run(["mkdir", "-p", directory])

class MetronomeCluster:
    _server_processes: dict[int, subprocess.Popen]
    _client_processes: dict[int, subprocess.Popen]

    @dataclass
    class ClusterConfig:
        name: str
        nodes: list[int]
        use_metronome: Optional[int]=None
        metronome_quorum_size: Optional[int]=None
        flexible_quorum: Optional[tuple[int, int]]=None
        initial_leader: Optional[int]=None

    @dataclass
    class ServerConfig:
        server_id: int
        instance_config: InstanceConfig
        persist_config: 'MetronomeCluster.PersistConfig'
        delay_config: 'MetronomeCluster.DelayConfig'
        rust_log: str="warn"

    @dataclass
    class ClientConfig:
        client_id: int
        instance_config: InstanceConfig
        total_requests: int
        num_parallel_requests: int
        rust_log: str="warn"

    @dataclass
    class DelayConfig:
        delay_type: str
        delay_value: int

        @staticmethod
        def Sleep(duration: int):
            return MetronomeCluster.DelayConfig(delay_type="Sleep", delay_value=duration)
        @staticmethod
        def File(size: int):
            return MetronomeCluster.DelayConfig(delay_type="File", delay_value=size)

    @dataclass
    class PersistConfig:
        persist_type: str
        persist_value: Optional[int] = None

        @staticmethod
        def NoPersist():
            return MetronomeCluster.PersistConfig(persist_type="NoPersist")
        @staticmethod
        def Individual():
            return MetronomeCluster.PersistConfig(persist_type="Individual")
        @staticmethod
        def Every(interval: int):
            return MetronomeCluster.PersistConfig(persist_type="Every", persist_value=interval)
        @staticmethod
        def Opportunistic():
            return MetronomeCluster.PersistConfig(persist_type="Opportunistic")

        def to_label(self) -> str:
            if self.persist_type == "Every":
                return f"Every({self.persist_value})"
            else:
                return f"{self.persist_type}"

    def __init__(self, project_id: str, credentials_file: str, cluster_config: ClusterConfig, server_configs: dict[int, ServerConfig], client_configs: dict[int, ClientConfig]):
        self._server_configs = server_configs
        self._server_processes = {}
        self._client_processes = {}
        self._client_configs = client_configs
        self._cluster_config = cluster_config
        instance_configs = [c.instance_config for c in server_configs.values()]
        instance_configs.extend([c.instance_config for c in client_configs.values()])
        self._gcp_cluster = GcpCluster(project_id, credentials_file, instance_configs)

    def start_servers(self):
        print("Starting servers")
        for server_id in self._server_configs.keys():
            self.start_server(server_id)

    def start_server(self, server_id: int):
        server_config = self._server_configs.get(server_id)
        assert server_config is not None, f"Server {server_id} doesn't exist"
        current_server_process = self._server_processes.get(server_id)
        if current_server_process is not None:
            current_server_process.terminate()
        start_command = start_server_command(self._cluster_config, server_config)
        server_process = self._gcp_cluster.ssh_command(server_config.instance_config.name, start_command)
        self._server_processes[server_id] = server_process

    def stop_server(self, server_id: int):
        server_config = self._server_configs.get(server_id)
        assert server_config is not None, f"Server {server_id} doesn't exist"
        current_server_process = self._server_processes.pop(server_id, None)
        if current_server_process is not None:
            current_server_process.terminate()

    def stop_servers(self):
        for server_id in self._server_configs.keys():
            self.stop_server(server_id)
    
    def await_servers(self):
        print(f"Awaiting servers...")
        for server_process in self._server_processes.values():
            server_process.wait(timeout=600)

    def start_client(self, id: int):
        client_config = self._client_configs.get(id)
        assert client_config is not None, f"Client {id} doesn't exist"
        current_client_process = self._client_processes.get(id)
        if current_client_process is not None:
            current_client_process.terminate()
        start_command = start_client_command(self._cluster_config, client_config)
        client_process = self._gcp_cluster.ssh_command(client_config.instance_config.name, start_command)
        self._client_processes[id] = client_process

    def await_client(self, id: int):
        print(f"Awaiting client {id}...")
        client_process = self._client_processes.get(id)
        assert client_process is not None, f"Client process {id} doesn't exist"
        client_process.wait(timeout=600)

    def shutdown(self):
        instance_names = [c.instance_config.name for c in self._server_configs.values()]
        client_names = [c.instance_config.name for c in self._client_configs.values()]
        instance_names.extend(client_names)
        self.stop_servers()
        self._gcp_cluster.shutdown_instances(instance_names)

    def get_logs(self, dest_directory: Path, file_prefix: str):
        create_directories(dest_directory)
        processes = []
        for config in self._server_configs.values():
            name = config.instance_config.name
            # Get logs
            log_src_path = f"~/server-{config.server_id}.json"
            log_dest_path = f"{dest_directory}/{file_prefix}_server-{config.server_id}.json"
            scp_process = self._gcp_cluster.scp_command(name, log_src_path, log_dest_path)
            processes.append(scp_process)
            # Get stderr
            err_src_path = f"~/server-{config.server_id}-err.log"
            err_dest_path = f"{dest_directory}/xerr-{file_prefix}_server-{config.server_id}.log"
            scp_process = self._gcp_cluster.scp_command(name, err_src_path, err_dest_path)
            processes.append(scp_process)
        for config in self._client_configs.values():
            name = config.instance_config.name
            log_src_path = f"~/client-{config.client_id}.json"
            log_dest_path = f"{dest_directory}/{file_prefix}_client-{config.client_id}.json"
            scp_process = self._gcp_cluster.scp_command(name, log_src_path, log_dest_path)
            processes.append(scp_process)
        total_logs = 0
        for process in processes:
            process.wait()
            if process.returncode == 0:
                total_logs += 1
        print(f"Collected {total_logs} logs")

    def change_cluster_config(
        self,
        use_metronome: Optional[int]=None,
        metronome_quorum_size: Optional[int]=None,
        flexible_quorum: Optional[tuple[int, int]]=None,
    ):
        if use_metronome is not None:
            self._cluster_config.use_metronome = use_metronome
        if metronome_quorum_size is not None:
            self._cluster_config.metronome_quorum_size = metronome_quorum_size
        if flexible_quorum is not None:
            self._cluster_config.flexible_quorum = flexible_quorum

    def change_server_config(
        self,
        server_id: int,
        persist_config: Optional[PersistConfig]=None,
        delay_config: Optional[DelayConfig]=None,
        rust_log: Optional[str]=None,
    ):
        if persist_config is not None:
            self._server_configs[server_id].persist_config = persist_config
        if delay_config is not None:
            self._server_configs[server_id].delay_config = delay_config
        if rust_log is not None:
            self._server_configs[server_id].rust_log = rust_log

    def change_client_config(
        self,
        client_id: int,
        total_requests: Optional[int] = None,
        num_parallel_requests: Optional[int] = None,
        rust_log: Optional[str]=None,
    ):
        if total_requests is not None:
            self._client_configs[client_id].total_requests = total_requests
        if num_parallel_requests is not None:
            self._client_configs[client_id].num_parallel_requests = num_parallel_requests
        if rust_log is not None:
            self._client_configs[client_id].rust_log = rust_log

class MetronomeClusterBuilder:
    def __init__(self, name:str, project_id: str = "my-project-1499979282244", credentials_file: str = "service-account-key.json") -> None:
        self.name = name
        self._project_id = project_id
        self._credentials_file = credentials_file
        self._server_configs: dict[int, MetronomeCluster.ServerConfig] = {}
        self._client_configs: dict[int, MetronomeCluster.ClientConfig] = {}
        self._use_metronome: Optional[int] = None
        self._metronome_quorum_size: Optional[int] = None
        self._flexible_quorum: Optional[tuple[int, int]] = None
        self._initial_leader: Optional[int] = None

    def add_server(
        self,
        server_id: int,
        zone: str,
        machine_type: str = "e2-standard-8",
        persist_config: MetronomeCluster.PersistConfig = MetronomeCluster.PersistConfig.NoPersist(),
        delay_config: MetronomeCluster.DelayConfig = MetronomeCluster.DelayConfig.Sleep(0),
        rust_log: str="warn"
    ):
        assert server_id > 0
        assert server_id not in self._server_configs.keys(), f"Server {server_id} already exists"
        assert rust_log in ["error", "debug", "trace", "info", "warn"]
        assert delay_config.delay_value >= 0
        if persist_config.persist_value:
            assert persist_config.persist_value > 0
        instance_config = InstanceConfig(
            f"{self.name}-server-{server_id}",
            zone,
            machine_type,
            server_startup_script(),
            firewall_tag="omnipaxos-server",
            dns_name=f"{self.name}-server-{server_id}",
        )
        server_config = MetronomeCluster.ServerConfig(
            server_id,
            instance_config,
            persist_config=persist_config,
            delay_config=delay_config,
            rust_log=rust_log,
        )
        self._server_configs[server_id] = server_config
        return self

    def add_client(
        self,
        nearest_server: int,
        zone: str,
        total_requests: int,
        num_parallel_requests: int,
        machine_type: str = "e2-standard-4",
        rust_log: str="warn"
    ):
        assert nearest_server > 0
        assert nearest_server not in self._client_configs.keys(), f"Client {nearest_server} already exists"
        assert rust_log in ["error", "debug", "trace", "info", "warn"]
        instance_config = InstanceConfig(
            f"{self.name}-client-{nearest_server}",
            zone,
            machine_type,
            client_startup_script(),
        )
        client_config = MetronomeCluster.ClientConfig(
            nearest_server,
            instance_config,
            total_requests=total_requests,
            num_parallel_requests=num_parallel_requests,
            rust_log=rust_log,
        )
        self._client_configs[nearest_server] = client_config
        return self

    def use_metronome(self, use_metronome: int):
        assert use_metronome in [0,1,2]
        self._use_metronome = use_metronome
        return self

    def metronome_quorum_size(self, metronome_quorum_size: int):
        assert metronome_quorum_size > 0
        self._metronome_quorum_size = metronome_quorum_size
        return self

    def flexible_quorum(self, flexible_quorum: tuple[int, int]):
        assert flexible_quorum[0] >= 2, "Omnipaxos doesn't support singleton quorums"
        assert flexible_quorum[1] >= 2, "Omnipaxos doesn't support singleton quorums"
        self._flexible_quorum = flexible_quorum
        return self

    def initial_leader(self, initial_leader: int):
        self._initial_leader = initial_leader
        return self

    def build(self) -> MetronomeCluster:
        # Validate config
        for (client_id, _config) in self._client_configs.items():
            assert client_id in self._server_configs.keys(), f"Client {client_id} has no server to connect to"
        assert self._initial_leader in self._server_configs.keys()
        if self._flexible_quorum is not None:
            read_quorum = self._flexible_quorum[0]
            write_quorum = self._flexible_quorum[1]
            cluster_size = len(self._server_configs)
            assert read_quorum + write_quorum > cluster_size, "Flexible quorum must guarantee overlap" 
            assert read_quorum <= cluster_size
            assert write_quorum <= cluster_size
        if self._metronome_quorum_size is not None:
            if self._flexible_quorum is not None:
                write_quorum = self._flexible_quorum[1]
                assert self._metronome_quorum_size >= write_quorum, f"Metronome quorum is {self._metronome_quorum_size} but it can't be smaller than write quorum which is {write_quorum}"
            else:
                majority = len(self._server_configs) // 2 + 1
                assert self._metronome_quorum_size >= majority, f"Metronome quorum is {self._metronome_quorum_size} but it can't be smaller than majority which is {majority}"
        cluster_config = MetronomeCluster.ClusterConfig(
            self.name,
            sorted(self._server_configs.keys()),
            use_metronome=self._use_metronome,
            initial_leader=self._initial_leader,
        )
        return MetronomeCluster(self._project_id, self._credentials_file, cluster_config, self._server_configs, self._client_configs)


# TODO: GCP creates the user for us (in my case as "kevin") unless we do something like this: https://cloud.google.com/compute/docs/instances/ssh#third-party-tools
def server_startup_script() -> str:
    user = "kevin"
    container_image_location = "my-project-1499979282244/metronome_server"
    return f"""#! /bin/bash
# Create deployment user
sudo useradd -m "{user}"

# Configure Docker credentials for the user
sudo -u {user} docker-credential-gcr configure-docker --registries=gcr.io
sudo -u {user} echo "https://gcr.io" | docker-credential-gcr get
sudo groupadd docker
sudo usermod -aG docker {user}

# Pull the container as user
sudo -u {user} docker pull "gcr.io/{container_image_location}"
"""

def start_server_command(
    cluster_config: MetronomeCluster.ClusterConfig,
    config: MetronomeCluster.ServerConfig,
) -> str:
    user = "kevin"
    container_name = "server"
    container_image_location = "my-project-1499979282244/metronome_server"
    logs_location = f"/home/{user}/server-{config.server_id}.json"
    err_location = f"/home/{user}/server-{config.server_id}-err.log"
    config_location = f"/home/{user}/server-config.toml"
    server_config = _generate_server_config(cluster_config, config)
    rust_log = config.rust_log

    kill_prev_container_command = f"docker kill {container_name} > /dev/null 2>&1"
    gen_config_command = f"echo -e '{server_config}' > {config_location}"
    docker_command = f"""docker run \\
        --name {container_name} \\
        -p 800{config.server_id}:800{config.server_id} \\
        --env RUST_LOG={rust_log} \\
        --env CONFIG_FILE="{config_location}" \\
        -v {config_location}:{config_location} \\
        --rm \\
        "gcr.io/{container_image_location}" \\
        1> {logs_location} 2> {err_location}"""
    # NOTE: We sleep here to ensure we don't connect to currently shutting
    # down servers.
    return f"{kill_prev_container_command} ; sleep 1 ; {gen_config_command} && {docker_command}"

def _generate_server_config(
    cluster_config: MetronomeCluster.ClusterConfig,
    config: MetronomeCluster.ServerConfig,
) -> str:
    persist_value_toml = f"persist_value = {config.persist_config.persist_value}" if config.persist_config.persist_value is not None else ""
    use_metronome_toml = f"use_metronome = {cluster_config.use_metronome}" if cluster_config.use_metronome is not None else ""
    metronome_quorum_toml = f"metronome_quorum_size = {cluster_config.metronome_quorum_size}" if cluster_config.metronome_quorum_size is not None else ""
    flex_quorum_toml = f"flexible_quorum = {{ read_quorum_size = {cluster_config.flexible_quorum[0]}, write_quorum_size = {cluster_config.flexible_quorum[1]} }}" if cluster_config.flexible_quorum is not None else ""
    init_leader_toml = f"initial_leader = {cluster_config.initial_leader}" if cluster_config.initial_leader is not None else ""

    toml = f"""
cluster_name = \\"{cluster_config.name}\\"
location = \\"{config.instance_config.zone}\\"
{init_leader_toml}

[persist_config]
persist_type = \\"{config.persist_config.persist_type}\\"
{persist_value_toml}

[delay_config]
delay_type = \\"{config.delay_config.delay_type}\\"
delay_value = {config.delay_config.delay_value}

[cluster_config]
configuration_id = 1
nodes = {cluster_config.nodes}
{use_metronome_toml}
{metronome_quorum_toml}
{flex_quorum_toml}

[server_config]
pid = {config.server_id}
election_tick_timeout = 1
resend_message_tick_timeout = 5
flush_batch_tick_timeout = 200
batch_size = 1
"""
    return toml.strip().replace("\n", "\\n")


# TODO: GCP creates the user for us (in my case as "kevin") unless we do something like this: https://cloud.google.com/compute/docs/instances/ssh#third-party-tools
def client_startup_script() -> str:
    user = "kevin"
    container_image_location = "my-project-1499979282244/metronome_client"
    return f"""#! /bin/bash
# Create deployment user
sudo useradd -m "{user}"

# Configure Docker credentials for the user
sudo -u {user} docker-credential-gcr configure-docker --registries=gcr.io
sudo -u {user} echo "https://gcr.io" | docker-credential-gcr get
sudo groupadd docker
sudo usermod -aG docker {user}

# Pull the container as user
sudo -u {user} docker pull "gcr.io/{container_image_location}"
"""
 
def start_client_command(
    cluster_config: MetronomeCluster.ClusterConfig,
    config: MetronomeCluster.ClientConfig,
) -> str:
    user = "kevin"
    container_name = "client"
    container_image_location = "my-project-1499979282244/metronome_client"
    config_location = f"/home/{user}/client-config.toml"
    logs_location = f"/home/{user}/client-{config.client_id}.json"
    client_config = _generate_client_config(cluster_config, config)

    kill_prev_container_command = f"docker kill {container_name} > /dev/null 2>&1"
    gen_config_command = f"echo -e '{client_config}' > {config_location}"
    docker_command = f"""docker run \\
    --name={container_name} \\
    --rm \\
    --env RUST_LOG={config.rust_log} \\
    --env CONFIG_FILE={config_location} \\
    -v {config_location}:{config_location} \\
    gcr.io/{container_image_location} > {logs_location}"""
    # NOTE: We sleep here to ensure we don't connect to currently shutting down servers.
    return f"{kill_prev_container_command} ; sleep 1 ; {gen_config_command} && {docker_command}"

def _generate_client_config(
    cluster_config: MetronomeCluster.ClusterConfig,
    config: MetronomeCluster.ClientConfig,
) -> str:
    use_metronome_toml = f"use_metronome = {cluster_config.use_metronome}" if cluster_config.use_metronome is not None else ""
    metronome_quorum_toml = f"metronome_quorum_size = {cluster_config.metronome_quorum_size}" if cluster_config.metronome_quorum_size is not None else ""
    flex_quorum_toml = f"flexible_quorum = {{ read_quorum_size = {cluster_config.flexible_quorum[0]}, write_quorum_size = {cluster_config.flexible_quorum[1]} }}" if cluster_config.flexible_quorum is not None else ""
    toml = f"""
cluster_name = \\"{cluster_config.name}\\"
location = \\"{config.instance_config.zone}\\"
server_id = {config.client_id}
total_requests = {config.total_requests}
num_parallel_requests = {config.num_parallel_requests}
nodes = {cluster_config.nodes}
{use_metronome_toml}
{metronome_quorum_toml}
{flex_quorum_toml}
"""
    return toml.strip().replace("\n", "\\n")
