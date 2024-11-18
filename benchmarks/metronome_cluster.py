import subprocess
import time
import toml
from dataclasses import dataclass, asdict
from typing import Optional
from pathlib import Path

from gcp_cluster import GcpCluster, InstanceConfig

def create_directories(directory: Path):
    subprocess.run(["mkdir", "-p", directory])

# Orchestration class for a Metronome Cluster.
# Deployment steps:
# 1. Push Metronome server and client Docker images to gcr.io artifact registry. See ../build_scripts/push-server-image.sh and ../build_scripts/push-client-image.sh
# 2. `__init__` Creates GCP instances for Metronome servers and clients which run passed startup scripts to setup Docker on the instances for your OS login user.
# 3. `start_servers` and `start_client` to ssh in to instances, pass configuration files, and docker run the containers from the artifact registry.
# 4. `await_cluster` to wait for ssh processes to finish.
# 5. `get_logs` pull logs from the GCP instances
# 6. `shutdown` shutdown GCP instances (or not it you want to reuse them)
class MetronomeCluster:
    _server_processes: dict[int, subprocess.Popen]
    _client_processes: dict[int, subprocess.Popen]
    STDERR_INSTANCE_LOOKUP_FAILURE: str = "ERROR: (gcloud.compute.start-iap-tunnel) Error while connecting [4047: 'Failed to lookup instance'].\n"

    @dataclass
    class ClusterConfig:
        cluster_name: str
        nodes: list[int]
        metronome_config: str
        persist_config: 'MetronomeCluster.PersistConfig'
        delay_config: 'MetronomeCluster.DelayConfig'
        initial_leader: Optional[int]=None
        metronome_quorum_size: Optional[int]=None

    @dataclass
    class ServerConfig:
        instance_config: InstanceConfig
        server_id: int
        instrumentation: bool
        debug_filename: str
        rust_log: str="info"

    @dataclass
    class ClientConfig:
        instance_config: InstanceConfig
        server_id: int
        end_condition: 'MetronomeCluster.EndConditionConfig'
        num_parallel_requests: int
        summary_filename: str
        output_filename: str
        rust_log: str="info"

    @dataclass
    class EndConditionConfig:
        end_condition_type: str
        end_condition_value: int

        @staticmethod
        def ResponsesCollected(response_limit: int):
            return MetronomeCluster.EndConditionConfig(end_condition_type="ResponsesCollected", end_condition_value=response_limit)
        @staticmethod
        def SecondsPassed(seconds: int):
            return MetronomeCluster.EndConditionConfig(end_condition_type="SecondsPassed", end_condition_value=seconds)

    @dataclass
    class DelayConfig:
        delay_type: str
        delay_value: int

        @staticmethod
        def Sleep(microseconds: int):
            return MetronomeCluster.DelayConfig(delay_type="Sleep", delay_value=microseconds)
        @staticmethod
        def File(data_size: int):
            return MetronomeCluster.DelayConfig(delay_type="File", delay_value=data_size)

        def to_label(self) -> str:
            return f"{self.delay_type}{self.delay_value}"

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
                return f"Every{self.persist_value}"
            else:
                return f"{self.persist_type}"

    @dataclass
    class MetronomeServerToml:
        location: str
        server_id: int
        instrumentation: bool
        debug_filename: str
        # Cluster-wide config
        cluster_name: str
        nodes: list[int]
        metronome_config: str
        persist_config: 'MetronomeCluster.PersistConfig'
        delay_config: 'MetronomeCluster.DelayConfig'
        initial_leader: Optional[int]=None
        metronome_quorum_size: Optional[int]=None

    @dataclass
    class MetronomeClientToml:
        cluster_name: str
        location: str
        server_id: int
        end_condition: 'MetronomeCluster.EndConditionConfig'
        num_parallel_requests: int
        summary_filename: str
        output_filename: str


    def __init__(self, project_id: str, cluster_config: ClusterConfig, server_configs: dict[int, ServerConfig], client_configs: dict[int, ClientConfig]):
        self._server_configs = server_configs
        self._server_processes = {}
        self._client_processes = {}
        self._client_configs = client_configs
        self._cluster_config = cluster_config
        instance_configs = [c.instance_config for c in server_configs.values()]
        instance_configs.extend([c.instance_config for c in client_configs.values()])
        self._gcp_cluster = GcpCluster(project_id, instance_configs)

    def start_servers(self, pull_images: bool=False):
        print("Starting servers")
        for server_id in self._server_configs.keys():
            self.start_server(server_id, pull_images)

    def start_server(self, server_id: int, pull_image: bool=False):
        server_config = self._server_configs.get(server_id)
        assert server_config is not None, f"Server {server_id} doesn't exist"
        current_server_process = self._server_processes.get(server_id)
        if current_server_process is not None:
            current_server_process.terminate()
        start_command = start_server_command(self._cluster_config, server_config, pull_image=pull_image)
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

    def start_client(self, id: int, pull_image: bool=False):
        client_config = self._client_configs.get(id)
        assert client_config is not None, f"Client {id} doesn't exist"
        current_client_process = self._client_processes.get(id)
        if current_client_process is not None:
            current_client_process.terminate()
        start_command = start_client_command(self._cluster_config, client_config, pull_image=pull_image)
        client_process = self._gcp_cluster.ssh_command(client_config.instance_config.name, start_command, capture_stderr=True)
        self._client_processes[id] = client_process

    # Wait for cluster processes to finish.
    # Retries SSH connections if client fails due to instance lookup failure. This is necessary
    # because we ssh with gcloud's tunnel-through-iap. There is a delay between when a GCP instance
    # starts and when the GCP IAP proxy service can find it. To address this we retry the cluster's
    # ssh processes if the client process receives the instance lookup failure error.
    def await_cluster(self):
        print(f"Awaiting client...")
        assert len(self._client_processes) > 0, "Need a client to exist to await on cluster"
        tries = 0
        while True:
            client_id, client_process = self._client_processes.popitem()
            assert client_process.stderr is not None, "Client processes should capture stderr"

            # Capture and print client process stderr
            ssh_err = False
            for line in iter(client_process.stderr.readline, ""):
                if line == self.STDERR_INSTANCE_LOOKUP_FAILURE:
                    ssh_err = True
                    print(line, end="")
                    break
                print(line, end="")
            client_process.stderr.close()
            client_process.wait()

            # Retry cluster processes if client failed due to ssh instance lookup error
            if ssh_err and tries < 3:
                print(f"Retrying client and server SSH connections")
                time.sleep(3)
                tries += 1
                self.start_servers()
                self.start_client(client_id)
            else:
                break
        if ssh_err:
            print("Failed SSH 3 times")
            self.stop_servers()
        else:
            # TODO: should await servers here but when running for a long time it seems server docker containers end, but ssh sessions don't end
            # self.await_servers()
            print("Cluster finished")

    def shutdown(self):
        instance_names = [c.instance_config.name for c in self._server_configs.values()]
        client_names = [c.instance_config.name for c in self._client_configs.values()]
        instance_names.extend(client_names)
        self.stop_servers()
        self._gcp_cluster.shutdown_instances(instance_names)

    def get_logs(self, dest_directory: Path):
        create_directories(dest_directory)
        instance_results_dir = "./results"
        processes = []
        for config in self._server_configs.values():
            name = config.instance_config.name
            scp_process = self._gcp_cluster.scp_command(name, instance_results_dir, dest_directory)
            processes.append(scp_process)
        for config in self._client_configs.values():
            name = config.instance_config.name
            scp_process = self._gcp_cluster.scp_command(name, instance_results_dir, dest_directory)
            processes.append(scp_process)
        successes = 0
        for process in processes:
            process.wait()
            if process.returncode == 0:
                successes += 1
        print(f"Collected logs from {successes} instances")

    def change_cluster_config(
        self,
        metronome_config: Optional[str]=None,
        persist_config: Optional[PersistConfig]=None,
        delay_config: Optional[DelayConfig]=None,
        initial_leader: Optional[int]=None,
        metronome_quorum_size: Optional[int]=None,
    ):
        if metronome_config is not None:
            self._cluster_config.metronome_config = metronome_config
        if persist_config is not None:
            self._cluster_config.persist_config = persist_config
        if delay_config is not None:
            self._cluster_config.delay_config = delay_config
        if initial_leader is not None:
            self._cluster_config.initial_leader = initial_leader
        if metronome_quorum_size is not None:
            self._cluster_config.metronome_quorum_size = metronome_quorum_size

    def change_client_config(
        self,
        client_id: int,
        end_condition: Optional[EndConditionConfig] = None,
        num_parallel_requests: Optional[int] = None,
        rust_log: Optional[str]=None,
    ):
        if end_condition is not None:
            self._client_configs[client_id].end_condition = end_condition
        if num_parallel_requests is not None:
            self._client_configs[client_id].num_parallel_requests = num_parallel_requests
        if rust_log is not None:
            self._client_configs[client_id].rust_log = rust_log

# Builder class for MetronomeCluster. Used to define and then validate the configs necessary to
# start a MetronomeCluster
class MetronomeClusterBuilder:
    def __init__(self, cluster_name:str, project_id: str = "my-project-1499979282244") -> None:
        self.cluster_name = cluster_name
        self._project_id = project_id
        self._service_account = f"deployment@{project_id}.iam.gserviceaccount.com"
        self._server_configs: dict[int, MetronomeCluster.ServerConfig] = {}
        self._client_configs: dict[int, MetronomeCluster.ClientConfig] = {}
        # Cluster-wide settings
        self._metronome_config: str = "Off"
        self._persist_config: MetronomeCluster.PersistConfig = MetronomeCluster.PersistConfig.NoPersist()
        self._delay_config: MetronomeCluster.DelayConfig = MetronomeCluster.DelayConfig.Sleep(0)
        self._metronome_quorum_size: Optional[int] = None
        self._flexible_quorum: Optional[tuple[int, int]] = None
        self._initial_leader: Optional[int] = None
        self._gcloud_ssh_user = GcpCluster.get_oslogin_username()

    def add_server(
        self,
        server_id: int,
        zone: str,
        machine_type: str = "e2-standard-8",
        instrumentation: bool=False,
        rust_log: str="info"
    ):
        assert server_id > 0
        assert server_id not in self._server_configs.keys(), f"Server {server_id} already exists"
        assert rust_log in ["error", "debug", "trace", "info", "warn"]
        instance_config = InstanceConfig(
            f"{self.cluster_name}-server-{server_id}",
            zone,
            machine_type,
            server_startup_script(self._gcloud_ssh_user),
            firewall_tag="omnipaxos-server",
            dns_name=f"{self.cluster_name}-server-{server_id}",
            service_account=self._service_account,
        )
        server_config = MetronomeCluster.ServerConfig(
            instance_config,
            server_id,
            instrumentation=instrumentation,
            debug_filename=f"server-{server_id}.csv",
            rust_log=rust_log,
        )
        self._server_configs[server_id] = server_config
        return self

    def add_client(
        self,
        server_id: int,
        zone: str,
        end_condition: MetronomeCluster.EndConditionConfig,
        num_parallel_requests: int,
        machine_type: str = "e2-standard-4",
        rust_log: str="info"
    ):
        assert server_id > 0
        assert server_id not in self._client_configs.keys(), f"Client {server_id} already exists"
        assert rust_log in ["error", "debug", "trace", "info", "warn"]
        instance_config = InstanceConfig(
            f"{self.cluster_name}-client-{server_id}",
            zone,
            machine_type,
            client_startup_script(self._gcloud_ssh_user),
            service_account=self._service_account,
        )
        client_config = MetronomeCluster.ClientConfig(
            instance_config=instance_config,
            server_id=server_id,
            end_condition=end_condition,
            num_parallel_requests=num_parallel_requests,
            summary_filename=f"client-{server_id}.json",
            output_filename=f"client-{server_id}.csv",
            rust_log=rust_log,
        )
        self._client_configs[server_id] = client_config
        return self

    def metronome_config(self, metronome_config: str):
        assert metronome_config in ["Off", "RoundRobin", "RoundRobin2", "FastestFollower"]
        self._metronome_config = metronome_config
        return self

    def persist_config(self, persist_config: MetronomeCluster.PersistConfig):
        if persist_config.persist_value:
            assert persist_config.persist_value > 0
        self._persist_config = persist_config
        return self

    def delay_config(self, delay_config: MetronomeCluster.DelayConfig):
        assert delay_config.delay_value >= 0
        self._delay_config = delay_config
        return self

    def initial_leader(self, initial_leader: int):
        self._initial_leader = initial_leader
        return self

    def metronome_quorum_size(self, metronome_quorum_size: int):
        assert metronome_quorum_size > 0
        self._metronome_quorum_size = metronome_quorum_size
        return self

    def build(self) -> MetronomeCluster:
        # TODO: Validate that config won't cause deadlock due to parallel requests not reaching server batch io size
        # Validate config
        for client_id in self._client_configs.keys():
            assert client_id in self._server_configs.keys(), f"Client {client_id} has no server to connect to"
        assert self._initial_leader in self._server_configs.keys()
        if self._metronome_quorum_size is not None:
            majority = len(self._server_configs) // 2 + 1
            assert self._metronome_quorum_size >= majority, f"Metronome quorum is {self._metronome_quorum_size} but it can't be smaller than majority which is {majority}"
        cluster_config = MetronomeCluster.ClusterConfig(
            cluster_name=self.cluster_name,
            nodes=sorted(self._server_configs.keys()),
            metronome_config=self._metronome_config,
            persist_config=self._persist_config,
            delay_config=self._delay_config,
            initial_leader=self._initial_leader,
            metronome_quorum_size=self._metronome_quorum_size,
        )
        return MetronomeCluster(self._project_id, cluster_config, self._server_configs, self._client_configs)

# Startup script executed on creation of the GCP instance for a Metronome server.
# ssh into instance and run `sudo journalctl -u google-startup-scripts.service`
# to debug startup script
def server_startup_script(user: str) -> str:
    container_image_location = "my-project-1499979282244/metronome_server"
    return f"""#! /bin/bash
# Ensure OS login user is setup
useradd -m {user}
mkdir -p /home/{user}
chown {user}:{user} /home/{user}

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
    pull_image: bool=False,
) -> str:
    container_name = "server"
    container_image_location = "my-project-1499979282244/metronome_server"
    instance_config_location = "~/server-config.toml"
    container_config_location = f"/home/$(whoami)/server-config.toml"
    instance_output_dir = "./results"
    container_output_dir = "/app"
    instance_err_location = f"{instance_output_dir}/xerr-server-{config.server_id}.log"
    server_config = _generate_server_config(cluster_config, config)

    # pull_command = f"docker pull gcr.io/{container_image_location} > /dev/null"
    # kill_prev_container_command = f"docker kill {container_name} > /dev/null 2>&1"
    pull_command = f"docker pull gcr.io/{container_image_location}"
    kill_prev_container_command = f"docker kill {container_name}"
    gen_config_command = f"mkdir -p {instance_output_dir} && echo -e '{server_config}' > {instance_config_location}"
    docker_command = f"""docker run \\
        --name {container_name} \\
        -p 800{config.server_id}:800{config.server_id} \\
        --env RUST_LOG={config.rust_log} \\
        --env CONFIG_FILE="{container_config_location}" \\
        -v {instance_config_location}:{container_config_location} \\
        -v {instance_output_dir}:{container_output_dir} \\
        --rm \\
        "gcr.io/{container_image_location}" \\
        2> {instance_err_location}"""
    if pull_image:
        full_command = f"{kill_prev_container_command}; {pull_command}; {gen_config_command} && {docker_command}"
    else:
        # Add a sleep to help avoid connecting to any currently shutting down servers.
        full_command = f"{kill_prev_container_command}; sleep 1; {gen_config_command} && {docker_command}"
    return full_command

def _generate_server_config(
    cluster_config: MetronomeCluster.ClusterConfig,
    config: MetronomeCluster.ServerConfig,
) -> str:
    server_toml = MetronomeCluster.MetronomeServerToml(
        location=config.instance_config.zone,
        server_id=config.server_id,
        instrumentation=config.instrumentation,
        debug_filename=config.debug_filename,
        cluster_name=cluster_config.cluster_name,
        nodes=cluster_config.nodes,
        metronome_config=cluster_config.metronome_config,
        persist_config=cluster_config.persist_config,
        delay_config=cluster_config.delay_config,
        initial_leader=cluster_config.initial_leader,
        metronome_quorum_size=cluster_config.metronome_quorum_size,
    )
    server_toml_str = toml.dumps(asdict(server_toml))
    return server_toml_str
#     persist_value_toml = f"persist_value = {config.persist_config.persist_value}" if config.persist_config.persist_value is not None else ""
#     use_metronome_toml = f"use_metronome = {cluster_config.metronome_config}" if cluster_config.metronome_config is not None else ""
#     metronome_quorum_toml = f"metronome_quorum_size = {cluster_config.metronome_quorum_size}" if cluster_config.metronome_quorum_size is not None else ""
#     flex_quorum_toml = f"flexible_quorum = {{ read_quorum_size = {cluster_config.flexible_quorum[0]}, write_quorum_size = {cluster_config.flexible_quorum[1]} }}" if cluster_config.flexible_quorum is not None else ""
#     init_leader_toml = f"initial_leader = {cluster_config.initial_leader}" if cluster_config.initial_leader is not None else ""
#
#     toml = f"""
# cluster_name = "{cluster_config.cluster_name}"
# location = "{config.instance_config.zone}"
# instrumentation = {str(config.instrumentation).lower()}
# debug_filepath = "{config.debug_filename}"
# {init_leader_toml}
#
# [persist_config]
# persist_type = "{config.persist_config.persist_type}"
# {persist_value_toml}
#
# [delay_config]
# delay_type = "{config.delay_config.delay_type}"
# delay_value = {config.delay_config.delay_value}
#
# [cluster_config]
# configuration_id = 1
# nodes = {cluster_config.nodes}
# {use_metronome_toml}
# {metronome_quorum_toml}
# {flex_quorum_toml}
#
# [server_config]
# pid = {config.server_id}
# election_tick_timeout = 1
# resend_message_tick_timeout = 5
# flush_batch_tick_timeout = 200
# batch_size = 1
# """
#     return toml

# Startup script executed on creation of the GCP instance for a Metronome client.
# ssh into instance and run `sudo journalctl -u google-startup-scripts.service`
# to debug startup script
def client_startup_script(user: str) -> str:
    container_image_location = "my-project-1499979282244/metronome_client"
    return f"""#! /bin/bash
# Ensure OS login user is setup
useradd -m {user}
mkdir -p /home/{user}
chown {user}:{user} /home/{user}

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
    pull_image: bool=False,
) -> str:
    container_name = "client"
    container_image_location = "my-project-1499979282244/metronome_client"
    instance_config_location = "~/client-config.toml"
    container_config_location = f"/home/$(whoami)/client-config.toml"
    instance_output_dir = "./results"
    container_output_dir = "/app"
    client_config = _generate_client_config(cluster_config, config)

    # pull_command = f"docker pull gcr.io/{container_image_location} > /dev/null"
    # kill_prev_container_command = f"docker kill {container_name} > /dev/null 2>&1"
    pull_command = f"docker pull gcr.io/{container_image_location}"
    kill_prev_container_command = f"docker kill {container_name}"
    gen_config_command = f"echo -e '{client_config}' > {instance_config_location}"
    docker_command = f"""docker run \\
    --name={container_name} \\
    --rm \\
    --env RUST_LOG={config.rust_log} \\
    --env CONFIG_FILE={container_config_location} \\
    -v {instance_config_location}:{container_config_location} \\
    -v {instance_output_dir}:{container_output_dir} \\
    gcr.io/{container_image_location}"""
    full_command = f"{kill_prev_container_command}; sleep 2; {gen_config_command} && {docker_command}"
    if pull_image:
        full_command = f"{kill_prev_container_command}; {pull_command}; {gen_config_command} && {docker_command}"
    else:
        # Add a sleep to help avoid connecting to any currently shutting down servers.
        full_command = f"{kill_prev_container_command}; sleep 1; {gen_config_command} && {docker_command}"
    return full_command

def _generate_client_config(
    cluster_config: MetronomeCluster.ClusterConfig,
    config: MetronomeCluster.ClientConfig,
) -> str:
    client_toml = MetronomeCluster.MetronomeClientToml(
        cluster_name=cluster_config.cluster_name,
        location=config.instance_config.zone,
        server_id=config.server_id,
        end_condition=config.end_condition,
        num_parallel_requests=config.num_parallel_requests,
        summary_filename=config.summary_filename,
        output_filename=config.output_filename,
    )
    client_toml_str = toml.dumps(asdict(client_toml))
    return client_toml_str
#     toml = f"""
# cluster_name = "{cluster_config.cluster_name}"
# location = "{config.instance_config.zone}"
# server_id = {config.client_id}
# num_parallel_requests = {config.num_parallel_requests}
# summary_filepath = "{config.summary_filename}"
# output_filepath = "{config.output_filename}"
#
# [end_condition]
# end_condition_type = "{config.end_condition.end_condition_type}"
# end_condition_value = {config.end_condition.end_condition_value}
# """
#     return toml
