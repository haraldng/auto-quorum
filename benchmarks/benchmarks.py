from pathlib import Path
from metronome_cluster import MetronomeCluster, MetronomeClusterBuilder
import time


def closed_loop_experiment(cluster_size: int, number_of_clients: int, batch_config: MetronomeCluster.BatchConfig, end_condition: MetronomeCluster.EndConditionConfig):
    experiment_log_dir = Path(f"./logs/debug-tests/closed-loop-experiments-{batch_config.to_label()}/{cluster_size}-node-cluster-{number_of_clients}-clients")
    print(f"RUNNING CLOSED LOOP EXPERIMENT: {cluster_size=}, {end_condition=}, {number_of_clients=}")
    print(experiment_log_dir)

    # Create cluster instances
    cluster_name = f"cluster-{cluster_size}-1"
    cluster = MetronomeClusterBuilder(cluster_name).initial_leader(1).batch_config(batch_config)
    for i in range(1, cluster_size+1):
        cluster = cluster.add_server(
            i,
            "us-east1-b",
            instrumentation=False,
            rust_log="info",
        )
    cluster = cluster.add_client(1,
        "us-east1-b",
        request_mode_config=MetronomeCluster.RequestModeConfig.ClosedLoop(number_of_clients),
        end_condition=end_condition,
        rust_log="info",
        summary_only=True,
    ).build()

    # Run experiments
    # for data_size in [256, 1024, 1024*4, 1024*16]:
    for data_size in [1024]:
        persist_config = MetronomeCluster.PersistConfig.File(data_size)
        cluster.change_cluster_config(persist_config=persist_config)
        # for metronome_config in ["Off", "RoundRobin2", "FastestFollower"]:
        for metronome_config in ["RoundRobin2"]:
            print(f"{metronome_config=}, {data_size=}")
            cluster.change_cluster_config(metronome_config=metronome_config)
            cluster.start_servers(pull_images=True)
            cluster.start_client(1, pull_image=True)
            cluster.await_cluster()
            cluster.stop_servers()
            iteration_directory = Path.joinpath(experiment_log_dir, f"metronome-{metronome_config}-datasize-{data_size}")
            time.sleep(2)
            cluster.get_logs(iteration_directory)
    # cluster.shutdown()

def num_clients_latency_experiment(cluster_size: int, batch_config: MetronomeCluster.BatchConfig, end_condition: MetronomeCluster.EndConditionConfig, metronome_quorum_size: int | None=None):
    metronome_quorum_size_label = f"-met-quorum-{metronome_quorum_size}" if metronome_quorum_size else ""
    experiment_log_dir = Path(f"./logs/closed-loop-latency-experiments/{batch_config.to_label()}/{cluster_size}-node-cluster{metronome_quorum_size_label}")
    print(f"RUNNING NUM CLIENTS-LATENCY EXPERIMENT: {cluster_size=}, {end_condition=}, {batch_config=}")
    print(experiment_log_dir)

    # Create cluster instances
    cluster_name = f"cluster-{cluster_size}-1"
    cluster = MetronomeClusterBuilder(cluster_name).initial_leader(1).batch_config(batch_config)
    if metronome_quorum_size:
        cluster = cluster.metronome_quorum_size(metronome_quorum_size)
    for i in range(1, cluster_size+1):
        cluster = cluster.add_server(
            i,
            "us-central1-a",
            instrumentation=False,
            rust_log="error",
        )
    cluster = cluster.add_client(1,
        "us-central1-a",
        request_mode_config=MetronomeCluster.RequestModeConfig.ClosedLoop(1),
        end_condition=end_condition,
        rust_log="info",
    ).build()

    # Run experiments
    for data_size in [256, 1024]:
        if data_size == 0:
            persist_config = MetronomeCluster.PersistConfig.NoPersist()
        else:
            persist_config = MetronomeCluster.PersistConfig.File(data_size)
        cluster.change_cluster_config(persist_config=persist_config)
        for number_of_clients in [1, 10, 100, 1000, 5000, 10_000, 12_000]:
            request_mode_config=MetronomeCluster.RequestModeConfig.ClosedLoop(number_of_clients)
            cluster.change_client_config(1, request_mode_config=request_mode_config)
            for metronome_config in ["Off", "RoundRobin2"]:
                print(f"{metronome_config=}, {data_size=}, {number_of_clients=}")
                cluster.change_cluster_config(metronome_config=metronome_config)
                cluster.start_servers()
                cluster.start_client(1)
                cluster.await_cluster()
                cluster.stop_servers()
                iteration_directory = Path.joinpath(experiment_log_dir, f"metronome-{metronome_config}-datasize-{data_size}-clients-{number_of_clients}")
                cluster.get_logs(iteration_directory)
                time.sleep(2)
    # cluster.shutdown()

def latency_throughput_experiment(cluster_size: int, end_condition: MetronomeCluster.EndConditionConfig, persist_config: MetronomeCluster.PersistConfig):
    experiment_log_dir = Path(f"./logs/latency-throughput-experiment/{cluster_size}-node-cluster-{persist_config.to_label()}")
    print(f"RUNNING LATENCY-THROUGHPUT EXPERIMENT: {cluster_size=}, {end_condition=}")
    print(experiment_log_dir)

    # Create cluster instances
    cluster_name = f"cluster-{cluster_size}-1"
    batch_config = MetronomeCluster.BatchConfig.Individual()
    cluster = MetronomeClusterBuilder(cluster_name).initial_leader(1).batch_config(batch_config).persist_config(persist_config)
    for i in range(1, cluster_size+1):
        cluster = cluster.add_server(
            i,
            "us-central1-a",
            instrumentation=False,
            rust_log="error",
        )
    cluster = cluster.add_client(1,
        "us-central1-a",
        request_mode_config=MetronomeCluster.RequestModeConfig.ClosedLoop(1),
        end_condition=end_condition,
        rust_log="info",
    ).build()

    # Run experiments
    request_interval_ms = 10
    # for request_batch_size in [1, 10, 100, 1000, 2000, 5000]:
    for request_batch_size in [15000]:
        request_mode_config = MetronomeCluster.RequestModeConfig.OpenLoop(request_interval_ms, request_batch_size)
        cluster.change_client_config(1, request_mode_config=request_mode_config)
        for metronome_config in ["Off", "RoundRobin2"]:
            print(f"{metronome_config=}, {request_mode_config=}")
            cluster.change_cluster_config(metronome_config=metronome_config)
            cluster.start_servers(pull_images=True)
            cluster.start_client(1, pull_image=True)
            cluster.await_cluster()
            cluster.stop_servers()
            iteration_directory = Path.joinpath(experiment_log_dir, f"metronome-{metronome_config}-{request_mode_config.to_label()}")
            cluster.get_logs(iteration_directory)

# def metronome_size_experiment(cluster_size: int, total_messages: int, number_of_clients: int):
#     experiment_log_dir = Path(f"./logs/metronome-size-experiments/{cluster_size}-node-cluster")
#     majority = cluster_size // 2 + 1
#     metronome_sizes = list(range(majority, cluster_size))
#     # critical_lengths = map(lambda x: math.comb(cluster_size, x), metronome_sizes)
#     # batch_size = math.lcm(*critical_lengths)
#     print(f"RUNNING METRONOME SIZE EXPERMIENT: {cluster_size=}, {metronome_sizes=}, {total_messages=}, {number_of_clients=}")
#
#     cluster_name = f"cluster-{cluster_size}-1"
#     cluster = MetronomeClusterBuilder(cluster_name).initial_leader(1).use_metronome(0)
#     for i in range(1, cluster_size+1):
#         cluster = cluster.add_server(
#             i,
#             "us-central1-a",
#             batch_config=MetronomeCluster.BatchConfig.Individual(),
#             persist_config=MetronomeCluster.PersistConfig.File(4096),
#             rust_log="warn",
#         )
#     cluster = cluster \
#         .add_client(1,
#             "us-central1-a",
#             total_requests=total_messages,
#             num_parallel_requests=number_of_clients,
#         ).build()
#     print("Waiting for instances to be ssh-able...")
#     time.sleep(30)
#
#     for metronome_quorum_size in metronome_sizes:
#         for use_metronome in [0, 2]:
#             print(f"use_metronome = {use_metronome}, metronome_quorum_size = {metronome_quorum_size}")
#             # write_quorum = min(metronome_quorum_size, majority)
#             # read_quorum = cluster_size - write_quorum + 1
#             # flex_quorum = (read_quorum, write_quorum)
#             cluster.change_cluster_config(use_metronome=use_metronome, metronome_quorum_size=metronome_quorum_size)
#             cluster.start_servers()
#             start_process = cluster.start_client(1)
#             start_process.wait(timeout=60*10)
#             experiment_log_prefix = f"metronome-{use_metronome}-quorum-size-{metronome_quorum_size}"
#             cluster.get_logs(experiment_log_dir, experiment_log_prefix)
#     cluster.shutdown()


def main():
    # batch_config = MetronomeCluster.BatchConfig.Every(100)
    # end_condition = MetronomeCluster.EndConditionConfig.SecondsPassed(2*60)
    # closed_loop_experiment(cluster_size=5, number_of_clients=1000, batch_config=batch_config, end_condition=end_condition)

    batch_config = MetronomeCluster.BatchConfig.Opportunistic()
    end_condition = MetronomeCluster.EndConditionConfig.SecondsPassed(5*60)
    closed_loop_experiment(cluster_size=5, number_of_clients=10_000, batch_config=batch_config, end_condition=end_condition)

    # batch_config = MetronomeCluster.BatchConfig.Opportunistic()
    # end_condition = MetronomeCluster.EndConditionConfig.SecondsPassed(5)
    # num_clients_latency_experiment(cluster_size=3, batch_config=batch_config, end_condition=end_condition)

    # persist_config = MetronomeCluster.PersistConfig.File(0)
    # end_condition = MetronomeCluster.EndConditionConfig.SecondsPassed(2)
    # latency_throughput_experiment(cluster_size=5, persist_config=persist_config, end_condition=end_condition)

    # persist_config = MetronomeCluster.PersistConfig.File(256)
    # end_condition = MetronomeCluster.EndConditionConfig.SecondsPassed(2)
    # latency_throughput_experiment(cluster_size=5, persist_config=persist_config, end_condition=end_condition)



    # metronome_size_experiment(7, 1000, 1, batch_config)
    pass

if __name__ == "__main__":
    main()

