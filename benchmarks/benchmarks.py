from pathlib import Path
import time
from metronome_cluster import MetronomeCluster, MetronomeClusterBuilder


def closed_loop_experiment(cluster_size: int, number_of_clients: int, end_condition: MetronomeCluster.EndConditionConfig, persist_config: MetronomeCluster.PersistConfig):
    experiment_log_dir = Path(f"./logs/test-closed-loop-experiments-{persist_config.to_label()}/{cluster_size}-node-cluster-{number_of_clients}-clients")
    print(f"RUNNING CLOSED LOOP EXPERIMENT: {cluster_size=}, {end_condition=}, {number_of_clients=}")
    print(experiment_log_dir)

    # Create cluster instances
    cluster_name = f"cluster-{cluster_size}-1"
    cluster = MetronomeClusterBuilder(cluster_name).initial_leader(1).use_metronome(0)
    for i in range(1, cluster_size+1):
        cluster = cluster.add_server(
            i,
            "us-central1-a",
            persist_config=persist_config,
            instrumentation=True,
            rust_log="info",
        )
    cluster = cluster.add_client(1,
        "us-central1-a",
        end_condition=end_condition,
        num_parallel_requests=number_of_clients,
        rust_log="info",
    ).build()

    # Run experiments
    for data_size in [256, 4096, 1048*500]:
        for server_id in range(1, cluster_size + 1):
            delay_config = MetronomeCluster.DelayConfig.File(data_size)
            cluster.change_server_config(server_id, delay_config=delay_config)
        for use_metronome in [0, 2]:
            print(f"{use_metronome=}, {data_size=}")
            cluster.change_cluster_config(use_metronome=use_metronome)
            cluster.start_servers()
            cluster.start_client(1)
            cluster.await_cluster()
            iteration_directory = Path.joinpath(experiment_log_dir, f"metronome-{use_metronome}-datasize-{data_size}")
            cluster.get_logs(iteration_directory)
    cluster.shutdown()

# def closed_loop_experiment_sleep(cluster_size: int, total_messages: int, number_of_clients: int):
#     experiment_log_dir = Path(f"./logs/closed-loop-experiments-sleep-Individual/{cluster_size}-node-cluster-{number_of_clients}-clients")
#     print(f"RUNNING CLOSED LOOP EXPERIMENT SLEEP: {cluster_size=}, {total_messages=}, {number_of_clients=}")
#
#     # Create cluster instances
#     cluster_name = f"cluster-{cluster_size}-1"
#     cluster = MetronomeClusterBuilder(cluster_name).initial_leader(1).use_metronome(0)
#     for i in range(1, cluster_size+1):
#         cluster = cluster.add_server(
#             i,
#             "us-central1-a",
#             persist_config=MetronomeCluster.PersistConfig.Individual(),
#             rust_log="warn",
#         )
#     cluster = cluster.add_client(1,
#         "us-central1-a",
#         total_requests=total_messages,
#         num_parallel_requests=number_of_clients,
#         rust_log="warn",
#     ).build()
#     print("Waiting for instances to be ssh-able...")
#     time.sleep(1)
#
#     # Run experiments
#     for storage_delay in [0, 1000, 2000]:
#         for use_metronome in [0, 2]:
#             print(f"{use_metronome=}, {storage_delay=}")
#             for server_id in range(1, cluster_size + 1):
#                 cluster.change_server_config(server_id, delay_config=MetronomeCluster.DelayConfig.Sleep(storage_delay))
#             cluster.change_cluster_config(use_metronome=use_metronome)
#             cluster.start_servers()
#             cluster.start_client(1)
#             cluster.await_client(1)
#             cluster.await_servers()
#             iteration_directory = Path.joinpath(experiment_log_dir, f"metronome-{use_metronome}-delay-{storage_delay}")
#             cluster.get_logs(iteration_directory)
#     # cluster.shutdown()

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
#             persist_config=MetronomeCluster.PersistConfig.Individual(),
#             delay_config=MetronomeCluster.DelayConfig.File(4096),
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
    persist_config = MetronomeCluster.PersistConfig.Every(10)
    end_condition = MetronomeCluster.EndConditionConfig.ResponsesCollected(100)
    closed_loop_experiment(cluster_size=5, number_of_clients=30, end_condition=end_condition, persist_config=persist_config)
    # closed_loop_experiment(5, 9990, 100, persist_config=persist_config)
    # closed_loop_experiment(5, 10000, 100)
    # closed_loop_experiment_sleep(5, 1000, 10)
    # metronome_size_experiment(7, 1000, 1, persist_config)
    pass

if __name__ == "__main__":
    main()

