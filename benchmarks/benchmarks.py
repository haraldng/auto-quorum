from pathlib import Path
from metronome_cluster import MetronomeClusterBuilder
from metronome_configs import EndConditionConfig, PersistConfig, BatchConfig, RequestModeConfig
import time

def closed_loop_experiment(
        cluster_size: int,
        number_of_clients: int,
        batch_config: BatchConfig,
        end_condition: EndConditionConfig
    ):
    experiment_log_dir = Path(f"./logs/closed-loop-experiments-{batch_config.to_label()}/{cluster_size}-node-cluster-{number_of_clients}-clients")
    print(f"RUNNING CLOSED LOOP EXPERIMENT: {cluster_size=}, {end_condition=}, {number_of_clients=}")
    print(experiment_log_dir)

    # Create cluster instances
    cluster_name = f"cluster-{cluster_size}-1"
    dummy_persist_config = PersistConfig.NoPersist()
    cluster = MetronomeClusterBuilder(cluster_name).initial_leader(1).batch_config(batch_config).persist_config(dummy_persist_config)
    for i in range(1, cluster_size+1):
        cluster = cluster.add_server(
            i,
            "us-east1-b",
            instrumentation=False,
            rust_log="info",
        )
    cluster = cluster.add_client(1,
        "us-east1-b",
        request_mode_config=RequestModeConfig.ClosedLoop(number_of_clients),
        end_condition=end_condition,
        rust_log="info",
        summary_only=False,
    ).build()

    # Run experiments
    # for data_size in [256, 1024]:
    for data_size in [1024]:
        persist_config = PersistConfig.File(data_size)
        cluster.change_cluster_config(persist_config=persist_config)
        for metronome_config in ["Off", "RoundRobin2", "FastestFollower"]:
            print(f"{metronome_config=}, {data_size=}")
            cluster.change_cluster_config(metronome_config=metronome_config)
            cluster.start_servers()
            cluster.start_client(1)
            cluster.await_cluster()
            cluster.stop_servers()
            iteration_directory = Path.joinpath(experiment_log_dir, f"metronome-{metronome_config}-datasize-{data_size}")
            time.sleep(2)
            cluster.get_logs(iteration_directory)
    # cluster.shutdown()

def num_clients_latency_experiment(
        num_runs: int,
        cluster_size: int,
        batch_config: BatchConfig,
        end_condition: EndConditionConfig,
        metronome_quorum_size: int | None=None,
    ):
    metronome_quorum_size_label = f"-met-quorum-{metronome_quorum_size}" if metronome_quorum_size else ""
    experiment_log_dir = Path(f"./logs/num-clients-latency-experiments/{batch_config.to_label()}/{cluster_size}-node-cluster{metronome_quorum_size_label}")
    print(f"RUNNING NUM CLIENTS-LATENCY EXPERIMENT: {cluster_size=}, {end_condition=}, {batch_config=}")
    print(experiment_log_dir)

    # Create cluster instances
    cluster_name = f"cluster-{cluster_size}-1"
    dummy_persist_config = PersistConfig.NoPersist()
    cluster = MetronomeClusterBuilder(cluster_name).initial_leader(1).batch_config(batch_config).persist_config(dummy_persist_config)
    if metronome_quorum_size:
        cluster = cluster.metronome_quorum_size(metronome_quorum_size)
    for i in range(1, cluster_size+1):
        cluster = cluster.add_server(
            i,
            "us-east1-b",
            instrumentation=False,
            rust_log="error",
        )
    cluster = cluster.add_client(1,
        "us-east1-b",
        request_mode_config=RequestModeConfig.ClosedLoop(1),
        end_condition=end_condition,
        rust_log="info",
        summary_only=True,
    ).build()

    # Run experiments
    for run in range(num_runs):
        for entry_size in [0, 256, 1024]:
            if entry_size == 0:
                persist_config = PersistConfig.NoPersist()
                metronome_configs = ["Off"]
            else:
                persist_config = PersistConfig.File(entry_size)
                metronome_configs = ["Off", "RoundRobin2"]
            cluster.change_cluster_config(persist_config=persist_config)
            if entry_size == 1024:
                client_runs = [1, 10, 100, 500, 1000, 1500, 2500]
            else:
                client_runs = [1, 10, 100, 500, 1000, 1500, 2500, 5000, 6000]
            for number_of_clients in client_runs:
                request_mode_config=RequestModeConfig.ClosedLoop(number_of_clients)
                cluster.change_client_config(1, request_mode_config=request_mode_config)
                for metronome_config in metronome_configs:
                    print(f"{metronome_config=}, {entry_size=}, {number_of_clients=}")
                    cluster.change_cluster_config(metronome_config=metronome_config)
                    cluster.start_servers()
                    cluster.start_client(1)
                    cluster.await_cluster()
                    cluster.stop_servers()
                    iteration_directory = Path.joinpath(experiment_log_dir, f"metronome-{metronome_config}-datasize-{entry_size}-clients-{number_of_clients}")
                    run_directory = Path.joinpath(iteration_directory, f"run-{run}")
                    time.sleep(2)
                    cluster.get_logs(run_directory)
    # cluster.shutdown()

def metronome_size_experiment(
        cluster_size: int,
        number_of_clients: int,
        batch_config: BatchConfig,
        end_condition: EndConditionConfig
    ):
    experiment_log_dir = Path(f"./logs/metronome-size-experiments/{batch_config.to_label()}/{cluster_size}-node-cluster")
    print(f"RUNNING METRONOME SIZE EXPERMIENT: {cluster_size=}, {number_of_clients=}, {batch_config=}")

    # Create cluster instances
    cluster_name = f"cluster-{cluster_size}-1"
    persist_config = PersistConfig.File(1024)
    cluster = MetronomeClusterBuilder(cluster_name).initial_leader(1).batch_config(batch_config).persist_config(persist_config)
    for i in range(1, cluster_size+1):
        cluster = cluster.add_server(
            i,
            "us-east1-b",
            instrumentation=False,
            rust_log="error",
        )
    cluster = cluster.add_client(1,
        "us-east1-b",
        request_mode_config=RequestModeConfig.ClosedLoop(number_of_clients),
        end_condition=end_condition,
        rust_log="info",
        summary_only=False,
    ).build()

    # Run baseline
    metronome_config = "Off"
    print(f"{metronome_config=}, metronome_quorum_size = None")
    cluster.change_cluster_config(metronome_config=metronome_config)
    cluster.start_servers()
    cluster.start_client(1)
    cluster.await_cluster()
    cluster.stop_servers()
    iteration_directory = Path.joinpath(experiment_log_dir, f"metronome-{metronome_config}")
    time.sleep(2)
    cluster.get_logs(iteration_directory)

    # Run with different metronome quorum sizes
    for metronome_config in ["RoundRobin2"]:
        cluster.change_cluster_config(metronome_config=metronome_config)
        majority = cluster_size // 2 + 1
        metronome_sizes = list(range(majority, cluster_size))
        for metronome_quorum_size in metronome_sizes:
            cluster.change_cluster_config(metronome_quorum_size=metronome_quorum_size)
            print(f"{metronome_config=}, {metronome_quorum_size=}")
            cluster.start_servers()
            cluster.start_client(1)
            cluster.await_cluster()
            cluster.stop_servers()
            iteration_directory = Path.joinpath(experiment_log_dir, f"metronome-{metronome_config}-met-quorum-{metronome_quorum_size}")
            time.sleep(2)
            cluster.get_logs(iteration_directory)
    # cluster.shutdown()

def main():
    batch_config = BatchConfig.Every(100)
    end_condition = EndConditionConfig.SecondsPassed(1)
    closed_loop_experiment(cluster_size=5, number_of_clients=1000, batch_config=batch_config, end_condition=end_condition)

    # batch_config = BatchConfig.Opportunistic()
    # end_condition = EndConditionConfig.SecondsPassed(60)
    # closed_loop_experiment(cluster_size=5, number_of_clients=1000, batch_config=batch_config, end_condition=end_condition)

    # batch_config = BatchConfig.Opportunistic()
    # end_condition = EndConditionConfig.SecondsPassed(60)
    # num_clients_latency_experiment(num_runs=2, cluster_size=5, batch_config=batch_config, end_condition=end_condition)

    # batch_config = BatchConfig.Opportunistic()
    # end_condition = EndConditionConfig.SecondsPassed(60)
    # metronome_size_experiment(cluster_size=7, number_of_clients=1000, batch_config=batch_config, end_condition=end_condition)

if __name__ == "__main__":
    main()

