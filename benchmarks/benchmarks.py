from pathlib import Path

from metronome_cluster import MetronomeCluster, MetronomeClusterBuilder
from metronome_configs import (
    BatchConfig,
    EndConditionConfig,
    PersistConfig,
    RequestModeConfig,
)


class ClosedLoopExperiment:
    cluster: MetronomeCluster
    experiment_dir: Path

    def __init__(
        self,
        base_experiment_dir: Path,
        cluster_size: int,
        batch_config: BatchConfig,
        end_condition: EndConditionConfig,
        straggler: int | None = None,
        instrumentation: bool = False,
        summary_only: bool = False,
    ) -> None:
        self.cluster = self._create_cluster(
            cluster_size,
            batch_config,
            end_condition,
            straggler,
            instrumentation,
            summary_only,
        )
        self.experiment_dir = self._create_experiment_dir(
            base_experiment_dir,
            cluster_size,
            batch_config,
            straggler,
        )

    @staticmethod
    def _create_cluster(
        cluster_size: int,
        batch_config: BatchConfig,
        end_condition: EndConditionConfig,
        straggler: int | None,
        instrumentation: bool,
        summary_only: bool,
    ) -> MetronomeCluster:
        dummy_persist_config = PersistConfig.NoPersist()
        dummy_request_config = RequestModeConfig.ClosedLoop(1)
        cluster = (
            MetronomeClusterBuilder(1)
            .initial_leader(1)
            .batch_config(batch_config)
            .persist_config(dummy_persist_config)
        )
        for i in range(1, cluster_size + 1):
            machine_type = "e2-standard-2" if i == straggler else "e2-standard-8"
            cluster = cluster.add_server(
                i,
                "us-central1-a",
                instrumentation=instrumentation,
                rust_log="info",
                machine_type=machine_type,
            )
        cluster = cluster.add_client(
            1,
            "us-central1-a",
            request_mode_config=dummy_request_config,
            end_condition=end_condition,
            rust_log="info",
            summary_only=summary_only,
        ).build()
        return cluster

    @staticmethod
    def _create_experiment_dir(
        base_experiment_dir: Path,
        cluster_size: int,
        batch_config: BatchConfig,
        straggler: int | None,
    ) -> Path:
        cluster_dir = (
            f"{cluster_size}-node-cluster"
            if straggler is None
            else f"{cluster_size}-node-cluster-{straggler}-straggler"
        )
        return base_experiment_dir / batch_config.to_label() / cluster_dir

    def run(
        self,
        data_sizes: list[int],
        number_of_clients: list[int],
        metronome_configs: list[str],
        metronome_quorum_sizes: list[int] | None = None,
        shutdown_cluster: bool = False,
    ):
        print("RUNNING CLOSED LOOP EXPERIMENT:", metronome_quorum_sizes)
        for data_size in data_sizes:
            for num_clients in number_of_clients:
                for metronome_config in metronome_configs:
                    if metronome_config == "Off" or metronome_quorum_sizes is None:
                        self._run_iteration(
                            data_size,
                            num_clients,
                            metronome_config,
                            None,
                        )
                    else:
                        for metronome_quorum in metronome_quorum_sizes:
                            self._run_iteration(
                                data_size,
                                num_clients,
                                metronome_config,
                                metronome_quorum,
                            )
        if shutdown_cluster:
            self.cluster.shutdown()

    def _run_iteration(
        self,
        data_size: int,
        num_clients: int,
        metronome_config: str,
        metronome_quorum_size: int | None,
    ):
        iteration_dir = f"{data_size=}-{num_clients=}-metronome={metronome_config}"
        if metronome_quorum_size is not None:
            iteration_dir += f"-{metronome_quorum_size=}"
        print("ITERATION:", iteration_dir)
        # Update server and client configs
        request_mode_config = RequestModeConfig.ClosedLoop(num_clients)
        self.cluster.change_client_config(1, request_mode_config=request_mode_config)
        persist_config = (
            PersistConfig.File(data_size)
            if data_size != 0
            else PersistConfig.NoPersist()
        )
        if metronome_config == "RoundRobin2WorkSteal":
            metronome_config = "RoundRobin2"
            worksteal_flag = True
        else:
            worksteal_flag = False
        self.cluster.change_cluster_config(
            persist_config=persist_config,
            metronome_config=metronome_config,
            worksteal_flag=worksteal_flag,
            metronome_quorum_size=metronome_quorum_size,
        )
        # Run cluster
        self.cluster.run(self.experiment_dir / iteration_dir)


# def num_clients_latency_experiment(
#     num_runs: int,
#     cluster_size: int,
#     batch_config: BatchConfig,
#     end_condition: EndConditionConfig,
#     metronome_quorum_size: int | None = None,
# ):
#     metronome_quorum_size_label = (
#         f"-met-quorum-{metronome_quorum_size}" if metronome_quorum_size else ""
#     )
#     experiment_log_dir = Path(
#         f"./logs/num-clients-latency-experiments-HHD2/{batch_config.to_label()}/{cluster_size}-node-cluster{metronome_quorum_size_label}"
#     )
#     print(
#         f"RUNNING NUM CLIENTS-LATENCY EXPERIMENT: {cluster_size=}, {end_condition=}, {batch_config=}"
#     )
#     print(experiment_log_dir)
#
#     # Create cluster instances
#     dummy_persist_config = PersistConfig.NoPersist()
#     cluster = (
#         MetronomeClusterBuilder(1)
#         .initial_leader(1)
#         .batch_config(batch_config)
#         .persist_config(dummy_persist_config)
#     )
#     if metronome_quorum_size:
#         cluster = cluster.metronome_quorum_size(metronome_quorum_size)
#     for i in range(1, cluster_size + 1):
#         cluster = cluster.add_server(
#             i,
#             "us-east1-b",
#             instrumentation=False,
#             rust_log="error",
#         )
#     cluster = cluster.add_client(
#         1,
#         "us-east1-b",
#         request_mode_config=RequestModeConfig.ClosedLoop(1),
#         end_condition=end_condition,
#         rust_log="info",
#         summary_only=True,
#     ).build()
#
#     # Run experiments
#     for run in range(num_runs):
#         for entry_size in [0, 256, 1024]:
#             if entry_size == 0:
#                 persist_config = PersistConfig.NoPersist()
#                 metronome_configs = ["Off"]
#             else:
#                 persist_config = PersistConfig.File(entry_size)
#                 metronome_configs = ["Off", "RoundRobin2"]
#             cluster.change_cluster_config(persist_config=persist_config)
#             if entry_size == 1024:
#                 client_runs = [1, 10, 100, 500, 1000, 1500, 2500]
#             else:
#                 client_runs = [1, 10, 100, 500, 1000, 1500, 2500, 5000, 6000]
#             for number_of_clients in client_runs:
#                 request_mode_config = RequestModeConfig.ClosedLoop(number_of_clients)
#                 cluster.change_client_config(1, request_mode_config=request_mode_config)
#                 for metronome_config in metronome_configs:
#                     print(f"{metronome_config=}, {entry_size=}, {number_of_clients=}")
#                     cluster.change_cluster_config(metronome_config=metronome_config)
#                     iteration_directory = Path.joinpath(
#                         experiment_log_dir,
#                         f"metronome-{metronome_config}-datasize-{entry_size}-clients-{number_of_clients}",
#                     )
#                     run_directory = Path.joinpath(iteration_directory, f"run-{run}")
#                     cluster.run(run_directory)
#     # cluster.shutdown()


def straggler_experiment():
    experiment_dir = Path(__file__).parent / "logs" / "straggler-experiment"
    experiment = ClosedLoopExperiment(
        base_experiment_dir=experiment_dir,
        cluster_size=5,
        batch_config=BatchConfig.Every(100),
        end_condition=EndConditionConfig.SecondsPassed(5),
        straggler=3,
        instrumentation=True,
    )
    experiment.run(
        data_sizes=[256, 1024],
        number_of_clients=[1000],
        metronome_configs=["Off", "RoundRobin2", "RoundRobin2WorkSteal"],
        # metronome_configs=["RoundRobin2WorkSteal"],
    )


def metronome_size_experiment():
    experiment_dir = Path(__file__).parent / "logs" / "metronome-size-experiment"
    # for cluster_size in [7, 5, 3]:
    for cluster_size in [7]:
        majority = cluster_size // 2 + 1
        metronome_sizes = list(range(majority, cluster_size))
        experiment = ClosedLoopExperiment(
            base_experiment_dir=experiment_dir,
            cluster_size=cluster_size,
            batch_config=BatchConfig.Opportunistic(),
            end_condition=EndConditionConfig.SecondsPassed(30),
        )
        experiment.run(
            data_sizes=[1024],
            number_of_clients=[1000],
            # data_sizes=[256],
            # number_of_clients=[4000],
            metronome_configs=["Off", "RoundRobin2"],
            metronome_quorum_sizes=metronome_sizes,
            shutdown_cluster=False,
        )


def increasing_clients_experiment():
    experiment_dir = Path(__file__).parent / "logs" / "increasing-clients-experiment"
    for run in range(3):
        experiment = ClosedLoopExperiment(
            base_experiment_dir=experiment_dir / f"run-{run}",
            cluster_size=5,
            batch_config=BatchConfig.Opportunistic(),
            end_condition=EndConditionConfig.SecondsPassed(5),
            summary_only=True,
        )
        experiment.run(
            data_sizes=[0],
            number_of_clients=[1, 10, 100, 500, 1000, 1500, 2500, 5000, 6000],
            metronome_configs=["Off"],
        )
        experiment.run(
            data_sizes=[256],
            number_of_clients=[1, 10, 100, 500, 1000, 1500, 2500, 5000, 6000],
            metronome_configs=["Off", "RoundRobin2"],
        )
        experiment.run(
            data_sizes=[1024],
            number_of_clients=[1, 10, 100, 500, 1000, 1500, 2500],
            metronome_configs=["Off", "RoundRobin2"],
        )


def main():
    # straggler_experiment()
    metronome_size_experiment()

    # batch_config = BatchConfig.Every(100)
    # end_condition = EndConditionConfig.SecondsPassed(5)
    # closed_loop_experiment(
    #     cluster_size=5,
    #     number_of_clients=1000,
    #     batch_config=batch_config,
    #     end_condition=end_condition,
    # )

    # batch_config = BatchConfig.Opportunistic()
    # end_condition = EndConditionConfig.SecondsPassed(60)
    # closed_loop_experiment(
    #     cluster_size=5,
    #     number_of_clients=1000,
    #     batch_config=batch_config,
    #     end_condition=end_condition,
    # )

    # batch_config = BatchConfig.Opportunistic()
    # end_condition = EndConditionConfig.SecondsPassed(5)
    # num_clients_latency_experiment(
    #     num_runs=1,
    #     cluster_size=5,
    #     batch_config=batch_config,
    #     end_condition=end_condition,
    # )

    # batch_config = BatchConfig.Opportunistic()
    # end_condition = EndConditionConfig.SecondsPassed(60)
    # metronome_size_experiment(cluster_size=7, number_of_clients=1000, batch_config=batch_config, end_condition=end_condition)
    pass


if __name__ == "__main__":
    main()
