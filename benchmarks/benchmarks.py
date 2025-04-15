from itertools import product
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
            is_straggler = i == straggler
            cluster = cluster.add_server(
                i,
                "us-central1-a",
                instrumentation=instrumentation,
                straggler=is_straggler,
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
        worksteal_ms: int | None = None,
        send_disable: int | None = None,
        disable_commands: list[str] | None = None,
        shutdown_cluster: bool = False,
    ):
        print("RUNNING CLOSED LOOP EXPERIMENT:")
        # Generate all valid combinations of run arguments
        for combination in product(data_sizes, number_of_clients, metronome_configs):
            data_size, num_clients, config = combination
            if config == "Off":
                disable_command = "Off"
                self._run_iteration(
                    data_size=data_size,
                    num_clients=num_clients,
                    metronome_config=config,
                    metronome_quorum_size=None,
                    send_disable=send_disable,
                    disable_command="Off",
                    worksteal_ms=worksteal_ms,
                )
            else:
                for metronome_quorum in metronome_quorum_sizes or [None]:
                    for disable_command in disable_commands or [None]:
                        self._run_iteration(
                            data_size=data_size,
                            num_clients=num_clients,
                            metronome_config=config,
                            metronome_quorum_size=metronome_quorum,
                            send_disable=send_disable,
                            disable_command=disable_command,
                            worksteal_ms=worksteal_ms,
                        )
        if shutdown_cluster:
            self.cluster.shutdown()

    def _run_iteration(
        self,
        data_size: int,
        num_clients: int,
        metronome_config: str,
        metronome_quorum_size: int | None,
        send_disable: int | None,
        disable_command: str | None,
        worksteal_ms: int | None,
    ):
        iteration_dir = f"{data_size=}-{num_clients=}-metronome={metronome_config}"
        if metronome_quorum_size is not None:
            iteration_dir += f"-{metronome_quorum_size=}"
        if disable_command is not None:
            iteration_dir += f"-{disable_command=}"
        print("ITERATION:", iteration_dir)
        # Update server and client configs
        request_mode_config = RequestModeConfig.ClosedLoop(num_clients)
        self.cluster.change_client_config(
            1,
            request_mode_config=request_mode_config,
            send_disable_config=send_disable,
            send_disable_command=disable_command,
        )
        persist_config = (
            PersistConfig.File(data_size)
            if data_size != 0
            else PersistConfig.NoPersist()
        )
        if metronome_config == "RoundRobin2WorkSteal":
            metronome_config = "RoundRobin2"
        else:
            worksteal_ms = None
        self.cluster.change_cluster_config(
            persist_config=persist_config,
            metronome_config=metronome_config,
            worksteal_ms=worksteal_ms,
            metronome_quorum_size=metronome_quorum_size,
        )
        # Run cluster
        self.cluster.run(self.experiment_dir / iteration_dir, pull_images=False)


def straggler_experiment():
    experiment_dir = Path(__file__).parent / "logs" / "no-straggler-experiment"
    experiment = ClosedLoopExperiment(
        base_experiment_dir=experiment_dir,
        cluster_size=5,
        batch_config=BatchConfig.Opportunistic(),
        end_condition=EndConditionConfig.SecondsPassed(60),
        straggler=None,
        instrumentation=False,
        summary_only=False,
    )
    experiment.run(
        data_sizes=[1024],
        number_of_clients=[2000],
        worksteal_ms=10,
        metronome_configs=["Off", "RoundRobin2", "RoundRobin2WorkSteal"],
    )


def new_straggler_experiment():
    experiment_dir = Path(__file__).parent / "logs" / "new-straggler-experiment"
    experiment = ClosedLoopExperiment(
        base_experiment_dir=experiment_dir,
        cluster_size=5,
        batch_config=BatchConfig.Opportunistic(),
        end_condition=EndConditionConfig.SecondsPassed(60),
        straggler=None,
        instrumentation=False,
        summary_only=False,
    )
    experiment.run(
        data_sizes=[1024],
        number_of_clients=[2000],
        send_disable=30000,
        disable_commands=["Off"],
        # disable_commands=["Off", "BiggerQuorum"], # TODO: BiggerQuorum is broken
        metronome_configs=["Off", "RoundRobin2"],
    )
    # Metronome quorum size run
    experiment = ClosedLoopExperiment(
        base_experiment_dir=experiment_dir,
        cluster_size=5,
        batch_config=BatchConfig.Opportunistic(),
        end_condition=EndConditionConfig.SecondsPassed(60),
        straggler=None,
        instrumentation=False,
        summary_only=False,
    )
    experiment.run(
        data_sizes=[1024],
        number_of_clients=[2000],
        metronome_configs=["RoundRobin2"],
        metronome_quorum_sizes=[4],
    )


def metronome_size_experiment():
    experiment_dir = Path(__file__).parent / "logs" / "metronome-size-experiment"
    for cluster_size in [7, 5, 3]:
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


# Run experiments here
def main():
    # straggler_experiment()
    # new_straggler_experiment()
    # metronome_size_experiment()
    # increasing_clients_experiment()
    pass


if __name__ == "__main__":
    main()
