import os
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from graphs.base_graphs import (
    graph_experiment_debug,
    parse_client_log,
    parse_clients_summaries,
    parse_server_logs,
)

LOCAL_RUN_DIR: Path = Path(__file__).parent.parent.parent / "build_scripts" / "logs"
LOCAL_SAVE_DIR: Path = Path(__file__).parent.parent.parent / "build_scripts" / "logs"


def local_experiment_data() -> pd.DataFrame:
    experiment_dir = LOCAL_RUN_DIR
    return parse_clients_summaries(experiment_dir)


def save_local_plot(filename: str, format: str = "svg"):
    file_path = LOCAL_SAVE_DIR / filename
    os.makedirs(file_path.parent, exist_ok=True)
    plt.savefig(file_path, format=format)


def local_debug_plots(nrows: int, skiprows: int, save: bool = True):
    summaries = local_experiment_data()
    for i, (_, client_summary) in enumerate(summaries.iterrows()):
        client_log = parse_client_log(client_summary, nrows=nrows, skiprows=skiprows)
        server_logs = parse_server_logs(client_summary, nrows=nrows, skiprows=skiprows)
        graph_experiment_debug(client_summary, client_log, server_logs)
        if save:
            save_filename = f"debug/debug-{i}.png"
            save_local_plot(save_filename, format="png")
        plt.show()
        plt.close()
    return
