import os
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd

from graphs.base_graphs import (
    LOGS_DIR,
    SAVE_DIR,
    parse_client_log,
    parse_clients_summaries,
)

STRAGGLER_DIR: Path = LOGS_DIR / "new-straggler-experiment" / "Opportunistic"
STRAGGLER_SAVE_DIR: Path = SAVE_DIR / "new-straggler-experiment"


def straggler_experiment_data() -> pd.DataFrame:
    experiment_dir = STRAGGLER_DIR / "5-node-cluster"
    summaries = parse_clients_summaries(experiment_dir)
    summaries["metronome_info"] = summaries["metronome_info"].astype(str)
    summaries["file"] = summaries["file"].astype(str)
    mask_ws1 = (summaries["metronome_info"] == "Metronome") & (
        summaries["file"].str.contains("disable_command='Off'")
    )
    mask_ws2 = (summaries["metronome_info"] == "Metronome") & (
        summaries["file"].str.contains("disable_command='BiggerQuorum'")
    )
    mask_metq = (summaries["metronome_info"] == "Metronome") & (
        summaries["metronome_quorum_size"] == 4
    )
    # summaries.loc[mask_ws1, "metronome_info"] = (
    #     summaries.loc[mask_ws1, "metronome_info"] + " WS1"
    # )
    summaries.loc[mask_ws2, "metronome_info"] = (
        summaries.loc[mask_ws2, "metronome_info"] + " WS2"
    )
    summaries.loc[mask_metq, "metronome_info"] = (
        summaries.loc[mask_metq, "metronome_info"] + " Q+1"
    )
    return summaries


def save_straggler_plot(filename: str, format: str = "svg"):
    file_path = STRAGGLER_SAVE_DIR / filename
    os.makedirs(file_path.parent, exist_ok=True)
    plt.savefig(file_path, format=format)


def new_straggler_plot():
    summaries = straggler_experiment_data()
    # Moving average latency lines
    for _, summary in summaries.iterrows():
        label = summary["metronome_info"]
        client_log = parse_client_log(summary)
        client_log["latency"] = (
            client_log["response_time"] - client_log["request_time"]
        ) / 1000
        client_log["time"] = pd.to_datetime(client_log["request_time"], unit="us")
        client_log.set_index("time", inplace=True)
        averaged = client_log["latency"].resample("1000ms").mean()
        # plt.scatter(client_log.index, client_log["latency"], label=label, alpha=0.3) # way too many datapoints to plot
        plt.plot(averaged.index, averaged.values, label=label, linewidth=2)
    # Node failure line
    plt.axvline(
        pd.to_datetime(30_000_000, unit="us"),
        color="red",
        linestyle="--",
        label="Node failure",
    )

    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter("%M:%S"))
    plt.gca().xaxis.set_major_locator(mdates.SecondLocator(interval=5))  # tick every 1s
    plt.xlabel("Experiment Time")
    plt.ylabel("Request Latency (ms)")
    plt.ylim(0, 16)
    plt.yticks(range(0, 17, 2))
    plt.legend()
    plt.grid(False)

    plt.show()
