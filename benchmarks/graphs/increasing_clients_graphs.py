import os
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
from matplotlib import ticker

from graphs.base_graphs import LOGS_DIR, SAVE_DIR, parse_clients_summaries

INCREASING_CLIENTS_DIR: Path = (
    LOGS_DIR / "increasing-clients-experiment" / "Opportunistic"
)
INCREASING_CLIENTS_SAVE_DIR: Path = SAVE_DIR / "increasing-clients-experiment"


def increasing_clients_experiment_data() -> pd.DataFrame:
    experiment_dir = INCREASING_CLIENTS_DIR / "5-node-cluster"
    return parse_clients_summaries(experiment_dir)


def save_increasing_clients_plot(filename: str, format: str = "svg"):
    file_path = INCREASING_CLIENTS_SAVE_DIR / filename
    os.makedirs(file_path.parent, exist_ok=True)
    plt.savefig(file_path, format=format)


def increasing_clients_graph(save: bool = True):
    summaries = increasing_clients_experiment_data()

    # Set up shared x-axis plot
    fig, axs = plt.subplots(
        3,
        1,
        sharex=True,
        gridspec_kw={"height_ratios": [1, 1, 1]},
        layout="constrained",
    )
    fig.set_size_inches((12, 9))
    axs[2].set_xlabel("Number of Clients", fontsize=14)
    # axs[0].set_xscale("log")  # Log scale for x-axis (if needed)
    colors = {
        "256 B": plt.cm.tab10(0),
        "1 KiB": plt.cm.tab10(1),
        "0 B": plt.cm.tab10(2),
    }
    markers = {"256 B": "o", "1 KiB": "D", "0 B": "s"}
    textures = {"OmniPaxos": "--", "Metronome": "-"}

    # Create line for theoretical max disk throughput but ignore it in the legend
    axs[0].axhline(
        y=150, color="black", linestyle="--", linewidth=2, label="Max disk throughput"
    )

    # Calculate metronome improvement (critical factor)
    cluster_size = summaries.iloc[0].cluster_size
    majority = (cluster_size // 2) + 1
    critical_factor = majority / cluster_size

    # Line plots
    grouped_summaries = (
        summaries.groupby(
            ["persist_label", "num_clients", "metronome_info"], observed=True
        )
        .agg(
            {
                "request_latency_average": ["mean", "std"],
                "throughput": ["mean", "std"],
                "persist_info.value": ["first"],
            }
        )
        .reset_index()
    )
    grouped = grouped_summaries.groupby(
        ["persist_label", "metronome_info"], observed=True
    )
    for (entry_size_label, metronome), group in grouped:
        print(group)
        num_clients = group["num_clients"]
        latency = group["request_latency_average"]["mean"]
        std_dev = group["request_latency_average"]["std"]
        throughput = group["throughput"]["mean"]
        entry_size = group["persist_info.value"]["first"]
        disk_throughput = (throughput * entry_size) / 1_000_000
        if metronome != "OmniPaxos":
            disk_throughput *= critical_factor
        if entry_size_label == "0 B":
            label = "In-memory"
        else:
            label = f"{entry_size_label}, {metronome}"
        color = colors[entry_size_label]
        marker = markers[entry_size_label]
        linestyle = textures[metronome]
        y_offset = -12 if entry_size_label == "256 B" else 9

        axs[2].errorbar(
            num_clients,
            latency,
            std_dev,
            label=label,
            marker=marker,
            linestyle=linestyle,
            color=color,
            elinewidth=1,
            capsize=2,
            capthick=1,
        )
        # for x, y in zip(num_clients, latency):
        #     axs[2].annotate(
        #         f"{y:.2f}",  # Format the value as needed
        #         (x, y),  # Position at the marker
        #         textcoords="offset points",  # Offset the text
        #         xytext=(0, y_offset),  # Offset by 5 points in the y-direction
        #         ha="center",  # Center-align the text
        #         fontsize=8,  # Adjust text size as needed
        #         color=color,
        #     )
        axs[1].plot(
            num_clients,
            throughput,
            label=label,
            marker=marker,
            linestyle=linestyle,
            color=color,
        )
        # for x, y in zip(num_clients, throughput):
        #     axs[1].annotate(
        #         f"{y:.2f}",  # Format the value as needed
        #         (x, y),  # Position at the marker
        #         textcoords="offset points",  # Offset the text
        #         xytext=(0, y_offset),  # Offset by 5 points in the y-direction
        #         ha="center",  # Center-align the text
        #         fontsize=8,  # Adjust text size as needed
        #         color=color,
        #     )
        axs[0].plot(
            num_clients,
            disk_throughput,
            label=label,
            marker=marker,
            linestyle=linestyle,
            color=color,
        )
        # for x, y in zip(num_clients, disk_throughput):
        #     axs[0].annotate(
        #         f"{y:.2f}",  # Format the value as needed
        #         (x, y),  # Position at the marker
        #         textcoords="offset points",  # Offset the text
        #         xytext=(0, y_offset),  # Offset by 5 points in the y-direction
        #         ha="center",  # Center-align the text
        #         fontsize=8,  # Adjust text size as needed
        #         color=color,
        #     )
        axs[2].fill_between(
            num_clients, latency - std_dev, latency + std_dev, alpha=0.2, color=color
        )
    axs[0].legend(
        bbox_to_anchor=(0.5, 1.10),
        loc="center",
        ncol=len(summaries.persist_label.unique()),
        fontsize=10,
        frameon=False,
    )

    def format_ticks(x, pos):
        return f"{x/1000:.0f}k"  # Divide by 1000 and append 'k'

    axs[1].yaxis.set_major_formatter(ticker.FuncFormatter(format_ticks))
    # axs[0].legend(title="Configurations", bbox_to_anchor=(1.01, 1), loc='upper left')  # Legend outside plot to right
    axs[2].set_ylabel("Latency (ms)", fontsize=14)
    axs[1].set_ylabel("Requests per sec", fontsize=14)
    axs[0].set_ylabel("Disk thru-put (MiB/sec)", fontsize=14)
    axs[2].grid(which="both", linestyle="--", linewidth=0.5, axis="x")
    axs[1].grid(which="both", linestyle="--", linewidth=0.5, axis="x")
    axs[0].grid(which="both", linestyle="--", linewidth=0.5, axis="x")
    # fig.tight_layout()  # Adjust layout to fit legend
    if save:
        save_filename = "throughput-latency.svg"
        save_increasing_clients_plot(save_filename)
    plt.show()
    plt.close()
