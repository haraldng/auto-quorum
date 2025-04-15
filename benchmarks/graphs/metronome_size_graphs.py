import os
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from matplotlib.colors import LinearSegmentedColormap

from graphs.base_graphs import (
    LOGS_DIR,
    SAVE_DIR,
    graph_experiment_debug,
    parse_client_log,
    parse_clients_summaries,
    parse_server_logs,
)

METRONOME_SIZE_DIR: Path = LOGS_DIR / "metronome-size-experiment" / "Opportunistic"
METRONOME_SIZE_SAVE_DIR: Path = SAVE_DIR / "metronome-size-experiment"


def metronome_size_experiment_data(
    data_size: int, cluster_size: int | None = None
) -> pd.DataFrame:
    experiment_dir = METRONOME_SIZE_DIR
    if cluster_size is not None:
        experiment_dir = experiment_dir / f"{cluster_size}-node-cluster"
    summaries = parse_clients_summaries(experiment_dir)
    summaries = summaries[summaries["persist_info.value"] == data_size]
    return summaries


def save_metronome_size_plot(filename: str, format: str = "svg"):
    file_path = METRONOME_SIZE_SAVE_DIR / filename
    os.makedirs(file_path.parent, exist_ok=True)
    plt.savefig(file_path, format=format)


def metronome_size_heatmap(data_size: int, relative: bool = False, save: bool = True):
    # Get experiment data
    summaries = metronome_size_experiment_data(data_size)
    summaries = summaries.sort_values(
        by=["cluster_size", "metronome_quorum_size"]
    ).reset_index()

    # Treat OmniPaxos runs as haveing metronome_quorum_size = cluster_size
    mask = summaries["metronome_info"] == "OmniPaxos"
    summaries.loc[mask, "metronome_quorum_size"] = summaries.loc[mask, "cluster_size"]

    # Calculate relative latency gains
    omni_latency = summaries[summaries["metronome_info"] == "OmniPaxos"][
        ["cluster_size", "request_latency_average"]
    ]
    omni_latency = omni_latency.rename(
        columns={"request_latency_average": "omni_latency"}
    )
    summaries = summaries.merge(omni_latency, on="cluster_size", how="left")
    summaries["latency_ratio"] = (
        summaries["request_latency_average"] / summaries["omni_latency"]
    )

    # Adjust x-axis so we have a triangle matrix
    summaries["metronome_quorum_configuration"] = summaries["metronome_quorum_size"] - (
        summaries["cluster_size"] // 2 + 1
    )
    # Heatmap
    metric = "latency_ratio" if relative else "request_latency_average"
    pivot = summaries.pivot(
        index="cluster_size",
        columns="metronome_quorum_configuration",
        values=metric,
    )
    plt.figure(figsize=(10, 8))
    colors = ["#f7a6c7", "#4575b4"]
    custom_cmap = LinearSegmentedColormap.from_list("blue_pink", colors)
    sns.heatmap(pivot, annot=True, fmt=".3f", cmap=custom_cmap)
    plt.title(f"Heatmap of Request Latency Average: data_size={data_size}")
    plt.xlabel("Metronome Quorum Size Above Majority")
    plt.ylabel("Cluster Size")

    if save:
        save_filename = f"heatmap_relative.svg" if relative else f"heatmap.svg"
        save_metronome_size_plot(f"{data_size}/{save_filename}")
    plt.show()
    plt.close()


def metronome_size_bar_chart(data_size: int, cluster_size: int, save: bool = True):
    # Get experiment data
    summaries = metronome_size_experiment_data(data_size, cluster_size)
    summaries = summaries.sort_values(by=["metronome_quorum_size"]).reset_index()
    labels = [
        str(size) if size is not None else "OmniPaxos"
        for size in summaries["metronome_quorum_size"]
    ]
    unique_labels = set()
    bar_labels = [
        info if info not in unique_labels and not unique_labels.add(info) else ""
        for info in summaries["metronome_info"]
    ]
    latencies = summaries["request_latency_average"]
    std_dev = summaries["request_latency_std_dev"]
    colors = ["tab:orange" if info != "OmniPaxos" else "tab:blue" for info in labels]

    # Create bar chart
    plt.figure(figsize=(8, 5))
    rects = plt.bar(
        labels,
        latencies,
        label=bar_labels,
        color=colors,
        yerr=std_dev,
        edgecolor="black",
        linewidth=1.5,
    )
    plt.bar_label(rects, fmt="%.2f", padding=3)
    plt.xlabel("Metronome Quorum Size", fontsize=14)
    plt.ylabel("Average Request Latency (ms)", fontsize=14)
    client_summary = summaries.loc[0]
    title = f"Average Request Latency\n" + ", ".join(
        [
            f"cluster_size={client_summary.cluster_size}",
            f"clients=({client_summary.request_mode},{client_summary.request_mode_value})",
            f"\nbatch_config=({client_summary['batch_info.type']},{client_summary['batch_info.value']})",
            f"persist_config=({client_summary['persist_info.type']}, {client_summary.persist_label})",
        ]
    )
    plt.title(title)
    plt.legend(frameon=False)
    plt.tight_layout()

    if save:
        save_filename = f"request_latency-{cluster_size}-cluster-size.svg"
        save_metronome_size_plot(f"{data_size}/{save_filename}")
    plt.show()
    plt.close()


def metronome_size_violin_plot(
    data_size: int, cluster_size: int, nrows: int, skiprows: int, save: bool = True
):
    # Get experiment data
    summaries = metronome_size_experiment_data(data_size, cluster_size)
    summaries = summaries.sort_values(by=["metronome_quorum_size"]).reset_index()
    labels = [
        str(size) if size is not None else "OmniPaxos"
        for size in summaries["metronome_quorum_size"]
    ]

    # Create violin plot
    client_logs = []
    for i, client_summary in summaries.iterrows():
        client_log = parse_client_log(client_summary, nrows=nrows, skiprows=skiprows)
        client_log.dropna(subset=["response_time"], inplace=True)
        client_log["latency"] = (
            client_log["response_time"] - client_log["request_time"]
        ) / 1000
        client_log["metronome_quorum_size"] = labels[i]
        client_log["metronome_info"] = client_summary.metronome_info
        client_logs.append(client_log)
    violin_data = pd.concat(client_logs, ignore_index=True)
    plt.figure(figsize=(10, 6))
    sns.violinplot(
        data=violin_data,
        x="metronome_quorum_size",
        y="latency",
        hue="metronome_info",
        inner="quart",
        density_norm="width",
    )
    client_summary = summaries.iloc[0]
    title = "Latency Distribution\n" + ", ".join(
        [
            f"cluster_size={client_summary.cluster_size}",
            f"clients=({client_summary.request_mode},{client_summary.request_mode_value})",
            f"batch_config=({client_summary['batch_info.type']},{client_summary['batch_info.value']})",
            f"persist_config=({client_summary['persist_info.type']}, {client_summary.persist_label})",
        ]
    )
    plt.title(title)
    # plt.xticks(rotation=45)
    plt.xlabel("Metronome Quorum Size")
    plt.ylabel("Response Latency (ms)")
    if save:
        save_filename = f"latency_distribution-{cluster_size}-cluster-size.svg"
        save_metronome_size_plot(f"{data_size}/{save_filename}")
    plt.show()
    plt.close()


def metronome_size_debug_plots(
    data_size: int, cluster_size: int, nrows: int, skiprows: int, save: bool = True
):
    summaries = metronome_size_experiment_data(data_size, cluster_size)
    summaries = summaries.sort_values(by=["metronome_quorum_size"]).reset_index()
    for i, (_, client_summary) in enumerate(summaries.iterrows()):
        client_log = parse_client_log(client_summary, nrows=nrows, skiprows=skiprows)
        server_logs = parse_server_logs(client_summary, nrows=nrows, skiprows=skiprows)
        graph_experiment_debug(client_summary, client_log, server_logs)
        if save:
            save_filename = f"debug/debug-{i}.png"
            save_metronome_size_plot(save_filename, format="png")
        # plt.show()
        plt.close()
