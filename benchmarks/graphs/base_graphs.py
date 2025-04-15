import json
import math
import os
from pathlib import Path

import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib import ticker

LOGS_DIR: Path = Path(__file__).parent.parent / "logs"
SAVE_DIR: Path = Path(__file__).parent.parent / "logs" / "to_show2"


def parse_clients_summaries(experiment_dir: Path) -> pd.DataFrame:
    experiment_data = [
        parse_client_summary(f) for f in experiment_dir.rglob("*client-1.json")
    ]
    df = pd.concat(experiment_data)
    # Ensure the metronome_info column is treated as an ordered Categorical variable
    category_mapping = {
        "Off": "OmniPaxos",
        "RoundRobin": "Metronome_old",
        "RoundRobin2": "Metronome",
        "FastestFollower": "Metronome-WS",
    }

    # for backwards compatibility
    if "worksteal_flag" in df.columns:
        df.rename(columns={"worksteal_flag": "worksteal_ms"}, inplace=True)
        df["worksteal_ms"] = np.nan

    df["metronome_info"] = df["metronome_info"].map(category_mapping)
    worksteal_mask = (df["metronome_info"] == "Metronome") & (
        df["worksteal_ms"].fillna(False)
    )
    df.loc[worksteal_mask, "metronome_info"] = "Metronome-WS-new"
    df["metronome_info"] = pd.Categorical(
        df["metronome_info"],
        categories=[
            "OmniPaxos",
            "Metronome_old",
            "Metronome",
            "Metronome-WS",
            "Metronome-WS-new",
        ],
        ordered=True,
    )
    pd.set_option("display.max_colwidth", None)
    df = df.sort_values(by=["persist_info.value", "metronome_info"]).reset_index()
    # Ensure the file entry_size column is treated as a Categorical variable, ordered by size
    df["persist_label"] = df["persist_info.value"].apply(format_bytes)
    df["persist_label"] = pd.Categorical(
        df["persist_label"],
        categories=[format_bytes(0)] + [format_bytes(2**i) for i in range(0, 32)],
        ordered=True,
    )
    return df


def parse_client_summary(file_path: Path) -> pd.DataFrame:
    with open(file_path, "r") as file:
        client_json = json.load(file)
        normalize_batch_persist_info(client_json)
    flattened_json = {
        "file": file_path,
        **client_json.pop("client_config"),
        **client_json.pop("server_info"),
        **client_json,
    }
    # flattened_json.pop('cluster_name')
    # flattened_json.pop("location")
    # flattened_json.pop('local_deployment')
    df = pd.DataFrame([flattened_json])
    return df


# Function to flatten batch_info and persist_info
def normalize_batch_persist_info(client_json: dict):
    server_info = client_json["server_info"]
    batch_info = server_info.pop("batch_info")
    if isinstance(batch_info, dict):
        key, value = next(iter(batch_info.items()))
        server_info["batch_info.type"] = key
        server_info["batch_info.value"] = value
    else:
        server_info["batch_info.type"] = batch_info
        server_info["batch_info.value"] = None
    persist_info = server_info.pop("persist_info")
    if isinstance(persist_info, dict):
        key, value = next(iter(persist_info.items()))
        server_info["persist_info.type"] = key
        server_info["persist_info.value"] = value
    else:
        server_info["persist_info.type"] = persist_info
        server_info["persist_info.value"] = 0
    request_mode = client_json["client_config"].pop("request_mode_config")
    server_info["request_mode"] = request_mode["request_mode_config_type"]
    server_info["request_mode_value"] = request_mode["request_mode_config_value"]
    if request_mode["request_mode_config_type"] == "ClosedLoop":
        server_info["num_clients"] = request_mode["request_mode_config_value"]
    else:
        server_info["open_loop_params"] = request_mode["request_mode_config_value"]


# Find the appropriate unit based on the value of x bytes
def format_bytes(num_bytes):
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    for i in range(len(units)):
        if num_bytes < 1024:
            return f"{num_bytes:.0f} {units[i]}"
        num_bytes /= 1024  # Divide by 1024 for each step to move to the next unit


def parse_client_log(
    client_summary: pd.Series, nrows: int | None = None, skiprows: int | None = None
) -> pd.DataFrame:
    experiment_start = client_summary.client_start_time
    client_filepath = str(client_summary.file)
    client_log_filepath = client_filepath.replace(".json", ".csv")
    print(client_log_filepath)
    skip_csv_rows = range(1, skiprows + 1) if skiprows else None
    df = pd.read_csv(
        client_log_filepath,
        header=0,
        usecols=["request_time", "response_time"],
        nrows=nrows,
        skiprows=skip_csv_rows,
    )
    df["request_time"] = df["request_time"] - experiment_start
    df["response_time"] = df["response_time"] - experiment_start
    return df


def parse_server_logs(
    client_summary: pd.Series, nrows: int | None = None, skiprows: int | None = None
) -> pd.DataFrame | None:
    if client_summary.instrumented is False:
        return None
    experiment_start = client_summary.client_start_time
    client_file = str(client_summary.file)
    leader_file = client_file.replace("client-1", f"server-1").replace(".json", ".csv")
    follower_files = []
    for server_id in range(2, client_summary.cluster_size + 1):
        follower_file = client_file.replace("client-1", f"server-{server_id}").replace(
            ".json", ".csv"
        )
        follower_files.append(follower_file)

    print(leader_file)
    df = parse_leader_log(leader_file, experiment_start)
    skip_csv_rows = range(1, skiprows + 1) if skiprows else None
    for i, follower_file in enumerate(follower_files):
        print(follower_file)
        df_follower = pd.read_csv(
            follower_file, header=0, nrows=nrows, skiprows=skip_csv_rows
        )
        df_follower["net_receive"] = df_follower["net_receive"] - experiment_start
        df_follower["start_persist"] = df_follower["start_persist"] - experiment_start
        df_follower["send_accepted"] = df_follower["send_accepted"] - experiment_start
        df_follower.rename(
            columns=lambda x: f"follower_{i+2}.{x}" if x != "command_id" else x,
            inplace=True,
        )
        df = pd.merge(df, df_follower, on="command_id", how="inner")
    return df


def parse_leader_log(
    log_path: str,
    experiment_start: int,
    nrows: int | None = None,
    skiprows: int | None = None,
) -> pd.DataFrame:
    skip_csv_rows = range(1, skiprows + 1) if skiprows else None
    df = pd.read_csv(log_path, header=0, nrows=nrows, skiprows=skip_csv_rows)
    df["net_receive"] = df["net_receive"] - experiment_start
    df["channel_receive"] = df["channel_receive"] - experiment_start
    df["commit"] = df["commit"] - experiment_start
    return df


def create_base_barchart(
    latency_means: dict,
    bar_group_labels: list[str],
    legend_args: dict = {"loc": "upper right", "ncols": 1, "fontsize": 16},
    relative: bool = False,
):
    x = np.arange(len(bar_group_labels)) * 1.2  # the label locations
    bar_group_size = len(latency_means)
    width = 0.25  # the width of the bars
    multiplier = 0.5
    fig, ax = plt.subplots(layout="constrained", figsize=(10, 6))
    for label, (avg, std_dev) in latency_means.items():
        avg = tuple(0 if v is None else v for v in avg)
        offset = width * multiplier
        rects = ax.bar(
            x + offset,
            avg,
            width,
            label=label,
            yerr=std_dev,
            edgecolor="black",
            linewidth=1.5,
        )
        # Adds value labels above bars
        ax.bar_label(rects, fmt="%.2f", padding=3)
        multiplier += 1
    if relative:
        ax.set_ylabel("Relative Latency", fontsize=16)
    else:
        ax.set_ylabel("Average Latency (ms)", fontsize=16)
    ax.tick_params(axis="y", labelsize=12)
    ax.set_xticks(x + width * bar_group_size / 2, bar_group_labels, fontsize=16)
    # ax.legend(bbox_to_anchor=(1.02, 1), loc='upper left')  # Legend outside plot
    ax.legend(**legend_args)
    return fig, ax


def graph_experiment_debug(
    client_summary: pd.Series,
    client_log: pd.DataFrame,
    server_logs: pd.DataFrame | None,
):
    title = ", ".join(
        [
            f"metronome={client_summary.metronome_info}",
            f"clients=({client_summary.request_mode},{client_summary.request_mode_value})",
            f"batch_config=({client_summary['batch_info.type']},{client_summary['batch_info.value']})",
            f"\npersist_config=({client_summary['persist_info.type']}, {client_summary.persist_label})",
            f"metronome_quorum_size={client_summary.metronome_quorum_size}",
        ]
    )
    if server_logs is None:
        assert (
            client_summary.instrumented is False
        ), "No server logs despite instrumented = True"
        fig, ax = plt.subplots(layout="constrained", figsize=(12, 6))
        ax.set_title(title)
        graph_request_latency_subplot(ax, client_summary, client_log)

        def microseconds_to_seconds(x, _):
            return f"{x / 1_000_000:.2f}s"

        # Apply the formatter to the x-axis
        plt.gca().xaxis.set_major_formatter(
            ticker.FuncFormatter(microseconds_to_seconds)
        )
        return fig
    else:
        # Setup shared figure
        fig, axs = plt.subplots(
            3,
            1,
            sharex=True,
            gridspec_kw={"height_ratios": [1, 1, 1]},
            layout="constrained",
        )
        fig.suptitle(title, y=-0.05)
        axs[0].set_title(title)
        fig.set_size_inches((12, 6))
        # X-axis settings
        axs[2].set_xlabel("Experiment Time (s)")

        def scale_x_tick_labels(value, _):
            return f"{value / 1_000_000:.2f}"

        axs[2].xaxis.set_major_formatter(ticker.FuncFormatter(scale_x_tick_labels))
        axs[2].set_xlim(
            client_log["request_time"].min(), client_log["response_time"].max()
        )
        plt.xticks(rotation=45)

        # Plot data
        graph_request_latency_subplot(axs[0], client_summary, client_log)
        graph_acceptor_queue_subplot(axs[1], client_summary, server_logs)
        # graph_persist_latency_subplot(axs[2], client_summary, server_logs)
        graph_average_persist_latency_subplot(axs[2], client_summary, server_logs)


def graph_request_latency_subplot(
    fig, client_summary: pd.Series, client_log: pd.DataFrame
):
    latencies = (client_log["response_time"] - client_log["request_time"]) / 1000
    fig.scatter(
        client_log["request_time"], latencies, label="Client Request Latency", alpha=0.3
    )
    fig.set_ylim(bottom=0)
    fig.set_ylabel("Request latency (ms)")
    fig.legend(frameon=False)


def graph_acceptor_queue_subplot(
    fig, client_summary: pd.Series, server_logs: pd.DataFrame
):
    followers = range(2, client_summary.cluster_size + 1)
    for follower in followers:
        requests_received = server_logs[f"follower_{follower}.net_receive"]
        persist_requests = requests_received[
            server_logs[f"follower_{follower}.start_persist"].notna()
        ]
        assert type(persist_requests) == pd.Series
        requests_processed = server_logs[f"follower_{follower}.send_accepted"].dropna()
        events = pd.DataFrame(
            {
                "timestamp": pd.concat(
                    [persist_requests, requests_processed], ignore_index=True
                ),
                "change": pd.concat(
                    [
                        pd.Series([1] * len(persist_requests)),
                        pd.Series([-1] * len(requests_processed)),
                    ],
                    ignore_index=True,
                ),
            }
        )
        events = events.sort_values(by="timestamp").reset_index(drop=True)
        events["queue_length"] = events["change"].cumsum()
        fig.plot(
            events["timestamp"], events["queue_length"], label=f"Follower {follower}"
        )
    fig.set_ylabel("Queue Length")
    fig.yaxis.set_major_locator(ticker.MaxNLocator(integer=True))
    # fig.legend(frameon=False)


def graph_persist_latency_subplot(
    fig, client_summary: pd.Series, server_logs: pd.DataFrame
):
    # TODO: take into account metronome_quorum_size
    majority = (client_summary.cluster_size // 2) + 1
    metronome_batch = math.comb(client_summary.cluster_size, majority)
    assert metronome_batch <= 10, "more batch than colors"
    followers = range(2, client_summary.cluster_size + 1)

    # Loop through each follower and plot the time intervals
    for i, follower in enumerate(followers):
        start_key = f"follower_{follower}.start_persist"
        start_times = server_logs[start_key]
        end_times = server_logs[f"follower_{follower}.send_accepted"]
        batched_persists = (
            server_logs.groupby(start_key)["command_id"].apply(list).reset_index()
        )
        bar_containers = fig.barh(
            [i] * len(start_times),
            end_times - start_times,
            left=start_times,
            label=f"Follower {follower}",
            edgecolor="black",
            linewidth=1,
        )
        for i, rect in enumerate(bar_containers.patches):
            # Make overlapping bars visible
            command_id = server_logs["command_id"].iloc[i]
            start = server_logs[start_key].iloc[i]
            if pd.notna(start):
                flush_batch = batched_persists[
                    batched_persists[start_key] == start
                ].command_id.values[0]
                if len(flush_batch) > 1:
                    position_in_batch = flush_batch.index(command_id)
                    new_height = rect.get_height() - (0.05 * position_in_batch)
                    rect.set_height(new_height)
            # Set bar color
            # color_idx = command_id % parallel_requests
            color_idx = i % metronome_batch
            rect.set(color=global_colors[color_idx], edgecolor="black")

    # Y-axis settings
    y_positions = range(len(followers))
    fig.set_yticks(y_positions)
    fig.set_yticklabels([f"Follower {follower}" for follower in followers])
    # X-axis settings
    fig.set_xlabel("Experiment Time (ms)")

    def scale_x_tick_labels(value, _):
        return f"{value / 1000:.1f}"

    fig.xaxis.set_major_formatter(ticker.FuncFormatter(scale_x_tick_labels))
    fig.set_xlim(
        server_logs["net_receive"].min(), server_logs["sending_response"].max()
    )
    plt.xticks(rotation=45)


def graph_average_persist_latency_subplot(
    fig, client_summary: pd.Series, server_logs: pd.DataFrame
):
    followers = range(2, client_summary.cluster_size + 1)
    for follower in followers:
        start_key = f"follower_{follower}.start_persist"
        start_times = server_logs[start_key].dropna()
        end_times = server_logs[f"follower_{follower}.send_accepted"].dropna()
        fig.plot(start_times, end_times - start_times, label=f"Follower {follower}")
    fig.legend(frameon=False)
    fig.set_ylim(bottom=0)
    fig.set_ylabel("Persist latency (us)")


def graph_open_loop_experiment(save: bool = True):
    # Get experiment data
    experiment_directory = "latency-throughput-experiment"
    run_directory = "5-node-cluster-File0"
    save_path = f"./logs/to_show2/{experiment_directory}/{run_directory}"
    summaries = parse_clients_summaries(f"{experiment_directory}/{run_directory}")
    throughput_data = summaries[
        [
            "request_mode",
            "metronome_info",
            "throughput",
            "request_latency_average",
            "request_latency_std_dev",
        ]
    ].sort_values(by=["throughput"])
    # throughput_data.to_csv(f"./logs/{experiment_directory}/{run_directory}/data.csv")

    # Separate data by 'metronome_info' for different lines
    grouped = throughput_data.groupby("metronome_info")

    # Plotting
    fig, ax = plt.subplots(figsize=(10, 6))
    for metronome, group in grouped:
        # Sort by throughput to ensure lines are continuous
        group = group.sort_values("throughput")
        x = group["throughput"]
        y = group["request_latency_average"]
        std_dev = group["request_latency_std_dev"]
        ax.plot(x, y, label=f"Metronome: {metronome}", marker="o")
        ax.fill_between(x, y - std_dev, y + std_dev, alpha=0.2)
    # Labels and legend
    ax.grid(which="both", linestyle="--", linewidth=0.5, axis="x")
    ax.set_xscale("log")  # Log scale for throughput if needed
    ax.set_xlabel("Throughput", fontsize=14)
    ax.set_ylabel("Request Latency Average", fontsize=14)
    ax.set_title("Throughput vs. Request Latency Average", fontsize=16)
    ax.legend(frameon=False)
    ax.grid(True)

    plt.show()
    if save:
        os.makedirs(save_path, exist_ok=True)
        fig.savefig(f"{save_path}/throughput-latency.svg", format="svg")

    # # Create debug plots
    # for i, (_, client_summary) in enumerate(summaries.iterrows()):
    #     client_log = parse_client_log(client_summary, 100_000)
    #     server_logs = parse_server_logs(client_summary)
    #     # client_log = client_log.iloc[-10_000:].reset_index(drop=True)
    #     fig = graph_experiment_debug(client_summary, client_log, server_logs)
    #     if save:
    #         os.makedirs(save_path, exist_ok=True)
    #         fig.savefig(f"{save_path}/debug-{i}.png", format="png")
    return


global_colors = list(mcolors.TABLEAU_COLORS.values())
