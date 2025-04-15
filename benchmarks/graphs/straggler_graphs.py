import os
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from graphs.base_graphs import (
    LOGS_DIR,
    SAVE_DIR,
    create_base_barchart,
    graph_experiment_debug,
    parse_client_log,
    parse_clients_summaries,
    parse_server_logs,
)

STRAGGLER_DIR: Path = LOGS_DIR / "straggler-experiment" / "Opportunistic"
STRAGGLER_SAVE_DIR: Path = SAVE_DIR / "straggler-experiment"


def straggler_experiment_data() -> pd.DataFrame:
    experiment_dir = STRAGGLER_DIR / "5-node-cluster-3-straggler"
    return parse_clients_summaries(experiment_dir)


def save_straggler_plot(filename: str, format: str = "svg"):
    file_path = STRAGGLER_SAVE_DIR / filename
    os.makedirs(file_path.parent, exist_ok=True)
    plt.savefig(file_path, format=format)


def straggler_bar_chart(relative: bool = False, save: bool = True):
    summaries = straggler_experiment_data()
    bar_labels = ("baseline", "metronome")
    legend_args = {"loc": "upper left", "ncols": 2, "fontsize": 12, "frameon": False}
    # legend_args = {"loc": "upper right", "ncols": 1, "fontsize": 12, "frameon": False}
    client_summary = summaries.iloc[0]
    for metric, err in [("request_latency_average", "request_latency_std_dev")]:
        pivot_df = summaries.pivot_table(
            index="persist_label", columns="metronome_info", values=[metric, err]
        )
        # print(pivot_df)
        if relative:
            relative_latency_df = pivot_df["request_latency_average"].div(
                pivot_df["request_latency_average"]["OmniPaxos"], axis=0
            )
        bar_group_labels = list(pivot_df.index)
        bar_labels = pivot_df.columns.get_level_values("metronome_info").unique().values
        latency_means = {}
        for label in bar_labels:
            if relative:
                latency_means[label] = (relative_latency_df[label], None)
            else:
                latency_means[label] = (pivot_df[metric][label], pivot_df[err][label])
        fig, ax = create_base_barchart(
            latency_means, bar_group_labels, legend_args, relative
        )
        ax.set_xlabel("Data Size (bytes)", fontsize=16)
        title = f"{metric}\n" + ", ".join(
            [
                f"cluster_size={client_summary.cluster_size}",
                f"clients=({client_summary.request_mode},{client_summary.request_mode_value})",
                f"\nbatch_config=({client_summary['batch_info.type']},{client_summary['batch_info.value']})",
            ]
        )
        fig.suptitle(title, fontsize=16)
        if save:
            save_filename = f"{metric}_relative.svg" if relative else f"{metric}.svg"
            save_straggler_plot(save_filename)
        plt.show()
        plt.close()


def straggler_violin_plot(nrows: int, skiprows: int, save: bool = True):
    summaries = straggler_experiment_data()
    client_logs = []
    for i, client_summary in summaries.iterrows():
        client_log = parse_client_log(client_summary, nrows=nrows, skiprows=skiprows)
        client_log.dropna(subset=["response_time"], inplace=True)
        client_log["latency"] = (
            client_log["response_time"] - client_log["request_time"]
        ) / 1000
        client_log["entry_size"] = client_summary["persist_label"]
        client_log["metronome_info"] = client_summary.metronome_info
        client_logs.append(client_log)
    violin_data = pd.concat(client_logs, ignore_index=True)
    plt.figure(figsize=(10, 6))
    sns.violinplot(
        data=violin_data,
        x="entry_size",
        y="latency",
        hue="metronome_info",
        inner="quart",
        density_norm="width",
    )
    # palette = {False: 'skyblue', True: 'orange'}
    # sns.violinplot(data=violin_data, x="entry_size", y="latency", hue="metronome_info", split=True, inner='quart', palette=palette, density_norm="width")
    # Labels
    client_summary = summaries.iloc[0]
    title = "Latency Distribution\n" + ", ".join(
        [
            f"cluster_size={client_summary.cluster_size}",
            f"clients=({client_summary.request_mode},{client_summary.request_mode_value})",
            f"\nbatch_config=({client_summary['batch_info.type']},{client_summary['batch_info.value']})",
        ]
    )
    plt.title(title)
    plt.legend(title=None, frameon=True)
    plt.tick_params(axis="y", labelsize=12)
    plt.tick_params(axis="x", labelsize=14)
    plt.ylim(bottom=0, top=16)
    plt.xlabel("Data Size (bytes)", fontsize=14)
    plt.ylabel("Response Latency (ms)", fontsize=14)
    plt.tight_layout()
    if save:
        save_filename = "latency_distribution.svg"
        save_straggler_plot(save_filename)
    plt.show()
    plt.close()


def straggler_debug_plots(nrows: int, skiprows: int, save: bool = True):
    summaries = straggler_experiment_data()
    for i, (_, client_summary) in enumerate(summaries.iterrows()):
        client_log = parse_client_log(client_summary, nrows=nrows, skiprows=skiprows)
        server_logs = parse_server_logs(client_summary, nrows=nrows, skiprows=skiprows)
        graph_experiment_debug(client_summary, client_log, server_logs)
        if save:
            save_filename = f"debug/debug-{i}.png"
            save_straggler_plot(save_filename, format="png")
        # plt.show()
        plt.close()
    return
