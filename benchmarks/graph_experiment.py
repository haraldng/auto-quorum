import math
from pathlib import Path
import pandas as pd
import json
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from matplotlib import ticker
import seaborn as sns
import numpy as np


def parse_clients_summaries(experiment_name: str) -> pd.DataFrame:
    experiment_directory = Path(f"logs/{experiment_name}")
    experiment_data = [parse_client_summary(f) for f in experiment_directory.rglob("*client-1.json")]
    df = pd.concat(experiment_data)
    df['metronome_info'] = pd.Categorical(df['metronome_info'], categories=["Off", "RoundRobin", "RoundRobin2", "FastestFollower"], ordered=True)
    df = df.sort_values(by=['delay_info.value', 'metronome_info']).reset_index()
    # Ensure the file data_size column is treated as a Categorical variable, ordered by size
    if df['delay_info.type'][0] == "File":
        df['delay_info.value'] = df['delay_info.value'].apply(format_bytes)
        df['delay_info.value'] = pd.Categorical(
            df['delay_info.value'],
            categories=[format_bytes(0)] + [format_bytes(2**i) for i in range(0, 32)],
            ordered=True
        )
    return df

def parse_client_summary(file_path: Path) -> pd.DataFrame:
    with open(file_path, 'r') as file:
        client_json = json.load(file)
        normalize_persist_delay_info(client_json)
    flattened_json = { "file": file_path, **client_json.pop('client_config'), **client_json.pop('server_info'), **client_json }
    flattened_json.pop('cluster_name')
    flattened_json.pop('location')
    flattened_json.pop('local_deployment')
    df = pd.DataFrame([flattened_json])
    return df

# Function to flatten persist_info and delay_info
def normalize_persist_delay_info(client_json: dict):
    server_info = client_json['server_info']
    persist_info = server_info.pop('persist_info')
    if isinstance(persist_info, dict):
        key, value = next(iter(persist_info.items()))
        server_info['persist_info.type'] = key
        server_info['persist_info.value'] = value
    else:
        server_info['persist_info.type'] = persist_info
        server_info['persist_info.value'] = None
    delay_info = server_info.pop('delay_info')
    if isinstance(delay_info, dict):
        key, value = next(iter(delay_info.items()))
        server_info['delay_info.type'] = key
        server_info['delay_info.value'] = value
    try:
        request_mode = client_json['client_config'].pop('request_mode_config')
        server_info['request_mode'] = request_mode['request_mode_config_type']
        if request_mode['request_mode_config_type'] == "ClosedLoop":
            server_info['num_clients'] = request_mode['request_mode_config_value']
        else:
            server_info['open_loop_params'] = request_mode['request_mode_config_value']
    except:
        print("WARNING: outdated data without request_mode")

# Find the appropriate unit based on the value of x bytes
def format_bytes(num_bytes):
    units = ['B', 'KiB', 'MiB', 'GiB', 'TiB']
    for i in range(len(units)):
        if num_bytes < 1024:
            return f'{num_bytes:.0f} {units[i]}'
        num_bytes /= 1024  # Divide by 1024 for each step to move to the next unit

def parse_client_log(client_summary: pd.Series, nrows: int | None=None, skiprows: int | None=None) -> pd.DataFrame:
    experiment_start = client_summary.client_start_time
    client_filepath = str(client_summary.file)
    client_log_filepath = client_filepath.replace(".json", ".csv")
    print(client_log_filepath)
    skip_csv_rows = range(1, skiprows+1) if skiprows else None
    df = pd.read_csv(client_log_filepath, header=0, usecols=['request_time', 'response_time'], nrows=nrows, skiprows=skip_csv_rows)
    df['request_time'] = df['request_time'] - experiment_start
    df['response_time'] = df['response_time'] - experiment_start
    return df

def parse_server_logs(client_summary: pd.Series, nrows: int | None=None, skiprows: int | None=None) -> pd.DataFrame | None:
    if client_summary.instrumented is False:
        return None
    experiment_start = client_summary.client_start_time
    client_file = str(client_summary.file)
    leader_file = client_file.replace("client-1", f"server-1").replace(".json", ".csv")
    follower_files = []
    for server_id in range(2, client_summary.cluster_size+1):
        follower_file = client_file.replace("client-1", f"server-{server_id}").replace(".json", ".csv")
        follower_files.append(follower_file)

    print(leader_file)
    df = parse_leader_log(leader_file, experiment_start)
    skip_csv_rows = range(1, skiprows+1) if skiprows else None
    for i, follower_file in enumerate(follower_files):
        print(follower_file)
        df_follower = pd.read_csv(follower_file, header=0, nrows=nrows, skiprows=skip_csv_rows)
        df_follower['net_receive'] = df_follower['net_receive'] - experiment_start
        df_follower['start_persist'] = df_follower['start_persist'] - experiment_start
        df_follower['send_accepted'] = df_follower['send_accepted'] - experiment_start
        df_follower.rename(columns=lambda x: f"follower_{i+2}.{x}" if x != 'command_id' else x, inplace=True)
        df = pd.merge(df, df_follower, on="command_id", how="inner")
    return df

def parse_leader_log(log_path: str, experiment_start: int, nrows: int | None=None, skiprows: int | None=None) -> pd.DataFrame:
    skip_csv_rows = range(1, skiprows+1) if skiprows else None
    df = pd.read_csv(log_path, header=0, nrows=nrows, skiprows=skip_csv_rows)
    df['net_receive'] = df['net_receive'] - experiment_start
    df['channel_receive'] = df['channel_receive'] - experiment_start
    df['commit'] = df['commit'] - experiment_start
    return df

def create_base_barchart(latency_means: dict, bar_group_labels: list[str], legend_args: dict = {"loc": "upper right", "ncols": 1, "fontsize": 16}):
    x = np.arange(len(bar_group_labels)) * 1.2  # the label locations
    bar_group_size = len(latency_means)
    width = 0.25  # the width of the bars
    multiplier = 0.5
    fig, ax = plt.subplots(layout='constrained', figsize=(10,6))
    for label, (avg, std_dev) in latency_means.items():
        avg = tuple(0 if v is None else v for v in avg)
        offset = width * multiplier
        rects = ax.bar(x + offset, avg, width, label=label, yerr=std_dev, edgecolor="black", linewidth=1.5)
        # Adds value labels above bars
        ax.bar_label(rects, fmt='%.2f', padding=3)
        multiplier += 1
    ax.set_ylabel('Relative Latency', fontsize=24)
    ax.tick_params(axis='y', labelsize=20)
    ax.set_xticks(x + width * bar_group_size/2, bar_group_labels, fontsize=20)
    ax.legend(bbox_to_anchor=(1.02, 1), loc='upper left')  # Legend outside plot
    # ax.legend(**legend_args)
    return fig, ax

def graph_experiment_debug(client_summary: pd.Series, client_log: pd.DataFrame, server_logs: pd.DataFrame| None):
    title = f"metronome={client_summary.metronome_info}, clients={client_summary.request_mode}, persist=({client_summary['persist_info.type']},{client_summary['persist_info.value']}), storage=({client_summary['delay_info.type']}, {client_summary['delay_info.value']}), metronome_quorum_size={client_summary.metronome_quorum_size}"
    if server_logs is None:
        assert client_summary.instrumented is False, "No server logs despite instrumented = True"
        fig, ax = plt.subplots(layout="constrained", figsize=(12,6))
        ax.set_title(title)
        graph_request_latency_subplot(ax, client_summary, client_log)
        def microseconds_to_seconds(x, _):
            return f'{x / 1_000_000:.2f}s'
        # Apply the formatter to the x-axis
        plt.gca().xaxis.set_major_formatter(ticker.FuncFormatter(microseconds_to_seconds))
        return fig
    else:
        # Setup shared figure
        fig, axs = plt.subplots(3, 1, sharex=True, gridspec_kw={'height_ratios': [1, 1, 1]}, layout="constrained")
        fig.suptitle(title, y=-0.05)
        axs[0].set_title(title)
        fig.set_size_inches((12,6))
        # X-axis settings
        axs[2].set_xlabel('Experiment Time (s)')
        def scale_x_tick_labels(value, _):
            return f'{value / 1_000_000:.2f}'
        axs[2].xaxis.set_major_formatter(ticker.FuncFormatter(scale_x_tick_labels))
        axs[2].set_xlim(client_log['request_time'].min(), client_log['response_time'].max())
        plt.xticks(rotation=45)

        # Plot data
        graph_request_latency_subplot(axs[0], client_summary, client_log)
        graph_acceptor_queue_subplot(axs[1], client_summary, server_logs)
        # graph_persist_latency_subplot(axs[2], client_summary, server_df)
        graph_average_persist_latency_subplot(axs[2], client_summary, server_logs)
        return fig

def graph_request_latency_subplot(fig, client_summary: pd.Series, client_log: pd.DataFrame):
    latencies = (client_log['response_time'] - client_log['request_time']) / 1000
    fig.scatter(client_log['request_time'], latencies, label='Client Request Latency', alpha=0.3)
    fig.set_ylim(bottom=0)
    fig.set_ylabel('Request latency (ms)')
    fig.legend()

def graph_acceptor_queue_subplot(fig, client_summary: pd.Series, server_logs: pd.DataFrame):
    followers = range(2, client_summary.cluster_size+1)
    for follower in followers:
        requests_received = server_logs[f'follower_{follower}.net_receive']
        persist_requests = requests_received[server_logs[f'follower_{follower}.start_persist'].notna()]
        assert type(persist_requests) == pd.Series
        requests_processed = server_logs[f'follower_{follower}.send_accepted'].dropna()
        events = pd.DataFrame({
            'timestamp': pd.concat([persist_requests, requests_processed], ignore_index=True),
            'change': pd.concat([pd.Series([1] * len(persist_requests)), pd.Series([-1] * len(requests_processed))], ignore_index=True)
        })
        events = events.sort_values(by='timestamp').reset_index(drop=True)
        events['queue_length'] = events['change'].cumsum()
        fig.plot(events['timestamp'], events['queue_length'], label=f'Follower {follower}')
    fig.set_ylabel('Queue Length')
    fig.yaxis.set_major_locator(ticker.MaxNLocator(integer=True))
    # fig.legend()


def graph_persist_latency_subplot(fig, client_summary: pd.Series, server_logs: pd.DataFrame):
    # TODO: take into account metronome_quorum_size
    majority = (client_summary.cluster_size // 2) + 1
    metronome_batch = math.comb(client_summary.cluster_size, majority)
    assert metronome_batch <= 10, "more batch than colors"
    followers = range(2, client_summary.cluster_size+1)

    # Loop through each follower and plot the time intervals
    for i, follower in enumerate(followers):
        start_key = f'follower_{follower}.start_persist';
        start_times = server_logs[start_key]
        end_times = server_logs[f'follower_{follower}.send_accepted']
        batched_persists = server_logs.groupby(start_key)['command_id'].apply(list).reset_index()
        bar_containers = fig.barh(
            [i] * len(start_times),
            end_times - start_times,
            left=start_times,
            label=f'Follower {follower}',
            edgecolor='black',
            linewidth=1
        )
        for (i, rect) in enumerate(bar_containers.patches):
            # Make overlapping bars visible
            command_id = server_logs['command_id'].iloc[i]
            start = server_logs[start_key].iloc[i]
            if pd.notna(start):
                flush_batch = batched_persists[batched_persists[start_key] == start].command_id.values[0]
                if len(flush_batch) > 1:
                    position_in_batch = flush_batch.index(command_id)
                    new_height = rect.get_height() - (0.05 * position_in_batch)
                    rect.set_height(new_height)
            # Set bar color
            # color_idx = command_id % parallel_requests
            color_idx = i % metronome_batch
            rect.set(color=global_colors[color_idx], edgecolor='black')

    # Y-axis settings
    y_positions = range(len(followers))
    fig.set_yticks(y_positions)
    fig.set_yticklabels([f'Follower {follower}' for follower in followers])
    # X-axis settings
    fig.set_xlabel('Experiment Time (ms)')
    def scale_x_tick_labels(value, _):
        return f'{value / 1000:.1f}'
    fig.xaxis.set_major_formatter(ticker.FuncFormatter(scale_x_tick_labels))
    fig.set_xlim(server_logs['net_receive'].min(), server_logs['sending_response'].max())
    plt.xticks(rotation=45)

def graph_average_persist_latency_subplot(fig, client_summary: pd.Series, server_logs: pd.DataFrame):
    followers = range(2, client_summary.cluster_size+1)
    for follower in followers:
        start_key = f'follower_{follower}.start_persist';
        start_times = server_logs[start_key].dropna()
        end_times = server_logs[f'follower_{follower}.send_accepted'].dropna()
        fig.plot(start_times, end_times - start_times, label=f'Follower {follower}')
    fig.legend()
    fig.set_ylim(bottom=0)
    fig.set_ylabel('Persist latency (us)')


def graph_local_experiment():
    # Get experiment data
    experiment_directory = "local-experiments"
    df = parse_clients_summaries(experiment_directory)
    for (i, (_, client_summary)) in enumerate(df.iterrows()):
        client_log = parse_client_log(client_summary, 10_000)
        server_logs = parse_server_logs(client_summary)
        fig = graph_experiment_debug(client_summary, client_log, server_logs)
        plt.show()
        fig.savefig(f"./logs/{experiment_directory}/debug-{i}.svg", format="svg")


def graph_closed_loop_experiment(save: bool=True):
    # Get experiment data
    experiment_directory = "5-min/closed-loop-experiments-Opportunistic"
    run_directory = "5-node-cluster-1000-clients"
    # three_df = parse_client_logs(f"{experiment_directory}/3-node-cluster-10-clients")
    five_df = parse_clients_summaries(f"{experiment_directory}/{run_directory}")
    # five_df = parse_experiment_logs(f"{experiment_directory}/5-node-cluster")
    # seven_df = parse_experiment_logs(f"{experiment_directory}/7-node-cluster")


    # Create bar chart
    bar_labels = ("baseline", "metronome")
    legend_args = {"loc": "upper left", "ncols": 1, "fontsize": 16}
    # for df in [three_df, five_df, seven_df]:
    for df in [five_df]:
        client_summary = df.iloc[0]
        for (metric, err) in [("request_latency_average", "request_latency_std_dev")]:#, ("total_time", None)]:
            if err is not None:
                pivot_df = df.pivot_table(index='delay_info.value', columns='metronome_info', values=[metric, err])
                # print(pivot_df)
                relative_latency_df = pivot_df['request_latency_average'].div(
                    pivot_df['request_latency_average']['Off'], axis=0
                )
                relative_std_dev_df = pivot_df['request_latency_std_dev'].div(
                    pivot_df['request_latency_std_dev']['Off'], axis=0
                )

                bar_group_labels = list(pivot_df.index)
                bar_labels = pivot_df.columns.get_level_values('metronome_info').unique().values
                latency_means = {}
                for label in bar_labels:
                    latency_means[label] = (relative_latency_df[label], None)
                    # latency_means[label] = (pivot_df[metric][label], pivot_df[err][label])
            else:
                pivot_df = df.pivot_table(index='delay_info.value', columns='metronome_info', values=[metric])
                bar_group_labels = list(pivot_df.index)
                bar_labels = pivot_df.columns.get_level_values('metronome_info').unique().values
                latency_means = {}
                for label in bar_labels:
                    latency_means[label] = (pivot_df[metric][label], None)
            fig, ax = create_base_barchart(latency_means, bar_group_labels, legend_args)
            ax.set_xlabel("Data Size (bytes)", fontsize=24)
            # plt.xticks(rotation=45)
            # fig.suptitle(f"{metric}\ncluster_size={client_summary.cluster_size}, clients={client_summary.request_mode_config}, persist_strat=({client_summary['persist_info.type']},{client_summary['persist_info.value']})", fontsize=16)
            fig.suptitle(f"{metric}\ncluster_size={client_summary.cluster_size}, clients=1000, persist_strat=({client_summary['persist_info.type']},{client_summary['persist_info.value']})", fontsize=16)
            if save:
                fig.savefig(f"./logs/{experiment_directory}/{run_directory}/{metric}_relative.svg", format="svg")
            plt.show()
    return

    nrows = 500_000
    # skiprows = 1_000_000
    skiprows = 10_000

    # # Create violin plot
    # client_logs = []
    # for i, client_summary in five_df.iterrows():
    #     if client_summary['delay_info.value'] == "16 KiB":
    #         skiprows = 10_000
    #     client_log = parse_client_log(client_summary, nrows=nrows, skiprows=skiprows)
    #     client_log.dropna(subset=["response_time"], inplace=True)
    #     client_log['latency'] = (client_log['response_time'] - client_log['request_time']) / 1000
    #     client_log['data_size'] = client_summary['delay_info.value']
    #     client_log['metronome_info'] = client_summary.metronome_info
    #     client_logs.append(client_log)
    # violin_data = pd.concat(client_logs, ignore_index=True)
    # plt.figure(figsize=(10, 6))
    # sns.violinplot(data=violin_data, x="data_size", y="latency", hue="metronome_info", inner='quart', density_norm="width")
    # # palette = {False: 'skyblue', True: 'orange'}
    # # sns.violinplot(data=violin_data, x="data_size", y="latency", hue="metronome_info", split=True, inner='quart', palette=palette, density_norm="width")
    # # Labels
    # client_summary = five_df.iloc[0]
    # # title = f"Latency Distribution\ncluster_size={client_summary.cluster_size}, clients={client_summary.request_mode_config}, persist_strat=({client_summary['persist_info.type']},{client_summary['persist_info.value']})"
    # title = f"Latency Distribution\ncluster_size={client_summary.cluster_size}, clients=1000, persist_strat=({client_summary['persist_info.type']},{client_summary['persist_info.value']})"
    # plt.title(title)
    # plt.xticks(rotation=45)
    # plt.xlabel("Datasize (bytes)")
    # plt.ylabel("Response Latency (ms)")
    # if save:
    #     plt.savefig(f"./logs/{experiment_directory}/{run_directory}/latency_distribution.svg", format="svg")
    # plt.show()

    # Create debug plots
    for i, (_, client_summary) in enumerate(five_df.iterrows()):
        # if client_summary['delay_info.value'] == "1 KiB" and client_summary['metronome_info'] == "RoundRobin2":
        #     pass
        # else:
        #     continue
        # client_log = parse_client_log(client_summary, skiprows=5_500_000)
        # latencies = (client_log['response_time'] - client_log['request_time']) / 1000
        # print(latencies.describe())
        # print()
        # print(latencies.nlargest(100).index)
        # fig = graph_experiment_debug(client_summary, client_log, None)
        client_log = parse_client_log(client_summary, nrows=nrows, skiprows=skiprows)
        server_logs = parse_server_logs(client_summary, nrows=nrows, skiprows=skiprows)
        fig = graph_experiment_debug(client_summary, client_log, server_logs)
        # plt.show()
        if save:
            fig.savefig(f"./logs/{experiment_directory}/{run_directory}/debug-{i}.png", format="png")
    return


def graph_num_clients_latency_experiment(save: bool=True):
    # Get experiment data
    experiment_directory = "closed-loop-latency-experiments"
    run_directory = "Individual/5-node-cluster"
    # three_df = parse_client_logs(f"{experiment_directory}/3-node-cluster-10-clients")
    five_df = parse_clients_summaries(f"{experiment_directory}/{run_directory}")
    # five_df = parse_experiment_logs(f"{experiment_directory}/5-node-cluster")
    # seven_df = parse_experiment_logs(f"{experiment_directory}/7-node-cluster")
    print(five_df.columns)


    df = five_df[['delay_info.value', 'num_clients', "throughput", 'metronome_info', 'request_latency_average', 'request_latency_std_dev']].sort_values(by=['delay_info.value', 'num_clients'])
    print(df)
    # Group data by 'delay_info.value' and 'metronome_info'
    grouped = df.groupby(['delay_info.value', 'metronome_info'])

    # Plotting
    fig, ax = plt.subplots(figsize=(16, 6))
    for (delay, metronome), group in grouped:
        if delay == "0 B":
            continue
        # Sort by 'num_clients' to ensure continuous lines
        group = group.sort_values("num_clients")
        x = group["num_clients"]
        y = group["request_latency_average"]

        # Plot line for this combination
        label = f"Delay: {delay}, Metronome: {metronome}"
        ax.plot(x, y, label=label, marker='o')

        # Optionally add shaded area for standard deviation
        # std_dev = group["request_latency_std_dev"]
        # ax.fill_between(x, y - std_dev, y + std_dev, alpha=0.2)

    # Labels, legend, and grid
    ax.set_xscale("log")  # Log scale for x-axis (if needed)
    # ax.set_yscale("log")  # Log scale for x-axis (if needed)
    ax.set_xlabel("Number of Clients", fontsize=14)
    ax.set_ylabel("Average Request Latency (ms)", fontsize=14)
    ax.set_title("Closed Loop Clients vs. Average Request Latency", fontsize=16)
    ax.legend(title="Configurations", bbox_to_anchor=(1.01, 1), loc='upper left')  # Legend outside plot
    ax.grid(which="both", linestyle="--", linewidth=0.5, axis="x")
    plt.tight_layout()  # Adjust layout to fit legend
    plt.show()
    if save:
        fig.savefig(f"./logs/{experiment_directory}/{run_directory}/throughput-latency.svg", format="svg")

def graph_latency_throughput_experiment(save: bool=True):
    # Get experiment data
    experiment_directory = "latency-throughput-experiment"
    run_directory = "5-node-cluster-File0"
    summaries = parse_clients_summaries(f"{experiment_directory}/{run_directory}")
    throughput_data = summaries[['request_mode', 'metronome_info','throughput', 'request_latency_average', 'request_latency_std_dev']].sort_values(by=['throughput'])
    print(throughput_data)
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
        ax.plot(x, y, label=f"Metronome: {metronome}", marker='o')
        ax.fill_between(x, y - std_dev, y + std_dev, alpha=0.2)
    # Labels and legend
    ax.grid(which="both", linestyle="--", linewidth=0.5, axis="x")
    ax.set_xscale("log")  # Log scale for throughput if needed
    ax.set_xlabel("Throughput", fontsize=14)
    ax.set_ylabel("Request Latency Average", fontsize=14)
    ax.set_title("Throughput vs. Request Latency Average", fontsize=16)
    ax.legend()
    ax.grid(True)

    plt.show()
    if save:
        fig.savefig(f"./logs/{experiment_directory}/{run_directory}/throughput-latency.svg", format="svg")


    

    # # Create debug plots
    # for i, (_, client_summary) in enumerate(summaries.iterrows()):
    #     client_log = parse_client_log(client_summary, 100_000)
    #     server_logs = parse_server_logs(client_summary)
    #     # client_log = client_log.iloc[-10_000:].reset_index(drop=True)
    #     fig = graph_experiment_debug(client_summary, client_log, server_logs)
    #     if save:
    #         fig.savefig(f"./logs/{experiment_directory}/{run_directory}/debug-{i}.png", format="png")
    return



# def graph_metronome_size_experiment():
#     # Get experiment data
#     experiment_directory = "metronome-size-experiments"
#     # three_df = parse_experiment_logs(f"{experiment_directory}/3-node-cluster")
#     five_df = parse_experiment_logs(f"{experiment_directory}/5-node-cluster")
#     seven_df = parse_experiment_logs(f"{experiment_directory}/7-node-cluster")
#
#     # Create experiment graphs
#     bar_labels = ("baseline", "metronome")
#     legend_args = {"loc": "upper left", "ncols": 1, "fontsize": 16}
#     # for df in [three_df, five_df, seven_df]:
#     for df in [five_df, seven_df]:
#         cluster_size = df["cluster_size"][0]
#         for (metric, err) in [("request_latency_average", "request_latency_std_dev"), ("batch_latency_average", "batch_latency_std_dev")]:
#             pivot_df = df.pivot_table(index='metronome_quorum_size', columns='metronome_info', values=[metric, err])
#             print(pivot_df)
#             bar_group_labels = list(pivot_df.index)
#             latency_means = {
#                 bar_labels[0]: (pivot_df[metric][0], pivot_df[err][0]),
#                 bar_labels[1]: (pivot_df[metric][2], pivot_df[err][2]),
#             }
#             fig, ax = create_base_barchart(latency_means, bar_group_labels, legend_args)
#             ax.set_xlabel("Metronome Quorum Size", fontsize=24)
#             fig.suptitle(f"{cluster_size}-cluster {metric}", fontsize=24)
#             fig.savefig(f"./logs/{experiment_directory}/{cluster_size}-node-cluster-{metric}.svg", format="svg")
#             plt.show()


def main():
    # graph_local_experiment()
    # graph_closed_loop_experiment()
    graph_num_clients_latency_experiment()
    # graph_latency_throughput_experiment()
    # graph_metronome_size_experiment()
    pass

if __name__ == "__main__":
    global_colors = list(mcolors.TABLEAU_COLORS.values())
    main()
