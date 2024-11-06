import os
import math
import pandas as pd
import json
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from matplotlib import ticker
import numpy as np
import re


def find_server_logs(experiment_directory: str) -> list[str]:
    server_pattern = re.compile(r'.*server-\d+\.json$')
    server_files = [f for f in os.listdir(experiment_directory) if server_pattern.match(f)]
    return server_files

def find_client_logs(experiment_directory: str) -> list[str]:
    client_pattern = re.compile(r'.*client-1.json$')
    client_files = [f for f in os.listdir(experiment_directory) if client_pattern.match(f)]
    return client_files

def parse_client_logs(experiment_name: str) -> pd.DataFrame:
    experiment_files = [f"logs/{experiment_name}/{f}" for f in find_client_logs(f"logs/{experiment_name}")]
    experiment_data = [parse_client_log(f) for f in experiment_files]
    df = pd.concat(experiment_data)
    df = df.sort_values(by=['delay_info.value', 'use_metronome']).reset_index()
    return df

def parse_client_log(file_path: str) -> pd.DataFrame:
    with open(file_path, 'r') as file:
        client_json = json.load(file)
        normalize_persist_delay_info(client_json)
    flattened_json = {"file": file_path, **client_json.pop('client_config'), **client_json.pop('server_info'), **client_json}
    flattened_json.pop('client_latencies_average')
    flattened_json.pop('client_latencies_std_dev')
    flattened_json.pop('cluster_name')
    flattened_json.pop('location')
    flattened_json.pop('local_deployment')
    df = pd.DataFrame([flattened_json])
    return df

# Function to flatten persist_info and delay_info
def normalize_persist_delay_info(client_json: dict):
    server_info = client_json['server_info']
    persist_info = server_info.pop('persist_info')
    if isinstance(persist_info, dict):  # Nested case
        key, value = next(iter(persist_info.items()))
        server_info['persist_info.type'] = key
        server_info['persist_info.value'] = value
    else:  # Simple case
        server_info['persist_info.type'] = persist_info
        server_info['persist_info.value'] = None
    delay_info = server_info.pop('delay_info')
    if isinstance(delay_info, dict):
        key, value = next(iter(delay_info.items()))
        server_info['delay_info.type'] = key
        server_info['delay_info.value'] = value

def parse_server_logs(client_data: pd.Series) -> pd.DataFrame:
    client_file = client_data.file
    leader_file = client_file.replace("client-1", f"server-1")
    follower_files = []
    for server_id in range(2, client_data.cluster_size+1):
        follower_file = client_file.replace("client-1", f"server-{server_id}")
        follower_files.append(follower_file)

    print(leader_file)
    df, experiment_start = parse_leader_log(leader_file)
    for i, follower_file in enumerate(follower_files):
        print(follower_file)
        df_follower = pd.read_json(follower_file)
        df_follower['receive_accdec'] = df_follower['receive_accdec'] - experiment_start
        df_follower['start_persist'] = df_follower['start_persist'] - experiment_start
        df_follower['send_accepted'] = df_follower['send_accepted'] - experiment_start
        df_follower.rename(columns=lambda x: f"follower_{i+2}.{x}" if x != 'command_id' else x, inplace=True)
        df = pd.merge(df, df_follower, on="command_id", how="inner")
    return df

def parse_leader_log(log_path: str):
    df = pd.read_json(log_path)
    experiment_start = df['net_receive'].min()
    df['net_receive'] = df['net_receive'] - experiment_start
    df['channel_receive'] = df['channel_receive'] - experiment_start
    df['commit'] = df['commit'] - experiment_start
    df['sending_response'] = df['sending_response'] - experiment_start
    def convert_time_in_list_of_dicts(list_of_dicts):
        for d in list_of_dicts:
            d['time'] = d['time'] - experiment_start
        return list_of_dicts
    df['send_accdec'] = df['send_accdec'].apply(convert_time_in_list_of_dicts)
    df['receive_accepted'] = df['receive_accepted'].apply(convert_time_in_list_of_dicts)
    return (df, experiment_start)

def create_base_barchart(latency_means: dict, bar_group_labels: list[str], legend_args: dict = {"loc": "upper right", "ncols": 1, "fontsize": 16}):
    x = np.arange(len(bar_group_labels))  # the label locations
    bar_group_size = len(latency_means)
    width = 0.25  # the width of the bars
    multiplier = 0.5
    fig, ax = plt.subplots(layout='constrained', figsize=(10,6))
    for label, (avg, std_dev) in latency_means.items():
        avg = tuple(0 if v is None else v for v in avg)
        offset = width * multiplier
        rects = ax.bar(x + offset, avg, width, label=label, yerr=std_dev)
        # Adds value labels above bars
        ax.bar_label(rects, fmt='%.2f', padding=3)
        multiplier += 1
    ax.set_ylabel('Latency (ms)', fontsize=24)
    ax.tick_params(axis='y', labelsize=20)
    if bar_group_size == 2:
        ax.set_xticks(x + width, bar_group_labels, fontsize=20)
    elif bar_group_size == 3:
        ax.set_xticks(x + width * 1.5, bar_group_labels, fontsize=20)
    else:
        raise ValueError(f"Haven't implemented {bar_group_size} sized bar groups")
    ax.legend(**legend_args)
    return fig, ax

def graph_experiment_debug(client_data: pd.Series, servers_data: pd.DataFrame):
    # TODO: take into account metronome_quorum_size
    majority = (client_data.cluster_size // 2) + 1
    metronome_batch = math.comb(client_data.cluster_size, majority)
    assert metronome_batch <= 10, "more batch than colors"
    followers = range(2, client_data.cluster_size+1)
    df = servers_data.iloc[0:100]

    # Loop through each follower and plot the time intervals
    _, ax = plt.subplots(figsize=(12, 6))
    for i, follower in enumerate(followers):
        # Extract the start and end times for the current follower
        start_key = f'follower_{follower}.start_persist';
        start_times = df[start_key]
        end_times = df[f'follower_{follower}.send_accepted']
        batched_persists = df.groupby(start_key)['command_id'].apply(list).reset_index()
        # Create horizontal bars for the intervals
        bar_containers = ax.barh(
            [i] * len(start_times),
            end_times - start_times,
            left=start_times,
            label=f'Follower {follower}',
            edgecolor='black',
            linewidth=1
        )
        for (i, rect) in enumerate(bar_containers.patches):
            # Make overlapping bars visible
            command_id = df['command_id'].iloc[i]
            start = df[start_key].iloc[i]
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

    # Set the y-ticks to follower names
    y_positions = range(len(followers))
    ax.set_yticks(y_positions)
    ax.set_yticklabels([f'Follower {follower}' for follower in followers])
    # Set labels and title
    ax.set_xlabel('Experiment Time (ms)')
    ax.set_title(f"metronome_quorum_size={client_data.metronome_quorum_size}, clients={client_data.num_parallel_requests}, storage=({client_data['delay_info.type']},{client_data['delay_info.value']}), persist=({client_data['persist_info.type']},{client_data['persist_info.value']}), metronome={client_data['use_metronome']}")
    # Formatting the x-axis to show time
    # Define a custom function to divide x-axis tick labels by 1000
    def scale_x_tick_labels(value, _):
        return f'{value / 1000:.1f}'
    # Apply FuncFormatter to x-axis
    ax.xaxis.set_major_formatter(ticker.FuncFormatter(scale_x_tick_labels))
    ax.set_xlim(df['net_receive'].min(), df['sending_response'].max())
    plt.xticks(rotation=45)
    # Show the plot
    plt.tight_layout()
    plt.show()

def graph_closed_loop_experiment(sleep: bool=False):
    # Get experiment data
    experiment_directory = "closed-loop-experiments-batch" if not sleep else "closed-loop-experiments-sleep"
    # three_df = parse_client_logs(f"{experiment_directory}/3-node-cluster-10-clients")
    five_df = parse_client_logs(f"{experiment_directory}/5-node-cluster-30-clients")
    # five_df = parse_experiment_logs(f"{experiment_directory}/5-node-cluster")
    # seven_df = parse_experiment_logs(f"{experiment_directory}/7-node-cluster")

    # Debug
    for (_, client_data) in five_df.iterrows():
        servers_df = parse_server_logs(client_data)
        graph_experiment_debug(client_data, servers_df)

    # Create experiment graphs
    bar_labels = ("baseline", "metronome")
    legend_args = {"loc": "upper left", "ncols": 1, "fontsize": 16}
    # for df in [three_df, five_df, seven_df]:
    for df in [five_df]:
        cluster_size = df["cluster_size"][0]
        print(df)
        for (metric, err) in [("request_latency_average", "request_latency_std_dev"), ("total_time", None)]:
            pivot_df = df.pivot_table(index='delay_info.value', columns='use_metronome', values=[metric, err])
            print(pivot_df)
            bar_group_labels = list(pivot_df.index)
            latency_means = {
                bar_labels[0]: (pivot_df[metric][0], pivot_df[err][0]),
                bar_labels[1]: (pivot_df[metric][2], pivot_df[err][2]),
            }
            fig, ax = create_base_barchart(latency_means, bar_group_labels, legend_args)
            ax.set_xlabel("Data Size (bytes)", fontsize=24)
            # ax.set_xlabel("Storage delay (microsec)", fontsize=24)
            fig.suptitle(f"{cluster_size}-cluster {metric}", fontsize=24)
            # fig.savefig(f"./logs/{experiment_directory}/{cluster_size}-node-cluster-{metric}.svg", format="svg")
            plt.show()

def graph_metronome_size_experiment():
    # Get experiment data
    experiment_directory = "metronome-size-experiments"
    # three_df = parse_experiment_logs(f"{experiment_directory}/3-node-cluster")
    five_df = parse_experiment_logs(f"{experiment_directory}/5-node-cluster")
    seven_df = parse_experiment_logs(f"{experiment_directory}/7-node-cluster")

    # Create experiment graphs
    bar_labels = ("baseline", "metronome")
    legend_args = {"loc": "upper left", "ncols": 1, "fontsize": 16}
    # for df in [three_df, five_df, seven_df]:
    for df in [five_df, seven_df]:
        cluster_size = df["cluster_size"][0]
        for (metric, err) in [("request_latency_average", "request_latency_std_dev"), ("batch_latency_average", "batch_latency_std_dev")]:
            pivot_df = df.pivot_table(index='metronome_quorum_size', columns='use_metronome', values=[metric, err])
            print(pivot_df)
            bar_group_labels = list(pivot_df.index)
            latency_means = {
                bar_labels[0]: (pivot_df[metric][0], pivot_df[err][0]),
                bar_labels[1]: (pivot_df[metric][2], pivot_df[err][2]),
            }
            fig, ax = create_base_barchart(latency_means, bar_group_labels, legend_args)
            ax.set_xlabel("Metronome Quorum Size", fontsize=24)
            fig.suptitle(f"{cluster_size}-cluster {metric}", fontsize=24)
            fig.savefig(f"./logs/{experiment_directory}/{cluster_size}-node-cluster-{metric}.svg", format="svg")
            plt.show()


def main():
    # create_closed_loop_experiment_csv()
    # graph_closed_loop_sleep_experiment()
    graph_closed_loop_experiment()
    # graph_metronome_size_experiment()
    pass

if __name__ == "__main__":
    global_colors = list(mcolors.TABLEAU_COLORS.values())
    main()
