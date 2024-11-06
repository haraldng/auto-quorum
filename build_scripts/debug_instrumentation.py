import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from matplotlib import ticker


def get_leader_data(leader: int):
    df = pd.read_json("logs/server-1.log")
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

def get_data(leader: int, followers: list[int]):
    (df, experiment_start) = get_leader_data(leader)
    for follower in followers:
        df_follower = pd.read_json(f"logs/server-{follower}.log")
        df_follower['net_receive'] = df_follower['net_receive'] - experiment_start
        df_follower['start_persist'] = df_follower['start_persist'] - experiment_start
        df_follower['send_accepted'] = df_follower['send_accepted'] - experiment_start
        if not (df['command_id'].equals(df_follower['command_id'])):
            raise ValueError(f"command_id column of follower {follower} does not match leader")
        df_follower.rename(columns=lambda x: f"follower_{follower}.{x}" if x != 'command_id' else x, inplace=True)
        df = pd.merge(df, df_follower, on="command_id", how="inner")
    return df

def debug_plot(idx_low: int, idx_high: int):
    leader = 1
    followers = [2,3,4,5]
    metronome_batch = 10
    assert metronome_batch <= 10, "more batch than colors"

    df = get_data(leader, followers)
    df = df.iloc[idx_low:idx_high]
    fig, ax = plt.subplots(figsize=(12, 6))
    y_positions = range(len(followers))
    print(df)

    # Loop through each follower and plot the time intervals
    for i, follower in enumerate(followers):
        # Extract the start and end times for the current follower
        start_key = f'follower_{follower}.start_persist';
        start_times = df[start_key]
        end_times = df[f'follower_{follower}.send_accepted']
        batched_persists = df.groupby(start_key)['command_id'].apply(list).reset_index()

        # Create horizontal bars for the intervals
        bar_containers = ax.barh(
            [i] * len(start_times),  # y position (height)
            end_times - start_times,  # width of the bars (duration)
            left=start_times,  # left position (start time)
            label=f'Follower {follower}',  # label for the legend
            edgecolor='black',  # outline color
            linewidth=1  # outline thickness
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
    ax.set_yticks(y_positions)
    ax.set_yticklabels([f'Follower {follower}' for follower in followers])

    # Set labels and title
    ax.set_xlabel('Experiment Time (ms)')
    ax.set_title('storage_delay=1000, jitter=0.01±1ms, metronome=false')
    # ax.set_title('storage_delay=1000, jitter=None, metronome=true')

    # Formatting the x-axis to show time
    # Define a custom function to divide x-axis tick labels by 1000
    def scale_x_tick_labels(value, pos):
        return f'{value / 1000:.1f}'

    # Apply FuncFormatter to x-axis
    ax.xaxis.set_major_formatter(ticker.FuncFormatter(scale_x_tick_labels))
    ax.set_xlim(df['net_receive'].min(), df['sending_response'].max())
    # ax.xaxis_date()  # Ensure x-axis is in date format
    plt.xticks(rotation=45)  # Rotate x labels for better readability

    # Show the plot
    plt.tight_layout()
    plt.show()


# def gen_colors_20():
#     c = plt.get_cmap("tab20")
#     reordered_colors = [
#         c(0), c(2), c(4), c(6), c(8),  c(10), c(12), c(14), c(16), c(18),
#         c(1), c(3), c(5), c(7), c(9),  c(11), c(13), c(15), c(17), c(19)
#     ]
#
# global_colors = gen_colors()

if __name__ == "__main__":
    global_colors = list(mcolors.TABLEAU_COLORS.values())
    debug_plot(0, 300)
