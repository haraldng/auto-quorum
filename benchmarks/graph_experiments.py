from graphs.increasing_clients_graphs import increasing_clients_graph
from graphs.local_run_graphs import local_debug_plots
from graphs.metronome_size_graphs import (
    metronome_size_bar_chart,
    metronome_size_debug_plots,
    metronome_size_heatmap,
    metronome_size_violin_plot,
)
from graphs.new_straggler_graphs import new_straggler_plot
from graphs.straggler_graphs import (
    straggler_bar_chart,
    straggler_debug_plots,
    straggler_violin_plot,
)

# data_size = 1024
# metronome_size_heatmap(data_size, relative=False)
# metronome_size_heatmap(data_size, relative=True)
# for cluster_size in [3, 5, 7]:
#     metronome_size_bar_chart(data_size, cluster_size)
#     metronome_size_violin_plot(data_size, cluster_size, 100_000, 100_000)
#     metronome_size_debug_plots(data_size, cluster_size, 100_000, 100_000)

# straggler_bar_chart()
# straggler_violin_plot(300_000, None)
# straggler_debug_plots(300_000, None)

new_straggler_plot()

# increasing_clients_graph()

# local_debug_plots(None, 1)
