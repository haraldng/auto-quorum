#!/bin/bash
set -eu

usage="Usage: run-local-cluster.sh"
cluster_size=5
rust_log="info"

# Clean up child processes
interrupt() {
    pkill -P $$
}
trap "interrupt" SIGINT

# Servers' output is saved into logs dir
local_experiment_dir="./logs"
mkdir -p "${local_experiment_dir}"

# Run servers
cluster_config_path="./cluster-config.toml"
for ((i = 1; i <= cluster_size; i++)); do
    server_config_path="./server-${i}-config.toml"
    err_path="${local_experiment_dir}/xerr-server-${i}.log"
    # RUST_LOG=info CONFIG_FILE="$config_path" cargo run --manifest-path="../Cargo.toml" --bin server 2> ${err_path} &
    RUST_LOG=$rust_log SERVER_CONFIG_FILE=$server_config_path CLUSTER_CONFIG_FILE=$cluster_config_path cargo run --manifest-path="../Cargo.toml" --bin server &
done
wait

