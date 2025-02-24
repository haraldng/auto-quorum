#!/bin/bash

server_id=1
rust_log="info"

# Clean up child processes
interrupt() {
    pkill -P $$
}
trap "interrupt" SIGINT

# Clients' output is saved into logs dir
local_experiment_dir="./logs"
mkdir -p "${local_experiment_dir}"

# Run client
client_config_path="./client-1-config.toml"
RUST_LOG=$rust_log CONFIG_FILE="$client_config_path"  cargo run --manifest-path="../Cargo.toml" --bin client
