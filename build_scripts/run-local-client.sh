usage="Usage: run-local-client.sh <server_id_client_should_connect_to>"
[ -z "$1" ] &&  echo "No first_clients_server_id given! $usage" && exit 1

server_id=$1
client1_config_path="./client-${server_id}-config.toml"
local_experiment_dir="../benchmarks/logs/local-experiments"
mkdir -p "${local_experiment_dir}"
client1_log_path="${local_experiment_dir}/client-${server_id}.json"

RUST_LOG=info CONFIG_FILE="$client1_config_path"  cargo run --release --manifest-path="../Cargo.toml" --bin client 1> "$client1_log_path"
