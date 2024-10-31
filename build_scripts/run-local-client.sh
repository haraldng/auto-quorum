usage="Usage: run-local-client.sh first_clients_server_id second_clients_server_id"
[ -z "$1" ] &&  echo "No first_clients_server_id given! $usage" && exit 1

server_id=$1
client1_config_path="./client-${server_id}-config.toml"
run_id=$(date +%s%N)
mkdir -p "../logs/run-${run_id}"
client1_log_path="../logs/run-${run_id}/client-${server_id}.log"

RUST_LOG=warn CONFIG_FILE="$client1_config_path"  cargo run --release --manifest-path="../Cargo.toml" --bin client 1> "$client1_log_path"
debug
