usage="Usage: run-local-client.sh first_clients_server_id second_clients_server_id"
[ -z "$1" ] &&  echo "No first_clients_server_id given! $usage" && exit 1
#[ -z "$2" ] &&  echo "No second_clients_server_id given! $usage" && exit 1

server_id=$1
#other_id=$2
client1_config_path="./client-${server_id}-config.toml"
#client2_config_path="./client-${other_id}-config.toml"

# Log paths previously used to graph latencies in the benchmark repository. No graph function exists currently for local data.
#if [ -z "$3" ]; then
#    client1_log_path="../../auto-quorum-benchmark/logs/test-local_client-${server_id}.log"
#    client2_log_path="../../auto-quorum-benchmark/logs/test-local_client-${other_id}.log"
#else
#    client1_log_path="../../auto-quorum-benchmark/logs/test-local-no-reconfig_client-${server_id}.log"
#    client2_log_path="../../auto-quorum-benchmark/logs/test-local-no-reconfig_client-${other_id}.log"
#fi

run_id=$(date +%s%N)

mkdir "../logs/run-${run_id}"
client1_log_path="../logs/run-${run_id}/client-${server_id}.log"
#client2_log_path="../logs/run-${run_id}/client-${other_id}.log"

#sync_buffer=1000
#utc_sync=$(($(date +%s%3N) + sync_buffer))

interrupt() {
    pkill -P $$
    cleanup
}
cleanup() {
    # Restore SCHEDULE placeholder in TOML config
    sed -i "s/$utc_sync/SCHEDULE/g" "$client1_config_path"
    sed -i "s/$utc_sync/SCHEDULE/g" "$client2_config_path"
}
#trap "interrupt" SIGINT
#trap "cleanup" EXIT

# Replace SCHEDULE placeholder in TOML config with unified start time
# Ensures that the clients' event loops start at the same time despite the executables starting at different times.
#sed -i "s/SCHEDULE/$utc_sync/g" "$client1_config_path" &&
#sed -i "s/SCHEDULE/$utc_sync/g" "$client2_config_path" &&

CONFIG_FILE="$client1_config_path"  cargo run --release --manifest-path="../omnipaxos_client/Cargo.toml" 1> "$client1_log_path"
#CONFIG_FILE="$client2_config_path"  cargo run --manifest-path="../omnipaxos_client/Cargo.toml" 1> "$client2_log_path"

