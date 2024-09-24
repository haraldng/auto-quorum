usage="Usage: run-local-cluster.sh cluster_size optimize_setting"
[ -z "$1" ] &&  echo "No cluster_size given! $usage" && exit 1
cluster_size=$1

interrupt() {
    pkill -P $$
    cleanup
}
cleanup() {
    for ((i = 1; i <= cluster_size; i++)); do
        config_path="./server-${i}-config.toml"
        # sed -i "s/optimize = true/optimize = OPTIMIZE/g" "$config_path"
        # sed -i "s/optimize = false/optimize = OPTIMIZE/g" "$config_path"
    done
    # sudo tc qdisc del dev lo root
}
trap "interrupt" SIGINT
trap "cleanup" EXIT

#sudo tc qdisc add dev lo root netem delay 1msec
for ((i = 1; i <= cluster_size; i++)); do
    config_path="./server-${i}-config.toml"
    # Set log path and optimize setting in TOML config file depending on arg 2
    if [ -z "$2" ]; then
        #sed -i "s/OPTIMIZE/true/g" "$config_path" &&
        log_path="../../auto-quorum-benchmark/logs/test-local_server-${i}.log"
    else
        #sed -i "s/OPTIMIZE/false/g" "$config_path" &&
        log_path="../../auto-quorum-benchmark/logs/test-local-no-reconfig_server-${i}.log"
    fi
    log_path="/dev/null"
    RUST_LOG=error CONFIG_FILE="$config_path" cargo run --release --manifest-path="../omnipaxos_server/Cargo.toml" 1> "$log_path" &
done
wait

