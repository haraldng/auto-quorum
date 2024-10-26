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
    log_path="logs/server-${i}.log"
    # RUST_LOG=debug CONFIG_FILE="$config_path"  strace -c -o logs/server-${i}.strace -e trace=read,fsync cargo run --release --manifest-path="../omnipaxos_server/Cargo.toml" 1> "$log_path" &
    RUST_LOG=debug CONFIG_FILE="$config_path" cargo run --release --manifest-path="../omnipaxos_server/Cargo.toml" 1> "$log_path" &
done
wait

