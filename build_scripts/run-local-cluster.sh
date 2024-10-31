usage="Usage: run-local-cluster.sh cluster_size optimize_setting"
[ -z "$1" ] &&  echo "No cluster_size given! $usage" && exit 1
cluster_size=$1

interrupt() {
    pkill -P $$
}
trap "interrupt" SIGINT
trap "interrupt" EXIT

for ((i = 1; i <= cluster_size; i++)); do
    config_path="./server-${i}-config.toml"
    log_path="logs/server-${i}.log"
    # RUST_LOG=debug CONFIG_FILE="$config_path"  strace -c -o logs/server-${i}.strace -e trace=read,fsync cargo run --release --manifest-path="../omnipaxos_server/Cargo.toml" 1> "$log_path" &
    RUST_LOG=debug CONFIG_FILE="$config_path" cargo run --release --manifest-path="../Cargo.toml" --bin server 1> "$log_path" &
done
wait

