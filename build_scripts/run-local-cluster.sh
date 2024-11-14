usage="Usage: run-local-cluster.sh cluster_size optimize_setting"
[ -z "$1" ] &&  echo "No cluster_size given! $usage" && exit 1
cluster_size=$1

interrupt() {
    pkill -P $$
}
trap "interrupt" SIGINT
trap "interrupt" EXIT

local_experiment_dir="../benchmarks/logs/local-experiments"
mkdir -p "${local_experiment_dir}"
for ((i = 1; i <= cluster_size; i++)); do
    config_path="./server-${i}-config.toml"
    # err_path="${local_experiment_dir}/xerr-server-${i}.log"
    RUST_LOG=warn CONFIG_FILE="$config_path" cargo run --release --manifest-path="../Cargo.toml" --bin server &
done
wait

