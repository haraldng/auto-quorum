# Metronome
This repo can be used to build Metronome server and client binaries which communicate over TCP. The repo also contains benchmarking code which delploys Metronome servers and clients onto [GCP](https://cloud.google.com) instances and to run experiments collecting client response latencies (see `benchmarks/README.md`).

# Prerequisites
 - [Rust](https://www.rust-lang.org/tools/install)
 - [Docker](https://www.docker.com/)

# How to run
The `build_scripts` directory contains various utilities for configuring and running AutoQuorum clients and servers. Also contains examples of TOML file configuration.
 - `run-local-client.sh` runs a Metronome client in a local process. Its configuration, such as which server to connect to, is defined in TOML.
 - `run-local-cluster.sh` runs a 5 server cluster in separate local processes.
 - `docker-compose.yml` docker compose for a 5 server cluster and 1 client.
 - See `benchmarks/README.md` for benchmarking scripts 
