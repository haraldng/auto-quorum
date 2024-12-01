# Metronome Benchmarks
The most applicable aspect of this project to other projects is the `GcpCluster` defined in `gcp_cluster.py`. It provides a class for starting, stopping, and SSH-ing a cluster of GCP instances. It assumes instances communicate via Google's VPC, and assumes the existence of an internal network named `internal.zone.`. Instance settings can be configured, see `InstanceConfig`, however instances are hardcoded to use the container-optimized cos family OS. Note that there is a delay between when an instance is created and when it becomes SSH-able, MetronomeCluster solves this with a retry mechanism.

Documentation on the GCP python client API seems to be scarce. The best resource I've found are the samples (here)[https://github.com/GoogleCloudPlatform/python-docs-samples/tree/main/compute].
## Project Structure
 - `setup-gcp.sh` Initial GCP setup. Only necessary if you are starting a new GCP project.
 - `gcp_cluster.py` Manage cluster of GCP instances. 
 - `metronome_cluster.py` Manage Metronome GCP cluster.
 - `metronome_configs.py` Defines configs for Metronome.
 - `benchmarks.py` Run Metronome benchmarks, results saved to `logs/`
 - `graph_experiment.py` Graph benchmark data in `logs/`
## Dependencies
 - (gcloud)[https://cloud.google.com/sdk/gcloud] CLI tool for interacting with GCP
 - (uv)[https://docs.astral.sh/uv/] a Python project/package manager.
## To Run
 1. Have an owner of the GCP project add you to the project and configure your permissions. Or start your own project with the help of `setup-gcp.sh`
 2. Run the commands in `../build_scripts/auth.sh` to configure your gcloud credentials
 3. Run python code with `uv run <python-file-here>`.
 4. Run `uv run benchmarks.py` to run a Metronome benchmark. Run `uv run graph_experiment.py` to graph benchmark data.
