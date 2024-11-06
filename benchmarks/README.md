# auto-quorum-benchmark
The most applicable aspect of this project to other projects is the `GcpCluster` defined in `gcp_cluster.py`. It provides a class for starting, stopping, and SSH-ing a cluster of GCP instances. It assumes instances communicate via Google's VPC, and assumes the existence of an internal network named `internal.zone.`. SSHing uses IAP tunneling which can take a good 60sec to until a new instance is registered and therefore SSH-able. Refactoring away from IAP and SSHing directly to instances should fix this. Instance settings can be configured, see `InstanceConfig`, however instances are hardcoded to use the container-optimized cos family OS. 

Documentation on the GCP python client API seems to be scarce. The best resource I've found are the samples (here)[https://github.com/GoogleCloudPlatform/python-docs-samples/tree/main/compute].
## Project Structure
 - `setup-gcp.sh` Initial GCP setup. Complete initial steps at top of file first.
 - `gcp_cluster.py` Manage cluster of GCP instances. 
 - `autoquorum_cluster.py` Manage AutoQuorum GCP cluster. 
 - `benchmarks.py` Run AutoQuorum benchmarks, results saved to `logs/`
 - `graph_experiment.py` Graph benchmark data in `logs/`
## To Run
 1. Complete initial steps to start the GCP project listed at the top of `setup-gcp.sh`.
 2. Run `setup-gcp.sh` to setup nework and deployment service account.
 3. Run `benchmarks.py` to run a AutoQuorum bencharmk, results saved to `logs/`
 4. Run `graph_experiment.py` to graph benchmark data in `logs/`
