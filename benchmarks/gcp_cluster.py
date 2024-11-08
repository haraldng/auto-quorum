from pathlib import Path
from typing import Optional
import subprocess

from google.oauth2 import service_account
# import google.auth.transport.requests
from google.cloud import compute_v1
from google.cloud import dns
from google.cloud.compute_v1 import DeleteInstanceRequest, InsertInstanceRequest, types
from google.api_core.extended_operation import ExtendedOperation

class InstanceConfig:
    def __init__(self, name: str, zone: str, machine_type: str, startup_script: str, firewall_tag: Optional[str]=None, dns_name: Optional[str]=None) -> None:
        self.name = name
        self.zone = zone
        self.machine_type = machine_type
        self.startup_script = startup_script
        self.firewall_tag = firewall_tag
        self.dns_name = dns_name
        self._recreate = False

    def matches_instance(self, instance: compute_v1.Instance) -> bool:
        if instance.name != self.name:
            return False
        if instance.zone != self.zone:
            return False
        if instance.machine_type != self.machine_type:
            return False
        # TODO: detect tag changes
        # if self.firewall_tag is not None and not self.firewall_tag in instance.tags:
        #     return False
        # TODO: detect change in dns name?
        return True

# The best resource I found for the GCP python client API are the samples here: https://github.com/GoogleCloudPlatform/python-docs-samples/tree/main/compute
# Manages a cluster of GCP instances.
# Assumes a VPC network internal.zone. is created
# Each instance is assigned a DNS name `{instance_dns_name}.internal.zone.` to be used in the internal VPC
class GcpCluster:
    missing_dns_msg = """Managed zone has to be created first
Run: gcloud dns managed-zones create internal-network \\
        --dns-name=internal.zone. \\
        --visibility=private \\
        --description="Private DNS zone for VPC" \\
        --networks=default"""

    def __init__(self, project_id: str, credentials_file: str, instance_configs: list[InstanceConfig]) -> None:
        new_instance_configs = {config.name: config for config in instance_configs}
        assert len(instance_configs) == len(new_instance_configs), "Instances must have unique names"

        print("Authenticating...")
        credentials = service_account.Credentials.from_service_account_file(
                credentials_file,
                scopes=['https://www.googleapis.com/auth/cloud-platform']
                )
        self.project_id = project_id
        self.service_account_key = credentials_file
        self.service_account_mail = credentials.service_account_email
        self.instances_client = compute_v1.InstancesClient(credentials=credentials)
        self.dns_client = None # Only create DNS client when it need to be used

        # TODO: Possible for a running instance to not have been assigned a DNS address
        # Get already running instances
        self.instances = self._get_running_instances()
        print(f"Running instances: {list(self.instances.keys())}")
        # Identify new/changed instances
        instances_to_create = dict()
        for name, instance_config in new_instance_configs.items():
            running_instance = self.instances.get(name)
            if running_instance is not None:
                if not instance_config.matches_instance(running_instance):
                    instances_to_create[name] = instance_config
                    instances_to_create[name]._recreate = True
            else:
                instances_to_create[name] = instance_config
        new_instance_names = list(instances_to_create.keys())
        print(f"Instances to create/modify: {new_instance_names}")
        # Identify unused instances
        unused_instances = set(self.instances.keys()) - set(new_instance_configs.keys())
        if len(unused_instances) > 0:
            print(f"WARNING the following instances are unused: {unused_instances}")

        self._create_instances(instances_to_create)

    # TODO: Check if ssh without IAP tunnel (os login?) can be started faster
    # NOTE: It can take a good 60sec before IAP registers newly started instance
    def ssh_command(self, instance_name: str, command: str) -> subprocess.Popen:
        instance = self.instances[instance_name]
        name = instance.name
        zone = instance.zone
        gcloud_command = f"gcloud compute ssh {name} --zone={zone} --tunnel-through-iap --project={self.project_id} --command=\"{command}\""
        p = subprocess.Popen(gcloud_command, shell=True)
        return p

    # TODO: Check if ssh without IAP tunnel (os login?) can be started faster
    # NOTE: It can take a good 60sec before IAP registers newly started instance
    def scp_command(self, instance_name: str, src_dir: str, dest_dir: Path) -> subprocess.Popen:
        instance = self.instances[instance_name]
        name = instance.name
        zone = instance.zone
        gcloud_command = f"gcloud compute scp --zone={zone} --tunnel-through-iap --project={self.project_id} --compress {name}:{src_dir}/* {dest_dir}"
        p = subprocess.Popen(gcloud_command, shell=True)
        return p

    # Shutdown all currently running instances
    def shutdown(self):
        self.shutdown_instances(list(self.instances.keys()))

    def shutdown_instances(self, instances_to_shutdown: list[str]):
        # Send delete instance requests
        shutdown_operations = []
        requests = 0
        total_req = len(instances_to_shutdown)
        request_prefix = "Requesting Instance Deletions"
        deleted_prefix = "Deleting Instances           "
        for name in instances_to_shutdown:
            instance = self.instances.get(name)
            if instance is None:
                print(f"Instance {name} can't be shutdown as it doesn't exist")
                continue
            print_progress_bar(requests, total_req, prefix=request_prefix)
            requests += 1
            delete_request = DeleteInstanceRequest()
            delete_request.project = self.project_id
            delete_request.instance = instance.name
            delete_request.zone = instance.zone
            shutdown_operation = self.instances_client.delete(delete_request)
            shutdown_operations.append(shutdown_operation)
        print_progress_bar(requests, total_req, prefix=request_prefix)

        # Wait for responses to delete requests
        total_shutdowns = 0
        for operation in shutdown_operations:
            print_progress_bar(total_shutdowns, total_req, prefix=deleted_prefix)
            wait_for_extended_operation(operation)
            total_shutdowns += 1
        print_progress_bar(total_shutdowns, total_req, prefix=deleted_prefix)

        for name in instances_to_shutdown:
            self.instances.pop(name, None)

    def _create_instances(self, new_instance_configs: dict[str, InstanceConfig]) -> None:
        # Delete instances which are to be recreated
        self.shutdown_instances([config.name for config in new_instance_configs.values() if config._recreate])

        # Send create instance requests
        create_instance_operations = []
        requests = 0
        total_req = len(new_instance_configs)
        for config in new_instance_configs.values():
            print_progress_bar(requests, total_req, prefix="Requesting Instances")
            create_instance_request = self._create_instance_request(config)
            pending_response = self.instances_client.insert(request=create_instance_request)
            create_instance_operations.append(pending_response)
            requests += 1
        print_progress_bar(requests, total_req, prefix="Requesting Instances")

        # Wait for responses to create requests
        created_instances = 0
        for operation in create_instance_operations:
            print_progress_bar(created_instances, total_req, prefix="Creating Instances  ")
            wait_for_extended_operation(operation)
            created_instances += 1
        print_progress_bar(created_instances, total_req, prefix="Creating Instances  ")

        # Update self.instances and DNS
        self.instances = self._get_running_instances()
        for config in new_instance_configs.values():
            if config.dns_name is not None:
                instance_ip = self.instances[config.name].network_interfaces[0].network_i_p
                self._add_instance_record_set(config.dns_name, instance_ip)

    def _create_instance_request(self, instance_config: InstanceConfig) -> InsertInstanceRequest:
        os_disk = types.AttachedDisk(
            device_name=instance_config.name,
            boot=True,
            auto_delete=True,
            initialize_params=types.AttachedDiskInitializeParams(
                disk_size_gb=10,
                disk_type=f"zones/{instance_config.zone}/diskTypes/pd-balanced",
                # Minimal OS optimized for running Docker containers
                source_image="projects/cos-cloud/global/images/family/cos-stable",
                # source_image="projects/ubuntu-os-cloud/global/images/family/ubuntu-2004-lts",
                ),
            )
        network_interface = types.NetworkInterface(
            access_configs=[
                types.AccessConfig(
                    name="External NAT",
                    network_tier="PREMIUM",
                    ),
                ],
            )
        metadata = types.Metadata(
            # Create with startup script
            items=[{
                "key":"startup-script",
                "value":instance_config.startup_script,
                }]
            # # Create as a kubernetes cluster
            # items=[{
            #     "key":"gce-container-declaration",
            #     "value": startup_script,
            #     }],
            )
        tags = types.Tags()
        if instance_config.firewall_tag:
            tags.items = [instance_config.firewall_tag]
        # scheduling = types.Scheduling(
        #         provisioning_model=compute_v1.Scheduling.ProvisioningModel.SPOT.name,
        #         instance_termination_action="STOP"
        #         )
        os_service_account = types.ServiceAccount(
            email=self.service_account_mail,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
            )
        request = types.InsertInstanceRequest(
                project=self.project_id,
                zone=instance_config.zone,
                instance_resource=types.Instance(
                    name=instance_config.name,
                    machine_type=f"zones/{instance_config.zone}/machineTypes/{instance_config.machine_type}",
                    tags=tags,
                    # scheduling=scheduling,
                    disks=[os_disk],
                    network_interfaces=[network_interface],
                    metadata=metadata,
                    service_accounts=[os_service_account],
                    ),
                )
        return request

    def _get_running_instances(self) -> dict[str, compute_v1.Instance]:
        instances = dict()
        list_instances_request = compute_v1.AggregatedListInstancesRequest() 
        list_instances_request.project = self.project_id
        agg_list = self.instances_client.aggregated_list(request=list_instances_request)
        for zone, response in agg_list:
            for instance in response.instances:
                assert instances.get(instance.name) is None, f"Cluster misconfigured: two instances have the same name {instance.name}"
                instance.zone = instance.zone.split("/")[-1]
                instance.machine_type = instance.machine_type.split("/")[-1]
                instances[instance.name] = instance
        return instances

    # def _get_instance_ip(self, instance_name: str) -> str:
    #     get_server_request = compute_v1.GetInstanceRequest()
    #     get_server_request.project = self.project_id
    #     get_server_request.zone = self.instance_configs[instance_name].zone
    #     get_server_request.instance = self.instance_configs[instance_name].name
    #     server = self.instances_client.get(request=get_server_request)
    #     server_ip = server.network_interfaces[0].network_i_p
    #     return server_ip

    # TODO: pass DNS domain / name through configs in a more general way
    def _add_instance_record_set(self, dns_name: str, server_ip: str) -> None:
        if self.dns_client is None:
            self._create_dns_client()
        dns_name = f"{dns_name}.internal.zone."
        changes = self.managed_zone.changes()
        record_set = self.managed_zone.list_resource_record_sets(client=self.dns_client)
        for resource_record_set in record_set:
            if resource_record_set.name == dns_name:
                if resource_record_set.rrdatas == [server_ip]:
                    print(f"Prev record is valid: {resource_record_set.name}: {resource_record_set.rrdatas}")
                    return
                else:
                    changes.delete_record_set(resource_record_set)

        print(f"Adding DNS record {dns_name}: {server_ip}")
        add_record = self.managed_zone.resource_record_set(
            name=dns_name,
            record_type="A",
            ttl=3000,
            rrdatas=[server_ip],
        )
        changes.add_record_set(add_record)
        changes.create(client=self.dns_client)

    def _create_dns_client(self) -> None:
        credentials = service_account.Credentials.from_service_account_file(
                self.service_account_key,
                scopes=['https://www.googleapis.com/auth/cloud-platform']
                )
        self.dns_client = dns.Client(project=self.project_id, credentials=credentials)
        self.managed_zone = self.dns_client.zone(
            name="internal-network",
            dns_name="internal.zone.",
            description="Internal network for benchmark",
        )
        if not self.managed_zone.exists(client=self.dns_client):
            # Its possible to create the zone with the python API, 
            # but not to link it to a network.
            raise MissingDNSZoneError(self.missing_dns_msg)

def print_progress_bar(iteration, total, prefix='', suffix='', length=50, fill='█'):
    if total == 0:
        return
    percent =f"{iteration}/{total}"
    # percent = ("{0:.1f}%").format(100 * (iteration / float(total)))
    filled_length = int(length * iteration // total)
    bar = fill * filled_length + '-' * (length - filled_length)
    print(f'\r{prefix} |{bar}| {percent} {suffix}', end='\r')
    if iteration == total:
        print()

def wait_for_extended_operation(
    operation: ExtendedOperation, verbose_name: str = "operation", timeout: int = 300):
    result = operation.result(timeout=timeout)
    if operation.error_code:
        print(f" Error during {verbose_name}: [Code: {operation.error_code}]: {operation.error_message}")
        print(f" Operation ID: {operation.name}")
        raise operation.exception() or RuntimeError(operation.error_message)
    if operation.warnings:
        print(f" Warnings during {verbose_name}:\n")
        for warning in operation.warnings:
            print(f" - {warning.code}: {warning.message}")
    return result

class MissingDNSZoneError(Exception):
    pass

