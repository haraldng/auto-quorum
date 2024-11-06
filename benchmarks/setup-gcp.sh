#!/usr/bin/env bash

# Initial steps:
#  - Create GCP project "my-project-1499979282244"
#  - Add service account "master", create key, save to "./master-service-account-key.json"
#  - Enable service usage API
#  - Get gcloud CLI

GREEN="\033[0;32m"
NO_COLOR="\033[0m"

project_id="my-project-1499979282244"
master_service_account_key_location=./master-service-account-key.json
deployment_service_account_id=deployment
deployment_service_account_key_location=./service-account-key.json
deployment_service_account_mail="${deployment_service_account_id}@${project_id}.iam.gserviceaccount.com"

printf "${GREEN}Setting up GCP project for${NO_COLOR}\n"
echo "==="
echo "project_id: ${project_id}"

printf "${GREEN}Activating master service account${NO_COLOR}\n"
gcloud auth activate-service-account --key-file="${master_service_account_key_location}" --project="${project_id}"

printf "${GREEN}Enabling APIs${NO_COLOR}\n"
gcloud services enable \
  artifactregistry.googleapis.com \
  compute.googleapis.com \
  iam.googleapis.com \
  storage.googleapis.com \
  cloudresourcemanager.googleapis.com \
  dns.googleapis.com
  # secretmanager.googleapis.com \

printf "${GREEN}Creating deployment service account with id '${deployment_service_account_id}'${NO_COLOR}\n"
gcloud iam service-accounts create "${deployment_service_account_id}" \
  --description="Used for the deployment application" \
  --display-name="Deployment Account"

printf "${GREEN}Creating JSON key file for deployment service account at ${deployment_service_account_key_location}${NO_COLOR}\n"
sleep(3) # Wait a bit to make sure deployment service account was initialized
gcloud iam service-accounts keys create "${deployment_service_account_key_location}" \
  --iam-account="${deployment_service_account_mail}"

printf "${GREEN}Adding roles for service account${NO_COLOR}\n"  
roles="artifactregistry.createOnPushRepoAdmin storage.admin compute.admin dns.admin iam.serviceAccountUser iam.serviceAccountTokenCreator iap.tunnelResourceAccessor"

for role in $roles; do
  gcloud projects add-iam-policy-binding "${project_id}" --member=serviceAccount:"${deployment_service_account_mail}" "--role=roles/${role}"
done;

printf "${GREEN}Creating firewall rules${NO_COLOR}\n"
gcloud compute firewall-rules create default-allow-http --allow tcp:80 --target-tags=http-server
gcloud compute firewall-rules create omnipaxos-server --allow tcp:8000-8010 --target-tags=omnipaxos-server

printf "${GREEN}Creating private DNS zone for VPC${NO_COLOR}\n"
gcloud dns managed-zones create internal-network \
        --dns-name=internal.zone. \
        --visibility=private \
        --description="Private DNS zone for VPC" \
        --networks=default
