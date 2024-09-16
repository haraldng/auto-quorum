#!/usr/bin/env bash
set -eux

GREEN="\033[0;32m"
NO_COLOR="\033[0m"

project_id=my-project-1499979282244
image_name="gcr.io/${project_id}/metronome_server"
# TODO: generalize absolute path
deployment_service_account_key_location=~/rise/harald_project/metronome-benchmark/service-account-key.json

printf "${GREEN}Building server docker image with name '${image_name}'${NO_COLOR}\n"
sudo docker build -t "${image_name}" -f  ./../omnipaxos_server/Dockerfile ./..

printf "${GREEN}Authenticating docker${NO_COLOR}\n"
cat "${deployment_service_account_key_location}" | sudo docker login -u _json_key --password-stdin https://gcr.io

printf "${GREEN}Pushing '${image_name}' to registry${NO_COLOR}\n"
sudo docker push "${image_name}"

printf "\n\n${GREEN}Done!${NO_COLOR}\n"
