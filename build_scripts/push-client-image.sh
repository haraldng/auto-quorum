#!/usr/bin/env bash
set -eu

GREEN="\033[0;32m"
NO_COLOR="\033[0m"

project_id=my-project-1499979282244
image_name="gcr.io/${project_id}/metronome_client"
deployment_service_account_key_location=../service-account-key.json

printf "${GREEN}Building client docker image with name '${image_name}'${NO_COLOR}\n"
sudo docker build -t "${image_name}" -f  ../client.dockerfile ../

printf "${GREEN}Authenticating docker${NO_COLOR}\n"
cat "${deployment_service_account_key_location}" | sudo docker login -u _json_key --password-stdin https://gcr.io

printf "${GREEN}Pushing '${image_name}' to registry${NO_COLOR}\n"
sudo docker push "${image_name}"

printf "\n\n${GREEN}Done!${NO_COLOR}\n"
