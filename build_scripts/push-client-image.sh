#!/usr/bin/env bash
set -eu

println_green() {
    printf "\033[0;32m$1\033[0m\n"
}

project_id=my-project-1499979282244
image_name="gcr.io/${project_id}/metronome_client"

println_green "Building client docker image with name '${image_name}'"
docker build -t "${image_name}" -f  ../client.dockerfile ../

println_green "Pushing '${image_name}' to registry"
docker push "${image_name}"

println_green "Done!"
