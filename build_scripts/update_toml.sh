#!/bin/bash

usage="Usage: update-toml.sh <use_metronome_value> <storage_duration_micros_value>"
[ -z "$1" ] && echo "No use_metronome value provided! $usage" && exit 1
[ -z "$2" ] && echo "No storage_duration_micros value provided! $usage" && exit 1

use_metronome_value=$1
storage_duration_micros_value=$2

# Loop through each .toml file in the current directory
for file in ./*.toml; do
    # Check if the file exists
    if [[ -f $file ]]; then
        # Update the use_metronome field
        sed -i "s/^use_metronome = .*/use_metronome = $use_metronome_value/" "$file"

        # Update the storage_duration_micros field
        sed -i "s/^storage_duration_micros = .*/storage_duration_micros = $storage_duration_micros_value/" "$file"

        echo "Updated $file"
    else
        echo "No .toml files found in the current directory"
    fi
done
