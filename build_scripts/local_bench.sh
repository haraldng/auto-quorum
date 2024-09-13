#!/bin/bash

usage="Usage: run-multiple-clients.sh N"
[ -z "$1" ] && echo "No N given! $usage" && exit 1

N=$1

for ((i=0; i<N; i++))
do
    ./run-local-client.sh 1
    sleep 0.1  # 100ms delay
done
