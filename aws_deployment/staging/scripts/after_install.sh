#!/bin/bash

# Before Install 
cd /opt/cipher-mainnet-beta-core

all_containers_up=$(docker-compose ps -q | xargs docker inspect -f '{{.State.Status}}' | grep -v "running")

# Check if $all_containers_up is empty (i.e., all containers are running)
if [ -z "$all_containers_up" ]; then
    docker-compose up -d --build cipher-node
else
    docker-compose up -d
fi