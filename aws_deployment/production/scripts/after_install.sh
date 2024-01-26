#!/bin/bash

# Before Install 
cd /opt/app

cp aws_deployment/production/environment.cred .env

# all_containers_up=$(docker-compose ps -q | xargs docker inspect -f '{{.State.Status}}' | grep -v "running")

# # Check if $all_containers_up is empty (i.e., all containers are running)
# if [ -z "$all_containers_up" ]; then
#     docker-compose up -d --build cipher-node
# else
#     docker-compose up -d
# fi

# Start cassandra1 and cassandra2 containers using the specified YAML file
docker-compose -f docker-compose-cassandra-snitch.yml up -d cassandra1 cassandra2

# Add a delay of 2 minutes (120 seconds)
echo "Waiting for 3 minutes before starting cipher-node..."
sleep 180

# Start cipher-node container using the specified YAML file
docker-compose -f docker-compose-cassandra-snitch.yml up -d cipher-node --build cipher-node

echo "All containers are up!"