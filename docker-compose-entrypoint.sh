#!/bin/bash

docker-compose -f docker-compose-cassandra-snitch.yml up -d cassandra1 cassandra2

echo "Waiting for 3 minutes before starting cipher-node..."
sleep 60

docker-compose -f docker-compose-cassandra-snitch.yml up -d  --build cipher-node

echo "All containers are up!"