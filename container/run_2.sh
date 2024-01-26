#!/bin/bash
echo "Setting environment variables"
source .env

# echo "Initializing server"
# /usr/local/bin/server init -w random_fold"cipher_data1"

echo "Sleeping for 5 minutes"
sleep 300

echo "Starting server"
RUST_LOG=info /usr/local/bin/server start -w "cipher-data/cipher2"