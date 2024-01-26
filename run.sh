#!/bin/bash
# echo "Setting environment variables"
# # source .env
echo "Sleeping for 5 minutes"

/usr/local/bin/server init -w "cipher-data/cipher"
# sleep 300

echo "Starting server"
RUST_LOG=info /usr/local/bin/server start -w "cipher-data/cipher"
