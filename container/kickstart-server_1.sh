#!/bin/bash

cargo build

# Generate a random folder name starting with 'cipher' followed by a random number
# random_folder="cipher_$RANDOM"
random_folder="cipher1"


# Create the base directory if it doesn't exist
mkdir -p cipher_data

# Create a new directory for the random folder inside cipher_data
mkdir -p "cipher_data/$random_folder"

# Run the server init command with the new directory inside cipher_data
RUST_LOG=info /cipher/cipher-consensus/target/debug/server init -w "/cipher/cipher-consensus/cipher_data/$random_folder"

# Run the server start command with the same directory
RUST_LOG=info /cipher/cipher-consensus/target/debug/server start -w "/cipher/cipher-consensus/cipher_data/$random_folder"