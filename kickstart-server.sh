#!/bin/bash

cargo build

random_folder="cipher"

mkdir -p cipher_data

mkdir -p "cipher_data/$random_folder"

./target/debug/server init -w "cipher_data/$random_folder"

RUST_LOG=info ./target/debug/server start -w "cipher_data/$random_folder"