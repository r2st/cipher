#!/bin/bash
export NODE_PRIVKEY=6913aeae91daf21a8381b1af75272fe6fae8ec4a21110674815c8f0691e32758
RUST_LOG=info cargo run --bin server -- --dev
