# Use an official Rust runtime as a parent image
FROM rust:1.74 AS build-stage

# Set the working directory in the container
WORKDIR /cipher/cipher-consensus

# Install any necessary dependencies
RUN apt-get update && \
    apt-get install -y clang libssl-dev make protobuf-compiler musl-dev vim && \
    rustup component add rustfmt && \
    apt-get update


COPY . /cipher/cipher-consensus

# Build the application
# RUN cargo build --release && \
# cp target/release/server target/release/cli /usr/local/bin/
RUN cargo install cargo-tree && cargo tree
RUN cargo clean && cargo build && \
    cp target/debug/server /usr/local/bin/


# Execute the script when the container starts
CMD ["sleep","infinity"]
