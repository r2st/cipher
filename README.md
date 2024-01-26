# CIPHER-CONSENSUS


## Setup

## Install Diesel dependence to support postgres.

* [Diesel](https://diesel.rs/guides/getting-started) Make sure you have followed and installed.

```bash
apt install libpq-dev    # Postgres driver
apt install libssl-dev   # OpenSSL
```


### Install Cassandra on Debian

```bash
echo "deb https://debian.cassandra.apache.org 41x main" | sudo tee -a /etc/apt/sources.list.d/cassandra.sources.list

curl https://downloads.apache.org/cassandra/KEYS | sudo apt-key add -
sudo apt-get update

sudo apt-get install cassandra
sudo apt-get install protobuf

# If errors

sudo apt --fix-broken install
```

### Install Cassandra on Mac

- Make sure you are the instance of cassandra database with proper username and password

```bash
brew install cassandra
brew install protobuf
```

### Linux Debian

```bash
sudo apt install build-essential
sudo apt install clang curl git make
sudo apt install --assume-yes git clang curl libssl-dev protobuf-compiler
sudo apt install --assume-yes git clang curl libssl-dev llvm libudev-dev make protobuf-compiler
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env
rustup default stable
rustup update
rustup update nightly
rustup target add wasm32-unknown-unknown --toolchain nightly
```

## How to run cassandra

- using docker instance.
* Make sure you have updated `db_type = 0` in `config.toml` in root directory when running cassandra.

```bash
docker run  -e CASSANDRA_USER=cassandra  -e CASSANDRA_PASSWORD=cassandra -d --name cassandra-docker -p 9042:9042 cassandra
```

## How to run postgres

- using docker instance.

* Make sure you have updated `db_type = 1` in `config.toml` in root directory when running postgres.

```bash
docker run --name postgres -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 -d postgres
```

# Setting up config.toml file 

- Please refer to config.toml.example file in root directory and create config.toml file in root directory.
- Change the config.toml file according to your need. Use the correct hostnames for database types and change the port numbers if required
```bash

# Formatting

- Please use the following command to format your code before pushing to the repo.

```bash
cargo +nightly fmt
```

## How to run node

- init command which creates genesis.json and config.toml file in home_directory.

```bash
./target/debug/server init -w {path/foldername} 
```````

- start command which reads genesis.json and config.toml file from home_directory.

```bash
RUST_LOG=info ./target/debug/server start -w {path/foldername}
```
**Note:** -w is working directory where genesis nad config.toml is created, if not passed it consider default home dir as path. 

## RPC Ports

GRPC runs on 50052

JSON RPC runs on 50051

## How to run cli

Make sure to change `compiled_contract_file_full_path` in `config.json` to the full path of compiled contract file. It's in project root directory. From th Project root directory run the following command:

```bash
cargo run --bin cli submit-txn --payload-file-path cli/txn-payload/smart_contract_deployment.json
```

## How to run test case

```bash
cargo test -- --test-threads=1
```

## Using docker-compose.

### Project contains two docker-compose file,

- `docker-compose.yml` single instance with cassandra and l1-consensus.

- `docker-compose-cassandra-snitch` cassadnra uses the simple snitch with l1-consensus.

- `container` directory includes various Docker-compose files for the `postgres` database, featuring configurations for both multiple nodes and a single node.

```bash
docker-compose -f docker-compose.yml up -d
```
* Running the docker container with multiple node using postgres check folder `container`

```bash
docker-compose -f ./container/docker-compose-multiple-node.yaml up -d
```


## Code coverage

Run tests with code coverage, generate markdown report and check coverage threshold:

```sh
make setup_coverage test_with_coverage markdown_coverage_report check_markdown_coverage_threshold
```

Generate html report:

```sh
make setup_coverage test_with_coverage html_coverage_report
open coverage/html/index.html
```

## Dev Accounts
| Private Key | Public Key | Address | Info |
|----------|----------|----------|----------|
| 0x6d657bbe6f7604fb53bc22e0b5285d3e2ad17f64441b2dc19b648933850f9b46 | 0x0215edb7e9a64f9970c60d94b866b73686980d734874382ad1002700e5d870d945 | 0x75104938baa47c54a86004ef998cc76c2e616289 | Funded at genesis
| 0x6913aeae91daf21a8381b1af75272fe6fae8ec4a21110674815c8f0691e32758 | 0x02976d7877db9e40669feb13630396dade7e045cd268dd53e686647ff77a7e42a4 | 0x78e044394595d4984f66c1b19059bc14ecc24063 | Funded at genesis
| 0xf6b82b53ecbe1978b8651f740739b1d181f0285381e65e5e3491d8e821ab9bd0 | 0x027f60fb901bdbc58996e0800f0f7d01b5d9a010f80f075ede7e05585a87737674 | 0x7b7ab20f75b691e90c546e89e41aa23b0a821444 | Funded at genesis
| 0xbf7b645cad4c527189fe9bf59a8db74f28dd6f927637b19e4fe0fe60b1afc72f | 0x022002b8e853f6c64c2c20e15170d8604628d2b0d0de5e234766adc4cea07bdf2a | 0x4489da9d81f0bc8125c8efdda1c117a7a895b43d | Funded at genesis
| 0xd545e67bfab13d1ae4e2e8db9b65f7288dd57802d1c1377f2d4dc3959f63a72b | 0x03228f905047eb33dc4e3d7bb97a98ba632c7c638cede18e6007fc60d3b92c11eb | 0x3b647b46c9ba4fca221ccf933c09b653c2b4581f | Funded at genesis