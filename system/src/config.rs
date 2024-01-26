use crate::account::Account;
use anyhow::{anyhow, Error};
use primitives::{DbType as DbTypei8, MemPoolSize};
use regex::Regex;
use secp256k1::{Secp256k1, SecretKey};
use serde::{Deserialize, Serialize};
use std::{fmt, fs::read_to_string, path::PathBuf};

/// Startup configuration for running an CIPHER node
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Config {
	pub dev_mode: bool,
	pub multinode_mode: bool,
	pub whitelist_check: bool,
	pub replication_enable: bool,
	pub max_size: MemPoolSize,
	pub fee_limit: u64,
	pub rate_limit: usize,
	pub time_frame_seconds: u64,
	pub expiration_seconds: u64,
	pub block_time: u64,
	pub node_port: String,
	pub node_ip_address: String, // eg. "/ip4/0.0.0.0/tcp/5010"
	pub node_private_key: String,
	pub node_verifying_key: String,
	pub node_address: String,
	pub node_metadata: String,
	pub cluster_address: String,
	pub validator_pool_address: String,
	pub boot_nodes: Vec<String>, // eg. &["/ip4/0.0.0.0/tcp/5010/p2p/1234567890"]
	pub grpc_port: String,
	pub jsonrpc_port: String,
	pub db_type: DbTypei8,
	pub cassandra_host: String,
	pub cassandra_username: String,
	pub cassandra_password: String,
	pub cassandra_keyspace: String,
	pub postgres_host: String,
	pub postgres_username: String,
	pub postgres_password: String,
	pub postgres_pool_size: u32,
	pub postgres_db_name: String,
	pub postgres_test_db_name: String,
	pub rocksdb_name: String,
}

impl Default for Config {
	fn default() -> Self {
		let default_config = Self::default_config().expect("Error reading default_config");
		let node_private_key = default_config.node_private_key;
		let secret_key = SecretKey::from_slice(
			&hex::decode(node_private_key.clone()).expect("Error decoding node_private_key"),
		)
		.expect("Failed to parse provided private_key");
		let secp = Secp256k1::new();
		let verifying_key = secret_key.public_key(&secp);
		let verifying_key_bytes = verifying_key.serialize().to_vec();
		let node_address =
			Account::address(&verifying_key_bytes.clone()).expect("Failed to get node address");
		Self {
			whitelist_check: default_config.whitelist_check,
			replication_enable: default_config.replication_enable,
			dev_mode: default_config.dev_mode,
			multinode_mode: default_config.multinode_mode,
			max_size: default_config.max_size,
			fee_limit: default_config.fee_limit,
			rate_limit: default_config.rate_limit,
			time_frame_seconds: default_config.time_frame_seconds,
			expiration_seconds: default_config.expiration_seconds,
			block_time: default_config.block_time,
			node_port: default_config.node_port,
			node_ip_address: default_config.node_ip_address,
			node_private_key,
			node_verifying_key: hex::encode(verifying_key_bytes).to_string(),
			node_address: hex::encode(node_address).to_string(),
			node_metadata: default_config.node_metadata,
			cluster_address: default_config.cluster_address,
			boot_nodes: default_config.boot_nodes,
			grpc_port: default_config.grpc_port,
			jsonrpc_port: default_config.jsonrpc_port,
			db_type: default_config.db_type,
			validator_pool_address: default_config.validator_pool_address,
			cassandra_host: default_config.cassandra_host,
			cassandra_username: default_config.cassandra_username,
			cassandra_password: default_config.cassandra_password,
			cassandra_keyspace: default_config.cassandra_keyspace,
			postgres_host: default_config.postgres_host,
			postgres_username: default_config.postgres_username,
			postgres_password: default_config.postgres_password,
			postgres_db_name: default_config.postgres_db_name,
			postgres_pool_size: default_config.postgres_pool_size,
			rocksdb_name: default_config.rocksdb_name,
			postgres_test_db_name: default_config.postgres_test_db_name,
		}
	}
}

impl Config {
	fn default_config() -> Result<Config, Error> {
		// Get the current directory
		let current_dir = std::env::current_dir().expect("Failed to get current directory");
		//println!("Working directory: {:?}", current_dir);

		// Define the regular expression pattern to match "cipher-consensus" or its subdirectories
		let pattern = Regex::new(r"^(.*/cipher-consensus)(/.*)?$").expect("Invalid regex pattern");

		// Check if the current directory matches the pattern
		let current_dir =
			if let Some(captures) = pattern.captures(current_dir.to_str().expect("Invalid path")) {
				if let Some(parent_dir) = captures.get(1) {
					// If there's a subdirectory, append it to the path
					let mut new_path = PathBuf::new();
					new_path.push(parent_dir.as_str());
					//println!("Trimmed path: {:?}", new_path);
					new_path
				} else {
					// If there's no subdirectory, keep the path as is
					//println!("Current directory matches: {:?}", current_dir);
					current_dir
				}
			} else {
				// If the current directory doesn't match, print a message
				//println!("Current directory does not match the pattern: {:?}", current_dir);
				current_dir
			};

		// Construct path to config.toml and genesis.json
		let mut config_path = current_dir.clone();
		config_path.push("config.toml");
		//println!("config_path: {:?}", config_path);
		// Read and parse config.toml
		match read_to_string(&config_path) {
			Ok(contents) => match toml::from_str::<Config>(&contents) {
				Ok(config) => Ok(config),
				Err(e) => Err(anyhow!("Could not parse config.toml: {:?}", e)),
			},
			Err(e) => Err(anyhow!("Could not read config.toml: {:?}", e)),
		}
	}
}
impl fmt::Display for Config {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		// You can customize this to print your struct as you see fit.
		// The example just uses Debug for simplicity.
		write!(f, "{:?}", self)
	}
}
