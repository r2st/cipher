use super::super::service::FullNodeService;
use crate::{grpc::run_server as grpc_server, json::run_server as json_server};
use db::db::Database;
use directories::UserDirs;
use genesis::genesis::Genesis;
use log::{error, info};
use node_crate::node::FullNode;
use std::{fs::read_to_string, path::PathBuf};
use structopt::StructOpt;
use system::{config::Config, mempool::ResponseMempool};
use tokio::{sync::mpsc, task};
use types::eth::signers::initialize_eth_signer;

#[derive(Debug, StructOpt)]
#[structopt(name = "start")]
pub struct StartCmd {
	#[structopt(long = "path", short = "w")]
	working_dir: Option<PathBuf>,
}

impl StartCmd {
	pub async fn execute(&self) {
		pretty_env_logger::init();

		let working_dir = match &self.working_dir {
			Some(dir) => dir.clone(),
			None => {
				let user_dirs = UserDirs::new().expect("Couldn't fetch home directory");
				user_dirs.home_dir().to_path_buf()
			},
		};
		println!("Working directory: {:?}", working_dir);

		if !working_dir.exists() {
			println!("cipher folder does not exist");
			return;
		}

		// Construct path to config.toml and genesis.json
		let mut config_path = working_dir.clone();
		config_path.push("config.toml");

		let mut genesis_path = working_dir.clone();
		genesis_path.push("genesis.json");

		// Read and parse config.toml
		let mut parsed_config: Config = Config::default();
		let mut parsed_genesis: Option<Genesis> = None;
		match read_to_string(&config_path) {
			Ok(contents) => match toml::from_str::<Config>(&contents) {
				Ok(config) => {
					parsed_config = config;
				},
				Err(e) => println!("Could not parse config.toml: {:?}", e),
			},
			Err(e) => println!("Could not read config.toml: {:?}", e),
		}

		// Read and parse genesis.json
		match read_to_string(&genesis_path) {
			Ok(contents) => match serde_json::from_str::<Genesis>(&contents) {
				Ok(genesis) => {
					parsed_genesis = Some(genesis);
				},
				Err(e) => println!("Could not parse genesis.json: {:?}", e),
			},
			Err(e) => println!("Could not read genesis.json: {:?}", e),
		}

		Database::new(&parsed_config).await;

		let boot_nodes: &[&str] =
			&parsed_config.boot_nodes.iter().map(|s| s.as_str()).collect::<Vec<&str>>();

		// Initialize the new node
		let (full_node, mempool_res_rx) = FullNode::new(
			parsed_config.max_size,
			parsed_config.fee_limit as u128,
			parsed_config.rate_limit,
			parsed_config.time_frame_seconds as u128,
			parsed_config.expiration_seconds as u128,
			parsed_config.dev_mode,
			parsed_config.multinode_mode,
			&parsed_config.node_ip_address,
			Some(parsed_config.node_private_key),
			boot_nodes,
			parsed_config.block_time.into(),
			hex::decode(parsed_config.cluster_address)
				.expect("unable to decode cluster address")
				.try_into()
				.expect("Wrong length of Vec"),
			hex::decode(parsed_config.validator_pool_address)
				.expect("unable to decode cluster address")
				.try_into()
				.expect("Wrong length of Vec"),
		)
		.await;

		let eth_signers = initialize_eth_signer();

		let service = FullNodeService {
			whitelist_check: parsed_config.whitelist_check,
			node: full_node,
			signers: eth_signers,
		};

		let (mempool_grpc_tx, mempool_grpc_rx) = mpsc::channel(1000);
		let (mempool_json_tx, mempool_json_rx) = mpsc::channel(1000);
		task::spawn(Self::mempool_response(mempool_res_rx, mempool_grpc_tx, mempool_json_tx));
		info!("Starting rpc servers");
		let grpc_task = task::spawn(grpc_server(
			parsed_config.grpc_port.clone(),
			service.clone(),
			mempool_grpc_rx,
		));
		let json_rpc_task =
			task::spawn(json_server(parsed_config.jsonrpc_port.clone(), service, mempool_json_rx));

		// exit when either task finishes
		tokio::select! {
			_ = grpc_task => (),
			_ = json_rpc_task => (),

		}
	}

	pub async fn mempool_response(
		mut mempool_rx: mpsc::Receiver<ResponseMempool>,
		mempool_grpc_tx: mpsc::Sender<ResponseMempool>,
		mempool_json_tx: mpsc::Sender<ResponseMempool>,
	) {
		while let Some(mempool_response) = mempool_rx.recv().await {
			if let Err(e) = mempool_grpc_tx.send(mempool_response.clone()).await {
				error!("Unable to write mempool_response to mempool_grpc_tx channel: {:?}", e);
			}
			if let Err(e) = mempool_json_tx.send(mempool_response.clone()).await {
				error!("Unable to write mempool_response to mempool_json_tx channel: {:?}", e);
			}
		}
	}
}
