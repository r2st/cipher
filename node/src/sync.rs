use anyhow::{anyhow, Error};
use block::block_state::BlockState;
use cipher_rpc::rpc_model::{
	node_client::NodeClient, GetBlockByNumberRequest, GetChainStateRequest,
};
use db::db::Database;
use execute::execute_block::ExecuteBlock;
use log::{error, info, warn};
use primitives::{Address, BlockNumber};
use regex::Regex;
use system::{block::Block, block_header::BlockHeader, network::EventBroadcast};
use tokio::sync::{broadcast, mpsc};
use tonic::transport::Channel;
use util::convert;

/// Syncs the node with the chain using the provided full/archive node
pub async fn sync_node(
	cluster_address: Address,
	endpoints: &[&str],
	event_tx: broadcast::Sender<EventBroadcast>,
	batch_size: u16,
) -> Result<(), Error> {
	let sync_endpoints = multiaddrs_to_http_urls(endpoints);
	println!("Past sync_endpoints");

	// Connect to the nodes
	let mut grpc_clients = Vec::new();
	for endpoint in sync_endpoints {
		let grpc_client = NodeClient::connect(endpoint.clone()).await?;
		info!("Connection successful, syncing blocks from endpoint: {}", endpoint);
		grpc_clients.push((grpc_client, endpoint));
	}

	// Get the latest block number from the archive node
	let response = grpc_clients[0].0.get_chain_state(GetChainStateRequest {}).await?;

	let result = response.into_inner();
	println!("GetChainState = \n{result:?}",);
	let chain_height: BlockNumber = result.head_block_number.parse()?;

	let db_pool_conn = Database::get_pool_connection().await?;
	let block_state = BlockState::new(&db_pool_conn).await?;

	// Highest block number in this nodes storage
	let highest_local_block = match block_state.block_head(cluster_address.clone()).await {
		Ok(block) => {
			info!("Highest locally stored block is #{}", block.block_header.block_number);
			Some(block.block_header.block_number)
		},
		Err(e) => {
			// If the block head is not found, start syncing from block 1
			if e.to_string().contains("No row for block header") ||
				e.to_string().contains("No matching records found for block")
			{
				None
			} else {
				return Err(anyhow!("Error getting block head when starting node sync: {:?}", e));
			}
		},
	};
	let start_download_block_number = match highest_local_block {
		Some(block_number) => block_number + 1,
		None => 1, // Currently we don't have a block 0, the chain starts at 1
	};

	// Find the highest EXECUTED block number from this nodes storage
	let mut start_execute_block_number = 1;

	if start_download_block_number > 1 {
		// Find the highest executed block in storage
		start_execute_block_number = start_download_block_number - 1;
		loop {
			match block_state
				.is_block_executed(start_execute_block_number, &cluster_address)
				.await?
			{
				true => {
					info!("Highest executed block is #{}", start_execute_block_number);
					start_execute_block_number = start_execute_block_number + 1;
					break;
				},
				false => {
					info!("Stored block #{} not executed. Finding the highest executed block in storage...", start_execute_block_number);
					start_execute_block_number = start_execute_block_number - 1;

					if start_execute_block_number == 1 {
						break;
					}
				},
			}
		}
		info!("Beginning execution from block #{}", start_execute_block_number);
	}

	// Channel for send block batches to be verified and stored
	let (block_batch_tx, block_batch_rx) = mpsc::channel(10_000);

	// Start the block validate and store process
	let store_validate_blocks =
		tokio::spawn(async move { validate_and_store_blocks(chain_height, block_batch_rx).await });

	// Start the block execution process
	let execute_blocks = tokio::spawn(async move {
		execute_blocks(
			start_execute_block_number,
			chain_height,
			batch_size,
			cluster_address,
			event_tx,
		)
		.await
	});

	// Cycle iterator of all connections. This will help us to distribute the load of block
	// downloads
	let mut grpc_clients_iter = grpc_clients.iter().cycle();
	let mut block_number = start_download_block_number;

	// Download blocks in batches until we reach the initialy found chain height. During the sync,
	// blocks will be produced with a higher block number than the initial chain height. These will
	// be picked up by the normal p2p network and will be buffered. When the sync is done, the
	// buffered blocks will be processed.
	while block_number < chain_height {
		// Download and store block_batch_size blocks in parallel
		let mut tasks = Vec::new();

		// Store the missing blocks from a batch so we can try to download them again
		let mut missing_blocks: Vec<BlockNumber> = Vec::new();

		// When remaining blocks are less than batch_size
		let mut batch_upper_limit = block_number + batch_size as u128;
		if batch_upper_limit > chain_height {
			batch_upper_limit = chain_height + 1;
			println!(
				"batch_upper_limit > chain_height\n batch_upper_limit = {}",
				batch_upper_limit
			);
		}

		info!("📥 Syncing: Downloading blocks #{} - #{}", block_number, batch_upper_limit - 1);

		// Download a batch of blocks
		for _ in block_number..batch_upper_limit {
			let (grpc_client, endpoint) = grpc_clients_iter.next().unwrap().clone();
			let task =
				tokio::spawn(async move { fetch_block(block_number, endpoint, grpc_client).await });

			tasks.push(task);
			block_number = block_number + 1;
		}

		// Now tasks.len() == batch_size
		// Wait for all tasks to complete
		let results = futures::future::join_all(tasks).await;

		// Store the received blocks
		let mut block_batch: Vec<Block> = Vec::new();

		// Unwrap results and push blocks into vec
		for result in results {
			match result {
				Ok(block) => {
					let (block_number, block_option) = block?;
					match block_option {
						Some(block) => {
							block_batch.push(block);
						},
						None => {
							missing_blocks.push(block_number);
							error!("Block {} is missing during sync", block_number);
						},
					}
				},
				Err(e) => return Err(anyhow!("Block sync results error: {:?}", e)),
			}
		}

		info!("📥 Syncing: Downloaded block batch of size {}", block_batch.len());

		// If there are missing blocks, retry downloading them
		if !missing_blocks.is_empty() {
			warn!("🚨🚨🚨 Retrying download of missing blocks: {:?}", missing_blocks);
			let found_blocks = get_missing_blocks(missing_blocks, grpc_clients.clone()).await;
			block_batch.extend(found_blocks);
		}

		block_batch_tx.send(block_batch).await.map_err(|e| {
			anyhow!("Error sending block batch to validate and store process: {}", e)
		})?;
	}

	info!("📥 Syncing: Block download finished");

	if let Err(e) = store_validate_blocks.await? {
		return Err(anyhow!("Error validating and storing sync blocks: {:?}", e));
	}
	info!("💾 Finished storing blocks");

	if let Err(e) = execute_blocks.await? {
		return Err(anyhow!("Error executing sync blocks: {:?}", e));
	}

	info!("🎉 Finished executing sync blocks");

	Ok(())
}

async fn execute_blocks(
	start_execute_block_number: BlockNumber,
	chain_height: BlockNumber,
	// batch size will be used in the future
	_batch_size: u16,
	cluster_address: Address,
	event_tx: broadcast::Sender<EventBroadcast>,
) -> Result<(), Error> {
	let db_pool_conn = Database::get_pool_connection().await?;
	let block_state = BlockState::new(&db_pool_conn).await?;

	for block_number in start_execute_block_number..=chain_height {
		let mut block: Block = Block::new(BlockHeader::default(), vec![]);
		loop {
			match block_state.load_block(block_number, &cluster_address).await {
				Ok(stored_block) => {
					block = stored_block;
					break;
				},
				Err(e) => {
					warn!("Error loading block #{} from storage: {:?}", block_number, e);
					warn!(
						"⏱️ Block #{} not found in storage. Waiting for block to be stored...",
						block_number
					);

					// Wait some to give the blocks time for download, verification, and storage
					tokio::time::sleep(tokio::time::Duration::from_millis(1_000)).await;
					continue;
				},
			};
		}

		// Don't worry about returned events during syncing
		let _ = ExecuteBlock::execute_block(&block, event_tx.clone(), &db_pool_conn).await?;

		info!(
			"✅ Block #{} executed, out of sync start chain height of #{}",
			block_number, chain_height
		);
	}

	Ok(())
}

async fn validate_and_store_blocks(
	chain_height: BlockNumber,
	mut block_batch_rx: mpsc::Receiver<Vec<Block>>,
) -> Result<(), Error> {
	let db_pool_conn = Database::get_pool_connection().await?;
	let block_state = BlockState::new(&db_pool_conn).await?;
	// Await new batches of blocks
	while let Some(mut block_batch) = block_batch_rx.recv().await {
		info!("🔎🔎🔎 Validating and storing block batch of size {}", block_batch.len());

		// Validate the blocks
		// todo!("Validate the blocks");

		// Sort the blocks by block number
		block_batch.sort_by(|a, b| a.block_header.block_number.cmp(&b.block_header.block_number));
		let last_block_number = match block_batch.last() {
			Some(block) => block.block_header.block_number,
			None => return Err(anyhow!("No blocks in batch, shouldn't happen")),
		};

		// Store a batch of blocks in the database
		block_state
			.batch_store_blocks(block_batch)
			.await
			.map_err(|e| anyhow!("Error batch storing blocks: {:?}", e))?;

		if last_block_number == chain_height {
			warn!(
				"🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨Exiting validate and sync loop. Channel will be closed."
			);
			break;
		}
	}
	info!("Block batch channel closed");

	Ok(())
}

async fn get_missing_blocks(
	block_numbers: Vec<BlockNumber>,
	grpc_clients: Vec<(NodeClient<Channel>, String)>,
) -> Vec<Block> {
	let mut grpc_clients_iter = grpc_clients.iter().cycle();

	let mut tasks = Vec::new();

	for block_number in block_numbers {
		let (grpc_client, endpoint) = grpc_clients_iter.next().unwrap().clone();
		let task =
			tokio::spawn(async move { fetch_block(block_number, endpoint, grpc_client).await });

		tasks.push(task);
	}

	// Wait for all tasks to complete
	let results = futures::future::join_all(tasks).await;
	let mut found_blocks: Vec<Block> = Vec::new();

	for result in results {
		match result {
			Ok(block) => match block {
				Ok(block) => {
					let (block_number, block_option) = block;
					match block_option {
						Some(block) => {
							found_blocks.push(block);
						},
						None => {
							panic!("Block {} is missing during sync", block_number);
						},
					}
				},
				Err(e) => panic!("Error syncing missing block(s): {}", e),
			},
			Err(e) => panic!("Error syncing missing block(s): {}", e),
		}
	}

	found_blocks
}

pub async fn fetch_block(
	block_number: BlockNumber,
	endpoint: String,
	grpc_client: NodeClient<Channel>,
) -> Result<(BlockNumber, Option<Block>), Error> {
	let mut grpc_client = grpc_client;
	let response = grpc_client
		.get_block_by_number(GetBlockByNumberRequest { block_number: block_number.to_string() })
		.await?;

	let block = match response.into_inner().block {
		Some(block) => {
			info!("Syncing block {:?} from {}", block, endpoint);

			// Convert the proto block to a system block and store in the database
			let sys_block = convert::from_proto_block(block).await?;
			Some(sys_block)
		},
		None => {
			info!("Block {} is missing during sync", block_number);
			None
		},
	};

	Ok((block_number, block))
}

/// Converts an array of multiaddresses to HTTP URLs.
///
/// This function takes an array of multiaddresses as input and converts each multiaddress
/// into an HTTP URL. It uses a regular expression pattern to match and extract the IP address
/// from each multiaddress. If an IP address is found, it constructs an HTTP URL using the
/// extracted IP address and adds it to a vector of HTTP URLs. If no IP address is found in
/// a multiaddress, it prints a message indicating that no IP address was found.
///
/// # Arguments
///
/// * `multiaddr` - An array of multiaddresses.
///
/// # Returns
///
/// A vector of HTTP URLs.
pub fn multiaddrs_to_http_urls(multiaddr: &[&str]) -> Vec<String> {
	// Regular expression pattern to match an IP address
	let re = Regex::new(r"/ip4/(\d+\.\d+\.\d+\.\d+)/").unwrap();

	// Extracting the IP address
	let mut http_urls: Vec<String> = Vec::new();
	for addr in multiaddr {
		if let Some(capture) = re.captures(addr) {
			let ip_address = capture.get(1).unwrap().as_str();
			let http_url = format!("http://{}:50052", ip_address);
			http_urls.push(http_url);
		} else {
			println!("No IP address found in the input string.");
		}
	}

	info!("Syncing node from endpoints: {:?}", http_urls);
	http_urls
}
