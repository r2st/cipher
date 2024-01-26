use crate::sync::sync_node;
use block::block_state::BlockState;
use consensus::consensus::Consensus;
use db::db::Database;
use libp2p::identity::Keypair;
use log::{debug, error, info, warn};
use mempool::mempool::Mempool;
use node_info::node_info_state::NodeInfoState;
use p2p::network::{self, Event};
use primitives::*;
use secp256k1::{hashes::sha256, Message, PublicKey, Secp256k1, SecretKey};
use serde_json::Value;
use std::{
	collections::HashMap,
	thread,
	time::{Instant, SystemTime, UNIX_EPOCH},
};
use system::{
	account::Account,
	block::{Block, BlockBroadcast, BlockPayload},
	block_header::{BlockHeaderBroadcast, BlockHeaderPayload},
	block_proposer::{BlockProposer, BlockProposerBroadcast, BlockProposerPayload},
	mempool::{ProcessMempool, ResponseMempool},
	network::{BroadcastNetwork, EventBroadcast, ReceiveNetwork},
	node_info::NodeInfoBroadcast,
	transaction::TransactionBroadcast,
	vote::VoteBroadcast,
	vote_result::VoteResultBroadcast,
};
use tokio::{
	sync::{broadcast, mpsc},
	task,
	time::{sleep, Duration},
};
use validator::validator_manager::ValidatorManager;

#[derive(Debug, Clone)]
pub struct FullNode {
	pub ip_address: IpAddress,
	pub metadata: Metadata,
	pub node_address: Address,
	pub node_keypair: Keypair,
	pub cluster_address: Address,
	pub mempool_tx: mpsc::Sender<ProcessMempool>,
	pub node_event_tx: broadcast::Sender<EventData>,
	pub node_evm_event_tx: broadcast::Sender<EventBroadcast>,
	pub historical_sync_blocks: HashMap<BlockNumber, Block>,
}

impl FullNode {
	/// Create a new FullNode
	/// # Arguments
	/// * `max_size` - The maximum size of the mempool
	/// * `fee_limit` - The maximum fee that can be charged for a transaction
	/// * `rate_limit` - The maximum number of transactions that can be added to the mempool per
	///   second
	/// * `time_frame_seconds` - The time frame in seconds for the rate limit
	/// * `expiration_seconds` - The time in seconds for a transaction to expire
	/// * `dev_mode` - Whether or not to run the node in dev mode. Dev mode provides default dev
	///   accounts that are funded for use in testing
	/// * `multinode_mode` - Whether or not to run the node in multinode mode. Multinode mode allows
	///   the node to connect to other nodes in the network
	/// * `node_ip_address` - The ip address of the node
	/// * `node_keypair` - The keypair of the node
	/// * `bootnodes` - The bootnodes of the node
	pub async fn new(
		max_size: MemPoolSize,
		fee_limit: Balance,
		rate_limit: usize,
		time_frame_seconds: TimeStamp,
		expiration_seconds: TimeStamp,
		dev_mode: bool,
		multinode_mode: bool,
		node_ip_address: &str, // eg. "/ip4/0.0.0.0/tcp/5010"
		NODE_PRIVKEY: Option<String>,
		bootnodes: &[&str], // er. &["/ip4/0.0.0.0/tcp/5010/p2p/1234567890"]
		block_time: TimeStamp,
		cluster_address: Address,
		validator_pool_address: Address,
	) -> (FullNode, mpsc::Receiver<ResponseMempool>) {
		let server_info = r#"
        _     __        _   _           _
        | |   /_ |      | \ | |         | |
        | |    | |_  __ |  \| | ___   __| | ___
        | |    | \ \/ / | . ` |/ _ \ / _` |/ _ \
        | |____| |>  <  | |\  | (_) | (_| |  __/
        |______|_/_/\_\ |_| \_|\___/ \__,_|\___|
          ____                   _                       _                _                                                          _
         / __ \                 (_)                     (_)              | |                                                        | |
        | |  | |_ __   ___ ___   _ _ __ ___   __ _  __ _ _ _ __   ___  __| |    _ __   _____      __  _ __  _ __ _____   _____ _ __ | |
        | |  | | '_ \ / __/ _ \ | | '_ ` _ \ / _` |/ _` | | '_ \ / _ \/ _` |   | '_ \ / _ \ \ /\ / / | '_ \| '__/ _ \ \ / / _ \ '_ \| |
        | |__| | | | | (_|  __/ | | | | | | | (_| | (_| | | | | |  __/ (_| |_  | | | | (_) \ V  V /  | |_) | | | (_) \ V /  __/ | | |_|
         \____/|_| |_|\___\___| |_|_| |_| |_|\__,_|\__, |_|_| |_|\___|\__,_( ) |_| |_|\___/ \_/\_/   | .__/|_|  \___/ \_/ \___|_| |_(_)
                                                    __/ |                  |/                        | |
                                                   |___/                                             |_|

             Version: 0.0.7

             August 31, 2023

             Initializing...

            "#;
		info!("{}", server_info);

		let dev_mode_enabled = match dev_mode {
			true => "Enabled",
			false => "Disabled",
		};
		let multinode_mode_enabled = match multinode_mode {
			true => "Enabled",
			false => "Disabled",
		};
		info!("\n============================\n| DEV MODE: {}        |\n| MULTINODE MODE: {} |\n============================", dev_mode_enabled, multinode_mode_enabled);

		let node_private_key = match NODE_PRIVKEY.clone() {
			Some(node_private_key) => node_private_key,
			None => {
				println!("An error occurred, node_private_key not found");
				panic!()
			},
		};
		let node_private_key = node_private_key.as_str();
		let secret_key = SecretKey::from_slice(
			&hex::decode(node_private_key).expect("Error decoding node_private_key"),
		)
		.expect("Failed to parse provided private_key");
		let secp = Secp256k1::new();
		let verifying_key = secret_key.public_key(&secp);
		let verifying_key_bytes = verifying_key.serialize().to_vec();
		let node_address =
			Account::address(&verifying_key_bytes).expect("Failed to get node address");

		debug!("Verifying key: {:?}", hex::encode(&verifying_key_bytes));
		info!("NODE ADDRESS: 0x{}", hex::encode(node_address));

		let node_keypair = NODE_PRIVKEY.as_ref().map_or_else(
			|| Keypair::generate_secp256k1(),
			|private_key| {
				let mut keypair_bytes =
					hex::decode(private_key).expect("Failed to hex decode privkey");
				let secret_key =
					libp2p::identity::secp256k1::SecretKey::try_from_bytes(&mut keypair_bytes)
						.expect("Failed to parse keypair");

				// Create a new Keypair using the secp256k1::Keypair constructor
				let secp_keypair = libp2p::identity::secp256k1::Keypair::from(secret_key);

				// Use try_into_secp256k1 from libp2p_identity to convert to Keypair
				let keypair: libp2p::identity::Keypair =
					secp_keypair.try_into().expect("Failed to convert to Keypair");

				keypair
			},
		);

		let (mut network_client, event_receiver, event_loop) =
			network::new(node_keypair.clone(), bootnodes)
				.await
				.expect("Network to be created");

		// Spawn the networking event loop
		tokio::spawn(event_loop.run());

		// Start listening
		network_client
			.start_listening(node_ip_address.parse().expect("Address parses correctly"))
			.await
			.expect("Listening not to fail");

		let (mempool_tx, mempool_rx) = mpsc::channel(1000);
		let (mempool_res_tx, mempool_res_rx) = mpsc::channel(1000);
		let (network_client_tx, network_client_rx) = mpsc::channel(10_000);
		let (network_receive_tx, network_receive_rx) = mpsc::channel(10_000);
		let (timer_tx, timer_rx) = mpsc::channel(1);

		// EVM (and maybe CIPHERVM) events are written to the sender channel.
		// let (event_tx, event_rx) = mpsc::channel(1000);
		let (event_tx, _) = broadcast::channel(1000);

		// For CIPHERVM events
		let (node_event_tx, _) = broadcast::channel(32);

		// Create a fresh mempool
		let mempool = Mempool::new(
			max_size,
			fee_limit,
			rate_limit,
			time_frame_seconds,
			expiration_seconds,
			network_client_tx.clone(),
			event_tx.clone(),
			cluster_address.clone(),
			node_address.clone(),
			secret_key.clone(),
			verifying_key.clone(),
			multinode_mode,
		);
		let message = format!(
			r#"
            +---------------------------------------------------------+
            ☀️ VALIDATOR'S STAKING POOL ADDRESS ☀️ ➡️ {}
            +---------------------------------------------------------+
            "#,
			hex::encode(validator_pool_address)
		);
		info!("{}", message);

		let consensus = Consensus::new(
			event_tx.clone(),
			node_event_tx.clone(),
			network_client_tx.clone(),
			node_address.clone(),
			validator_pool_address.clone(),
			cluster_address.clone(),
			secret_key.clone(),
			verifying_key.clone(),
			multinode_mode,
		);

		let ip_address = "127.0.0.1".as_bytes().to_vec();
		let metadata = "metadata".as_bytes().to_vec();
		let full_node = FullNode {
			ip_address: ip_address.clone(),
			metadata: metadata.clone(),
			node_address: node_address.clone(),
			node_keypair,
			cluster_address: cluster_address.clone(),
			mempool_tx: mempool_tx.clone(),
			node_event_tx: node_event_tx.clone(),
			node_evm_event_tx: event_tx.clone(),
			historical_sync_blocks: HashMap::new(),
		};

		// Handling EVM events, but can be updated to handle CIPHERVM events as well in the future I
		// think
		task::spawn(Self::evm_events(event_tx.clone()));

		// Handling CIPHERVM events
		task::spawn(Self::node_event_receiver_process(node_event_tx.clone()));

		// Start the node listening for supported events from the networking code for the node to
		// handle
		task::spawn(Self::event_receiver_process(
			event_receiver,
			mempool_tx.clone(),
			network_receive_tx,
		));

		// Only sync when in multinode mode
		if multinode_mode {
			if bootnodes.len() != 0 {
				let sync_start_time = Instant::now();
				// Sync historical blocks from existing archive/full node
				match sync_node(cluster_address, bootnodes, event_tx.clone(), 500).await {
					Ok(_) => {
						info!("✅ Syncing node successful ✅");
						info!(
							"⌛️ Syncing node took: {:?} seconds",
							sync_start_time.elapsed().as_secs()
						);
					},
					Err(e) => {
						panic!("Unable to sync node: {:?}", e);
					},
				}
			}
		}

		task::spawn(Self::block_production_timer(
			mempool_tx,
			timer_rx,
			network_client_tx.clone(),
			block_time,
			cluster_address.clone(),
			validator_pool_address.clone(),
			multinode_mode,
		));
		let block_proposer: BlockProposer = BlockProposer {
			cluster_address: cluster_address.clone(),
			block_number: 1,
			address: node_address.clone(),
		};
		task::spawn(Self::process_mempool(
			mempool_rx,
			mempool_res_tx,
			timer_tx,
			node_event_tx.clone(),
			mempool,
			block_proposer,
		));

		task::spawn(Self::broadcast_network(
			network_client_rx,
			network_client,
			secret_key,
			verifying_key,
		));
		task::spawn(Self::receive_network(network_receive_rx, consensus));

		if multinode_mode {
			// Give time for the node to connect to a peer before broadcasting its node info
			let duration = Duration::from_secs(2);
			thread::sleep(duration);

			// Broadcast the nodes info to the network on startup
			let db_pool_conn =
				Database::get_pool_connection().await.expect("unable to get db_pool_conn");
			let node_info_state =
				NodeInfoState::new(&db_pool_conn).await.expect("Unable to get node info state");
			let node_info = node_info_state
				.load_node_info(&node_address)
				.await
				.expect("Unable to get node info");
			network_client_tx
				.send(BroadcastNetwork::BroadcastNodeInfo(node_info.clone()))
				.await
				.expect("Unable to send node_info to network_client_tx");
		}

		(full_node, mempool_res_rx)
	}

	async fn evm_events(node_event_tx: broadcast::Sender<EventBroadcast>) {
		// loop {
		//     match event_receiver.recv().await {
		//         Some(event) => {
		//             info!("NEW EVENT: {event:?}");
		//         }
		//         None => {
		//             warn!("Shouldn't happen?");
		//         }
		//     }
		// }

		// Handle incoming node events
		let mut node_event_rx = node_event_tx.subscribe();

		loop {
			match node_event_rx.recv().await {
				Ok(event) => {
					// publish to WS event topic
					info!(
						"Received EVM event: {event:?}",
						/* serde_json::from_slice::<Value>(&event)
						 * .map(|x| x.to_string())
						 * .unwrap_or_else(|_| format!("Unserializable event: {event:?}")) */
					);
				},
				Err(e) => {
					warn!("Unable to read event from event_tx channel: {:?}", e);
				},
			}
		}
	}

	async fn node_event_receiver_process(node_event_tx: broadcast::Sender<EventData>) {
		// Handle incoming node events
		let mut node_event_rx = node_event_tx.subscribe();

		loop {
			match node_event_rx.recv().await {
				Ok(event) => {
					// publish to WS event topic
					info!(
						"Received node event: {}",
						serde_json::from_slice::<Value>(&event)
							.map(|x| x.to_string())
							.unwrap_or_else(|_| format!("Unserializable event: {event:?}"))
					);
				},
				Err(e) => {
					warn!("Unable to read event from event_tx channel: {:?}", e);
				},
			}
		}
	}

	/// When the node receives new supported events over the p2p network, they are routed here for
	/// further handling
	async fn event_receiver_process(
		mut event_receiver: mpsc::Receiver<Event>,
		mempool_tx: mpsc::Sender<ProcessMempool>,
		network_receive_tx: mpsc::Sender<ReceiveNetwork>,
	) {
		debug!("🔥🔥🔥event_receiver_process started");
		// Handle incoming network events
		loop {
			match event_receiver.recv().await {
				Some(event) => {
					match event {
						// A transaction has just been received from another node in the network.
						// Add it to this nodes mempool if it is valid.
						Event::InboundTransaction { transaction } => {
							info!("📨 I just received a new TX from the network");
							if let Err(e) =
								mempool_tx.send(ProcessMempool::AddTransaction(transaction)).await
							{
								warn!("Unable to write transaction to mempool channel: {:?}", e)
							}
						},
						Event::InboundNodeInfo { node_info } => {
							if let Err(e) = network_receive_tx
								.send(ReceiveNetwork::ReceiveNodeInfo(node_info))
								.await
							{
								warn!(
									"Unable to write node_info to network_receive_tx channel: {:?}",
									e
								)
							}
						},
						Event::InboundValidateBlock { block_payload } => {
							if let Err(e) = network_receive_tx
								.send(ReceiveNetwork::ReceiveValidateBlock(block_payload))
								.await
							{
								warn!("Unable to write block_payload to network_receive_tx channel: {:?}", e)
							}
						},
						Event::InboundBlock { block_payload } => {
							if let Err(e) = network_receive_tx
								.send(ReceiveNetwork::ReceiveBlock(block_payload))
								.await
							{
								warn!("Unable to write block_payload to network_receive_tx channel: {:?}", e)
							}
						},
						Event::InboundBlockHeader { block_header_payload } => {
							if let Err(e) = network_receive_tx
								.send(ReceiveNetwork::ReceiveBlockHeader(block_header_payload))
								.await
							{
								warn!("Unable to write block_payload to network_receive_tx channel: {:?}", e)
							}
						},
						Event::InboundBlockProposer { block_proposer_payload } => {
							if let Err(e) = network_receive_tx
								.send(ReceiveNetwork::ReceiveBlockProposer(block_proposer_payload))
								.await
							{
								warn!("Unable to write block_payload to network_receive_tx channel: {:?}", e)
							}
						},
						Event::InboundVote { vote } => {
							if let Err(e) =
								network_receive_tx.send(ReceiveNetwork::ReceiveVote(vote)).await
							{
								warn!("Unable to write vote to network_receive_tx channel: {:?}", e)
							}
						},
						Event::InboundVoteResult { vote_result } => {
							if let Err(e) = network_receive_tx
								.send(ReceiveNetwork::ReceiveVoteResult(vote_result))
								.await
							{
								warn!("Unable to write vote_result to network_receive_tx channel: {:?}", e)
							}
						},
					}
				},
				// Command channel closed, thus shutting down the network event loop.
				None => return,
			}
		}
	}

	async fn block_production_timer(
		mempool_tx: mpsc::Sender<ProcessMempool>,
		mut timer_rx: mpsc::Receiver<TimeStamp>,
		_network_client_tx: mpsc::Sender<BroadcastNetwork>,
		block_time: TimeStamp, // Assuming block_time is in milliseconds
		cluster_address: Address,
		pool_address: Address,
		multinode_mode: bool,
	) {
		info!("Inside block_production_timer: started");
		info!("Inside block_production_timer: block_time: {}", block_time);
		let mut last_propose_block_time = SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.expect("Failed to get system time")
			.as_millis();

		loop {
			if multinode_mode {
				select_validators(pool_address.clone(), cluster_address.clone()).await;
			}

			sleep(Duration::from_millis(1000)).await;

			let current_time = SystemTime::now()
				.duration_since(UNIX_EPOCH)
				.expect("Failed to get system time")
				.as_millis();
			if (last_propose_block_time + block_time) <= current_time {
				if let Err(e) = mempool_tx.send(ProcessMempool::ProposeBlockOnBlockTime).await {
					warn!("Unable to write time to mempool_tx channel: {:?}", e)
				}
				while let Ok(propose_block_time) = timer_rx.try_recv() {
					last_propose_block_time = propose_block_time;
				}
			} else {
				if let Err(e) = mempool_tx.send(ProcessMempool::ProposeBlockOnMempoolFull).await {
					warn!("Unable to write time to mempool_tx channel: {:?}", e)
				}
				while let Ok(propose_block_time) = timer_rx.try_recv() {
					last_propose_block_time = propose_block_time;
				}
			}
		}
	}

	async fn process_mempool(
		mut mempool_rx: mpsc::Receiver<ProcessMempool>,
		mempool_res_tx: mpsc::Sender<ResponseMempool>,
		timer_tx: mpsc::Sender<TimeStamp>,
		node_event_tx: broadcast::Sender<EventData>,
		mut mempool: Mempool,
		block_proposer: BlockProposer,
	) {
		let db_pool_conn =
			Database::get_pool_connection().await.expect("error getting db_pool_conn");

		while let Some(process_mempool) = mempool_rx.recv().await {
			match process_mempool {
				ProcessMempool::AddTransaction(transaction) => {
					match mempool.add_transaction(transaction, &db_pool_conn).await {
						Ok(_) => {
							if let Err(e) = mempool_res_tx.send(ResponseMempool::Success).await {
								error!("Unable to write ResponseMempool to mempool_res_tx channel: {:?}", e);
							}
						},
						Err(e) => {
							if let Err(e) =
								mempool_res_tx.send(ResponseMempool::FailedToAddTransaction).await
							{
								error!("Unable to write ResponseMempool to mempool_res_tx channel: {:?}", e);
							}
							warn!("Unable to add transaction to mempool: {:?}", e);
						},
					};
				},
				ProcessMempool::ProposeBlockOnMempoolFull => {
					if mempool.transactions_priority.len() >= mempool.max_size {
						info!("NODE => CREATING NEW BLOCK on Mempool Full");
						match mempool.propose_block(block_proposer.clone(), &db_pool_conn).await {
							Ok(events) => {
								for event in events {
									if let Err(err) = node_event_tx.send(event) {
										warn!(
											"Failed to publish ProposeBlock event due to {:?}",
											err
										);
									}
								}
								if let Err(e) = timer_tx
									.send(
										SystemTime::now()
											.duration_since(UNIX_EPOCH)
											.expect("Failed to get system time")
											.as_millis(),
									)
									.await
								{
									error!("Unable to write time to timer_tx channel: {:?}", e)
								}
							},
							Err(e) => {
								error!(
									"Propose block failed on ProposeBlockOnMempoolFull: {:?}",
									e
								);
							},
						};
					}
				},
				ProcessMempool::ProposeBlockOnBlockTime => {
					match mempool.propose_block(block_proposer.clone(), &db_pool_conn).await {
						Ok(events) => {
							for event in events {
								if let Err(err) = node_event_tx.send(event) {
									warn!("Failed to publish ReceiveBlock event due to {:?}", err);
								}
							}
							if let Err(e) = timer_tx
								.clone()
								.send(
									SystemTime::now()
										.duration_since(UNIX_EPOCH)
										.expect("Failed to get system time")
										.as_millis(),
								)
								.await
							{
								error!("Unable to write time to timer_tx channel: {:?}", e)
							}
						},
						Err(e) => {
							error!("Propose block failed on ProposeBlockOnBlockTime: {:?}", e);
						},
					};
				},
			}
		}
	}

	async fn broadcast_network(
		mut network_client_rx: mpsc::Receiver<BroadcastNetwork>,
		network_client: network::Client,
		secret_key: SecretKey,
		verifying_key: PublicKey,
	) {
		while let Some(broadcast_network) = network_client_rx.recv().await {
			match broadcast_network {
				BroadcastNetwork::BroadcastNodeInfo(node_info) => {
					match network_client.node_info_broadcast(node_info).await {
						Ok(_) => {},
						Err(e) => {
							match e.to_string().as_str() {
								"Duplicate" => {
									// Ignore this warning
								},
								_ => {
									warn!(
										"Unable to broadcast node_info using network_client: {:?}",
										e
									);
								},
							}
						},
					};
				},
				BroadcastNetwork::BroadcastTransaction(transaction) => {
					match network_client.transaction_broadcast(transaction).await {
						Ok(_) => {},
						Err(e) => {
							match e.to_string().as_str() {
								"Duplicate" => {
									// Ignore this warning
								},
								_ => {
									warn!("Unable to broadcast transaction using network_client: {:?}", e);
								},
							}
						},
					};
				},
				BroadcastNetwork::BroadcastValidateBlock(block) => {
					let json_str =
						serde_json::to_string(&block).expect("Unable to parse to json string");
					let message = Message::from_hashed_data::<sha256::Hash>(json_str.as_bytes());
					let sig = secret_key.sign_ecdsa(message);
					let block_payload = BlockPayload {
						block,
						signature: sig.serialize_compact().to_vec(),
						verifying_key: verifying_key.serialize().to_vec(),
					};
					match network_client.block_validate_broadcast(block_payload).await {
						Ok(_) => {},
						Err(e) => {
							match e.to_string().as_str() {
								"Duplicate" => {
									// Ignore this warning
								},
								_ => {
									warn!("Unable to broadcast validate_block using network_client: {:?}", e);
								},
							}
						},
					};
				},
				BroadcastNetwork::BroadcastBlock(block) => {
					let json_str =
						serde_json::to_string(&block).expect("Unable to parse to json string");
					let message = Message::from_hashed_data::<sha256::Hash>(json_str.as_bytes());
					let sig = secret_key.sign_ecdsa(message);
					let block_payload = BlockPayload {
						block,
						signature: sig.serialize_compact().to_vec(),
						verifying_key: verifying_key.serialize().to_vec(),
					};
					match network_client.block_broadcast(block_payload).await {
						Ok(_) => {},
						Err(e) => {
							match e.to_string().as_str() {
								"Duplicate" => {
									// Ignore this warning
								},
								_ => {
									warn!(
										"Unable to broadcast block using network_client: {:?}",
										e
									);
								},
							}
						},
					};
				},
				BroadcastNetwork::BroadcastBlockHeader(block_header) => {
					let json_str = serde_json::to_string(&block_header)
						.expect("Unable to parse to json string");
					let message = Message::from_hashed_data::<sha256::Hash>(json_str.as_bytes());
					let sig = secret_key.sign_ecdsa(message);
					let block_header_payload = BlockHeaderPayload {
						block_header,
						signature: sig.serialize_compact().to_vec(),
						verifying_key: verifying_key.serialize().to_vec(),
					};
					match network_client.block_header_broadcast(block_header_payload).await {
						Ok(_) => {},
						Err(e) => {
							match e.to_string().as_str() {
								"Duplicate" => {
									// Ignore this warning
								},
								_ => {
									warn!("Unable to broadcast block_header using network_client: {:?}", e);
								},
							}
						},
					};
				},
				BroadcastNetwork::BroadcastBlockProposer(cluster_block_proposers) => {
					let json_str = serde_json::to_string(&cluster_block_proposers)
						.expect("Unable to parse to json string");
					let message = Message::from_hashed_data::<sha256::Hash>(json_str.as_bytes());
					let sig = secret_key.sign_ecdsa(message);
					let block_proposer_payload = BlockProposerPayload {
						cluster_block_proposers,
						signature: sig.serialize_compact().to_vec(),
						verifying_key: verifying_key.serialize().to_vec(),
					};
					match network_client.block_proposer_broadcast(block_proposer_payload).await {
						Ok(_) => {},
						Err(e) => {
							match e.to_string().as_str() {
								"Duplicate" => {
									// Ignore this warning
								},
								_ => {
									warn!(
										"Unable to broadcast block_proposer using network_client: {:?}",
										e
									);
								},
							}
						},
					};
				},
				BroadcastNetwork::BroadcastVote(vote) => {
					match network_client.vote_broadcast(vote).await {
						Ok(_) => {},
						Err(e) => {
							match e.to_string().as_str() {
								"Duplicate" => {
									// Ignore this warning
								},
								_ => {
									warn!("Unable to broadcast vote using network_client: {:?}", e);
								},
							}
						},
					};
				},

				BroadcastNetwork::BroadcastVoteResult(vote_result) => {
					match network_client.vote_result_broadcast(vote_result).await {
						Ok(_) => {},
						Err(e) => {
							match e.to_string().as_str() {
								"Duplicate" => {
									// Ignore this warning
								},
								_ => {
									warn!("Unable to broadcast vote_result using network_client: {:?}", e);
								},
							}
						},
					};
				},
			}
		}
	}

	/// Routes the payloads received from the network to the consensus function
	async fn receive_network(
		mut network_receive_rx: mpsc::Receiver<ReceiveNetwork>,
		mut consensus: Consensus,
	) {
		while let Some(receive_network) = network_receive_rx.recv().await {
			// Normal operation, node is already synced
			FullNode::handle_receive_network(receive_network, &mut consensus).await;
		}
	}

	async fn handle_receive_network(payload: ReceiveNetwork, consensus: &mut Consensus) {
		match payload {
			ReceiveNetwork::ReceiveNodeInfo(node_info) => {
				info!("📨 ℹ️ I just received a new NODE INFO from the network");
				match consensus.receive_node_info(node_info).await {
					Ok(_res) => {},
					Err(e) => {
						warn!("Received node_info failed: {:?}", e);
					},
				};
			},
			ReceiveNetwork::ReceiveValidateBlock(block_payload) => {
				info!(
					"📨 🟪 I just received a new VALIDATE BLOCK from the network\n{}",
					&block_payload
				);
				match consensus.receive_validate_block(block_payload).await {
					Ok(_res) => {},
					Err(e) => {
						warn!("Received block_payload failed validation: {:?}", e);
					},
				};
			},
			ReceiveNetwork::ReceiveBlock(block_payload) => {
				info!("📨 🟪 I just received a new BLOCK from the network");
				match consensus.receive_block(block_payload).await {
					Ok(_res) => {},
					Err(e) => {
						warn!("Received block_payload failed validation: {:?}", e);
					},
				};
			},
			ReceiveNetwork::ReceiveBlockHeader(block_header_payload) => {
				info!("📨 🟪 I just received a new BLOCK HEADER from the network");
				match consensus.receive_block_header(block_header_payload).await {
					Ok(_res) => {},
					Err(e) => {
						warn!("Received block_header_payload failed validation: {:?}", e);
					},
				};
			},
			ReceiveNetwork::ReceiveBlockProposer(block_proposer_payload) => {
				info!("📨 🟪 I just received a new BLOCK PROPOSER from the network");
				match consensus.receive_block_proposer(block_proposer_payload).await {
					Ok(_res) => {},
					Err(e) => {
						warn!("Received block_proposer_payload failed validation: {:?}", e);
					},
				};
			},
			ReceiveNetwork::ReceiveVote(vote_payload) => {
				info!("📨 ✓ 𐄂 Received new vote");
				match consensus.receive_vote(vote_payload).await {
					Ok(_res) => {},
					Err(e) => {
						warn!("Received vote_payload failed validation: {:?}", e);
					},
				};
			},
			ReceiveNetwork::ReceiveVoteResult(vote_result_payload) => {
				match consensus.receive_vote_result(vote_result_payload).await {
					Ok(_res) => {},
					Err(e) => {
						warn!("Received vote_result_payload failed validation: {:?}", e);
					},
				};
			},
		}
	}
}

async fn select_validators(pool_address: Address, cluster_address: Address) {
	let db_pool_conn = Database::get_pool_connection().await.expect("unable to get db_pool_conn");
	let validator_manager = ValidatorManager {};
	let block_state = BlockState::new(&db_pool_conn).await.expect("unable to get block state");
	let chain_state = block_state
		.load_chain_state(cluster_address.clone())
		.await
		.expect("unable to load_chain_state"); // TODO: proper error handling
	validator_manager
		.select_validators(
			&cluster_address,
			&pool_address,
			chain_state.block_number + 1,
			5,
			&db_pool_conn,
		)
		.await;
}
