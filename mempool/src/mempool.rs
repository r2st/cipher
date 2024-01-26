use account::account_state::AccountState;
use anyhow::{anyhow, Error};
use block::{block_manager::BlockManager, block_state::BlockState};
use block_proposer::block_proposer_manager::BlockProposerManager;
use db::db::DbTxConn;
use execute::execute_block::ExecuteBlock;
use log::{info, warn};
use primitives::{Address, Balance, EventData, MemPoolSize, TimeStamp};
use secp256k1::{PublicKey, SecretKey};
use std::{
	collections::HashMap,
	time::{SystemTime, UNIX_EPOCH},
};
use system::{
	account::Account,
	block_proposer::BlockProposer,
	network::{BroadcastNetwork, EventBroadcast},
	transaction::Transaction,
	vote::{Vote, VoteSignPayload},
};
use tokio::sync::{broadcast, mpsc};
use validate::validate_common::ValidateCommon;
use vote::vote_state::VoteState;
use vrf_helper::common::SecpVRF;
pub struct Mempool {
	pub transactions: HashMap<Address, HashMap<TimeStamp, Vec<Transaction>>>,
	pub transactions_priority: Vec<(Transaction, TimeStamp)>,
	pub max_size: MemPoolSize,
	pub fee_limit: Balance,
	pub rate_limit: usize,
	pub time_frame_seconds: TimeStamp,
	pub expiration_seconds: TimeStamp,
	pub network_client_tx: mpsc::Sender<BroadcastNetwork>,
	pub event_tx: broadcast::Sender<EventBroadcast>,
	pub cluster_address: Address,
	pub node_address: Address,
	pub secret_key: SecretKey,
	pub verifying_key: PublicKey,
	pub multinode_mode: bool,
}

impl<'a> Mempool {
	pub fn new(
		max_size: MemPoolSize,
		fee_limit: Balance,
		rate_limit: usize,
		time_frame_seconds: TimeStamp,
		expiration_seconds: TimeStamp,
		network_client_tx: mpsc::Sender<BroadcastNetwork>,
		event_tx: broadcast::Sender<EventBroadcast>,
		cluster_address: Address,
		node_address: Address,
		secret_key: SecretKey,
		verifying_key: PublicKey,
		multinode_mode: bool,
	) -> Self {
		Mempool {
			transactions: HashMap::new(),
			transactions_priority: Vec::new(),
			max_size,
			fee_limit,
			rate_limit,
			time_frame_seconds,
			expiration_seconds,
			network_client_tx,
			event_tx,
			cluster_address,
			node_address,
			secret_key,
			verifying_key,
			multinode_mode,
		}
	}

	pub async fn add_transaction(
		&mut self,
		mut transaction: Transaction,
		db_pool_conn: &'a DbTxConn<'a>,
	) -> Result<(), Error> {
		// Check if the mempool is full
		if self.transactions_priority.len() >= self.max_size {
			return Err(anyhow!("Mempool is full"))
		}

		info!(
			"MEMPOOL => Transaction fee limit: {:?}\nAccount fee limit{:?}",
			transaction.fee_limit, self.fee_limit
		);

		// Check if the transaction fee is within the fee limit
		if transaction.fee_limit > self.fee_limit {
			return Err(anyhow!("Transaction fee exceeds fee limit"))
		}

		let sender = Account::address(&transaction.verifying_key)?;

		ValidateCommon::validate_tx(&transaction.clone(), &sender, &db_pool_conn)
			.await
			.and_then(|_| Ok(()))?;

		info!(
			"\nMEM_POOL => transaction validated\nFEE LIMIT => {:?}\nSIGNATURE => {:?}",
			transaction.clone().fee_limit,
			hex::encode(transaction.clone().signature)
		);

		// Check if the sender has exceeded the rate limit within the time frame
		let mut current_time = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();

		// Retrieve the transactions for the sender from the mempool within the time frame
		let entries: HashMap<TimeStamp, Vec<Transaction>> = match self.transactions.get(&sender) {
			Some(sender_transactions) => sender_transactions
				.clone()
				.into_iter()
				.filter(|(timestamp, _)| *timestamp >= (current_time - self.time_frame_seconds))
				.collect(),
			None => HashMap::new(), // No transactions for the sender, so an empty HashMap
		};

		// Calculate the sender's transaction count
		let sender_transaction_count = entries.values().flatten().count();

		if sender_transaction_count >= self.rate_limit {
			return Err(anyhow!("Rate limit exceeded for the sender"))
		}

		let conflicts: Vec<(Transaction, TimeStamp)> = self.get_conflicts(&transaction).await;
		if !conflicts.is_empty() {
			// Remove conflicting transactions from mempool
			for (conflict, conflict_timestamp) in &conflicts {
				let conflict_sender = Account::address(&conflict.verifying_key)?;
				let sender_transactions = self.transactions.get_mut(&conflict_sender);
				if let Some(sender_transactions) = sender_transactions {
					if let Some(transactions_by_timestamp) =
						sender_transactions.get_mut(conflict_timestamp)
					{
						transactions_by_timestamp.retain(|tx| tx.nonce != conflict.nonce);
					}
				}
			}
			// Handle conflicts based on transaction fee
			(transaction, current_time) =
				self.handle_conflicts(conflicts, (transaction, current_time));
		}

		let entry = self
			.transactions
			.entry(sender.clone())
			.or_insert_with(HashMap::new)
			.entry(current_time)
			.or_insert_with(Vec::new);
		entry.push(transaction.clone());

		// Determine the insertion position based on gas (higher gas first)
		let insertion_pos = self
			.transactions_priority
			.iter()
			.position(|(tx, _)| tx.fee_limit < transaction.fee_limit)
			.unwrap_or_else(|| self.transactions.len() - 1);

		// Insert the transaction at the determined position
		self.transactions_priority
			.insert(insertion_pos, (transaction.clone(), current_time));
		let size = self.transactions_priority.len();

		let message = format!(
			r#"
                +--------------------------------------------------------------------+
                |
                |  MEM_POOL => TX ADDED TO MEM_POOL
                |  mempool size => {}
                |
                +--------------------------------------------------------------------+
                "#,
			size
		);
		info!("{}", message);
		info!("Transaction nonce {:?} and sender {:?}", transaction.nonce, hex::encode(sender));

		if self.multinode_mode {
			if let Err(e) = self
				.network_client_tx
				.send(BroadcastNetwork::BroadcastTransaction(transaction))
				.await
			{
				warn!("Unable to write transaction to network_client_tx channel: {:?}", e)
			}
		}

		Ok(())
	}

	async fn get_conflicts(&mut self, transaction: &Transaction) -> Vec<(Transaction, TimeStamp)> {
		let mut conflicts: Vec<(Transaction, TimeStamp)> = Vec::new();
		let mut transactions_priority_copy = self.transactions_priority.clone();

		// Check for conflicts with existing transactions
		transactions_priority_copy.retain(|(existing_tx, existing_tx_timestamp)| {
			if self.has_conflict(existing_tx, &transaction) {
				conflicts.push((existing_tx.clone(), *existing_tx_timestamp));
				false // Remove the conflicting transaction from the mempool_priority
			} else {
				true // Keep the non-conflicting transaction in the mempool_priority
			}
		});
		self.transactions_priority = transactions_priority_copy;
		conflicts
	}

	fn has_conflict(&self, existing_tx: &Transaction, new_tx: &Transaction) -> bool {
		let existing_tx_sender = match Account::address(&existing_tx.verifying_key) {
			Ok(address) => address,
			Err(_) => return false,
		};
		let new_tx_sender = match Account::address(&new_tx.verifying_key) {
			Ok(address) => address,
			Err(_) => return false,
		};

		existing_tx_sender == new_tx_sender && existing_tx.nonce == new_tx.nonce
	}

	fn handle_conflicts(
		&self,
		conflicts: Vec<(Transaction, TimeStamp)>,
		new_tx: (Transaction, TimeStamp),
	) -> (Transaction, TimeStamp) {
		let (new_tx_transaction, _) = new_tx.clone();
		let (mut transaction, mut timestamp) = new_tx.clone();
		for (conflict, conflict_timestamp) in conflicts {
			if new_tx_transaction.fee_limit < conflict.fee_limit {
				transaction = conflict.clone();
				timestamp = conflict_timestamp;
			}
		}
		(transaction, timestamp)
	}

	pub async fn remove_transaction(&mut self, sender: &Address) {
		self.transactions.remove(sender);
	}

	pub async fn get_transactions(&mut self) -> Vec<Transaction> {
		self.transactions
			.values()
			.flat_map(|transactions_by_timestamp| {
				transactions_by_timestamp.values().flatten().cloned()
			})
			.collect()
	}

	pub async fn get_transactions_priority(&mut self) -> Vec<Transaction> {
		self.convert_to_transactions(self.transactions_priority.clone())
	}

	fn convert_to_transactions(&self, vec: Vec<(Transaction, TimeStamp)>) -> Vec<Transaction> {
		vec.into_iter().map(|(transaction, _)| transaction).collect()
	}

	pub async fn get_transactions_by_address(&mut self, address: Address) -> Vec<Transaction> {
		if let Some(transactions_by_timestamp) = self.transactions.get(&address) {
			transactions_by_timestamp.values().flatten().cloned().collect()
		} else {
			Vec::new()
		}
	}

	pub async fn remove_expired_transactions(&mut self) {
		let current_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();
		self.transactions.retain(|_sender, transactions_by_timestamp| {
			transactions_by_timestamp.retain(|timestamp, _transactions| {
				*timestamp >= current_time - self.expiration_seconds
			});
			!transactions_by_timestamp.is_empty()
		});
	}

	pub async fn clear_transactions(&mut self) {
		//let left_transactions = self.get_transactions().await;
		self.transactions_priority = vec![];
		self.transactions = HashMap::new();
		/*for tx in left_transactions {
			self.add_transaction(tx);
		}*/
	}
	pub async fn propose_block(
		&mut self,
		block_proposer: BlockProposer,
		db_pool_conn: &'a DbTxConn<'a>,
	) -> Result<Vec<EventData>, Error> {
		let mut block_proposer_manager = BlockProposerManager {};

		if !block_proposer_manager
			.is_block_proposer(
				1, //block_number , hard coded to same initial block proposer
				block_proposer.cluster_address.clone(),
				block_proposer.clone(),
				&db_pool_conn,
			)
			.await?
		{
			return Ok(vec![])
			//return Err(anyhow!("Account is not the proposer for last block"));
		}

		//block_proposer_manager.select_block_proposers(block_proposer.clone(), 5);
		println!();
		info!(
			"I am proposing a block. Block Proposer: {:?}",
			hex::encode(block_proposer.address.clone())
		);

		let block_state = BlockState::new(&db_pool_conn).await?;
		let account_state = AccountState::new(&db_pool_conn).await?;

		let transactions = self.transactions_priority.clone();
		self.clear_transactions().await;

		let block_manager = BlockManager::new();
		let (block, _block_header) = block_manager
			.new_block(
				self.convert_to_transactions(transactions),
				block_proposer,
				&block_state,
				&account_state,
			)
			.await?;
		//info!("(block, block_header): {:?} : {:?}", block, block_header);
		info!(
			"Block Produced, Block Hash - {:?}",
			hex::encode(block.block_header.block_hash.clone())
		);
		let mut events = Vec::new();

		// If not in multinode mode, just execute the block without broadcasting
		if !self.multinode_mode {
			events =
				ExecuteBlock::execute_block(&block, self.event_tx.clone(), db_pool_conn).await?;
			info!(
				"Block Executed, Block Hash - {:?}",
				hex::encode(block.block_header.block_hash.clone())
			);
		}

		// If in multinode mode, broadcast the block and vote
		if self.multinode_mode {
			if let Err(e) = self
				.network_client_tx
				.send(BroadcastNetwork::BroadcastValidateBlock(block.clone()))
				.await
			{
				warn!("Unable to write block to network_client_tx channel: {:?}", e)
			}
			let vote_sign_payload = VoteSignPayload::new(
				block.block_header.block_number,
				block.block_header.block_hash,
				self.cluster_address.clone(),
				true, // aye vote is implicit as the node produced the block itself
			);

			let sig = vote_sign_payload.sign_with_ecdsa(self.secret_key)?;

			let vote = Vote::new(
				vote_sign_payload,
				self.node_address.clone(),
				sig.serialize_compact().to_vec(),
				self.verifying_key.serialize().to_vec(),
			);
			let vote_state = VoteState::new(&db_pool_conn).await?;
			vote_state.store_vote(&vote).await?;
			if let Err(e) = self.network_client_tx.send(BroadcastNetwork::BroadcastVote(vote)).await
			{
				warn!("Unable to write vote to network_client_tx channel: {:?}", e)
			}
		}
		Ok(events)
	}
}
