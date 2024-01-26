use crate::block_state::BlockState;
use account::account_state::AccountState;
use anyhow::{anyhow, Error};

use log::info;
use primitives::*;
use sha2::{Digest, Sha256};
use std::{
	collections::HashMap,
	time::{SystemTime, UNIX_EPOCH},
};
use system::{
	account::Account,
	block::{Block, BlockSignPayload, BlockType},
	block_header::BlockHeader,
	block_proposer::BlockProposer,
	transaction::Transaction,
};

pub struct BlockManager;

impl<'a> BlockManager {
	pub fn new() -> BlockManager {
		BlockManager {}
	}
	pub async fn new_block(
		&self,
		transactions: Vec<Transaction>,
		block_proposer: BlockProposer,
		block_state: &BlockState<'a>,
		account_state: &AccountState<'a>,
	) -> Result<(Block, BlockHeader), Error> {
		let transactions = self.validate_nonce(transactions, account_state).await?;
		// Step 1: Load the last block header
		let last_block_header =
			match block_state.block_head_header(block_proposer.cluster_address).await {
				Ok(lbh) => lbh,
				Err(_e) => BlockHeader::default(),
			};

		// Step 2: Increment the block number from the last block header
		let block_number: BlockNumber = last_block_header.block_number + 1;
		info!(
			"🟪🟪🟪 Creating block #{} in cluster 0x{} 🟪🟪🟪",
			block_number,
			hex::encode(block_proposer.cluster_address)
		);
		// Step 3: Use the block hash from the last block as the parent hash
		let parent_hash = last_block_header.block_hash;

		let new_block_sign_payload = BlockSignPayload {
			block_number,
			parent_hash,
			block_type: BlockType::CIPHERTokenBlock, /* Putting this as a default right now.
			                                          * Needs
			                                          * added logic */
			cluster_address: block_proposer.cluster_address.clone(),
			transactions: transactions.clone(),
		};

		// Step 4: Hash the new_block_sign_payload to get the block hash for the new block
		let new_block_sign_payload_bytes: Vec<u8> =
			match bincode::serialize(&new_block_sign_payload) {
				Ok(val) => val,
				Err(err) =>
					return Err(anyhow!(
						"Error converting new_block_sign_payload to bytes: {:?}",
						err
					)),
			};

		let block_hash = self.compute_block_hash(&new_block_sign_payload_bytes); // Implement this function to compute the block hash
		let num_transactions = i32::try_from((&transactions).len()).unwrap_or(i32::MAX);
		let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
		// Step 5: Create and store the new BlockHeader
		let new_block_header = BlockHeader {
			block_number,
			block_hash,
			parent_hash,
			block_type: BlockType::CIPHERTokenBlock, /* Putting this as a default right now.
			                                          * Needs
			                                          * added logic */
			cluster_address: block_proposer.cluster_address,
			timestamp,
			num_transactions,
		};
		// Step 6: Store the block
		let new_block = Block { block_header: new_block_header.clone(), transactions };

		block_state.store_block(new_block.clone()).await?;
		Ok((new_block, new_block_header.clone()))
	}

	pub fn compute_block_hash(&self, data: &[u8]) -> BlockHash {
		let mut hasher = Sha256::new();
		hasher.update(data);
		let result = hasher.finalize();
		let hash_bytes = result.as_slice();
		let mut block_hash = BlockHash::default();
		block_hash.copy_from_slice(&hash_bytes[..]);
		block_hash
	}

	pub async fn validate_nonce(
		&self,
		transactions: Vec<Transaction>,
		account_state: &AccountState<'a>,
	) -> Result<Vec<Transaction>, Error> {
		// Step 1: Sort transactions based on verifying_key
		let mut transactions_by_key: HashMap<VerifyingKeyBytes, Vec<Transaction>> = HashMap::new();
		for transaction in transactions {
			transactions_by_key
				.entry(transaction.verifying_key.clone())
				.or_insert_with(Vec::new)
				.push(transaction);
		}

		// Step 2: Validate nonce sequence and keep only the correct ones
		let mut validated_transactions: Vec<Transaction> = Vec::new();
		for (verifying_key, mut txs) in transactions_by_key {
			txs.sort_by_key(|tx| tx.nonce);

			let mut prev_nonce =
				account_state.get_nonce(&Account::address(&verifying_key)?).await?;
			for tx in txs {
				if tx.nonce == prev_nonce + 1 {
					validated_transactions.push(tx.clone());
					prev_nonce = tx.nonce;
				}
				// If the nonce is not sequential, ignore this transaction
				// and assume that the next transactions with higher nonces are also invalid.
				else {
					break
				}
			}
		}

		Ok(validated_transactions)
	}
}
