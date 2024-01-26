use crate::{state_cas::StateCas, state_pg::StatePg, state_rock::StateRock};
use anyhow::Error;
use db::db::DbTxConn;
use db_traits::{base::BaseState, block::BlockState as BlockStateInternal};

use cipher_rpc::rpc_model::{self, TransactionResponse};
use primitives::*;
use rocksdb::DB;
use std::sync::Arc;
use system::{
	block::{Block, BlockResponse},
	block_header::BlockHeader,
	chain_state::ChainState,
	transaction::Transaction,
	transaction_receipt::TransactionReceiptResponse,
};

pub enum StateInternalImpl<'a> {
	StateRock(StateRock),
	StatePg(StatePg<'a>),
	StateCas(StateCas),
}

pub struct BlockState<'a> {
	pub state: Arc<StateInternalImpl<'a>>,
}

impl<'a> BlockState<'a> {
	pub async fn new(db_pool_conn: &'a DbTxConn<'a>) -> Result<Self, Error> {
		let state: StateInternalImpl<'a>;

		match &db_pool_conn {
			DbTxConn::POSTGRES(pg) => {
				state = StateInternalImpl::StatePg(StatePg { pg });
			},
			DbTxConn::CASSANDRA(session) => {
				state = StateInternalImpl::StateCas(StateCas { session: session.clone() });
			},
			DbTxConn::ROCKSDB(db_path) => {
				let db_path = format!("{}/block", db_path);
				state = StateInternalImpl::StateRock(StateRock {
					db_path: db_path.clone(),
					db: DB::open_default(db_path)?,
				});
			},
		}

		let state = BlockState { state: Arc::new(state) };

		state.create_table().await?;
		Ok(state)
	}

	pub async fn create_table(&self) -> Result<(), Error> {
		match &*self.state {
			StateInternalImpl::StateRock(s) => s.create_table().await,
			StateInternalImpl::StatePg(s) => s.create_table().await,
			StateInternalImpl::StateCas(s) => s.create_table().await,
		}
	}

	pub async fn raw_query(&self, query: &str) -> Result<(), Error> {
		match &*self.state {
			StateInternalImpl::StateRock(s) => s.raw_query(query).await,
			StateInternalImpl::StatePg(s) => s.raw_query(query).await,
			StateInternalImpl::StateCas(s) => s.raw_query(query).await,
		}
	}

	pub async fn store_block_head(
		&self,
		cluster_address: &Address,
		block_number: BlockNumber,
		block_hash: BlockHash,
	) -> Result<(), Error> {
		match &*self.state {
			StateInternalImpl::StateRock(s) =>
				s.store_block_head(cluster_address, block_number, block_hash).await,
			StateInternalImpl::StatePg(s) =>
				s.store_block_head(cluster_address, block_number, block_hash).await,
			StateInternalImpl::StateCas(s) =>
				s.store_block_head(cluster_address, block_number, block_hash).await,
		}
	}

	pub async fn batch_store_block_headers(
		&self,
		block_headers: Vec<BlockHeader>,
	) -> Result<(), Error> {
		match &*self.state {
			StateInternalImpl::StateRock(s) => s.batch_store_block_headers(block_headers).await,
			StateInternalImpl::StatePg(s) => s.batch_store_block_headers(block_headers).await,
			StateInternalImpl::StateCas(s) => s.batch_store_block_headers(block_headers).await,
		}
	}

	pub async fn update_block_head(
		&self,
		cluster_address: Address,
		big_block_number: BlockNumber,
		block_hash: BlockHash,
	) -> Result<(), Error> {
		match &*self.state {
			StateInternalImpl::StateRock(s) =>
				s.update_block_head(cluster_address, big_block_number, block_hash).await,
			StateInternalImpl::StatePg(s) =>
				s.update_block_head(cluster_address, big_block_number, block_hash).await,
			StateInternalImpl::StateCas(s) =>
				s.update_block_head(cluster_address, big_block_number, block_hash).await,
		}
	}

	pub async fn load_chain_state(&self, cluster_address: Address) -> Result<ChainState, Error> {
		match &*self.state {
			StateInternalImpl::StateRock(s) => s.load_chain_state(cluster_address).await,
			StateInternalImpl::StatePg(s) => s.load_chain_state(cluster_address).await,
			StateInternalImpl::StateCas(s) => s.load_chain_state(cluster_address).await,
		}
	}

	pub async fn block_head(&self, cluster_address: Address) -> Result<Block, Error> {
		match &*self.state {
			StateInternalImpl::StateRock(s) => s.block_head(cluster_address).await,
			StateInternalImpl::StatePg(s) => s.block_head(cluster_address).await,
			StateInternalImpl::StateCas(s) => s.block_head(cluster_address).await,
		}
	}

	pub async fn block_head_header(&self, cluster_address: Address) -> Result<BlockHeader, Error> {
		match &*self.state {
			StateInternalImpl::StateRock(s) => s.block_head_header(cluster_address).await,
			StateInternalImpl::StatePg(s) => s.block_head_header(cluster_address).await,
			StateInternalImpl::StateCas(s) => s.block_head_header(cluster_address).await,
		}
	}

	pub async fn store_block(&self, block: Block) -> Result<(), Error> {
		match &*self.state {
			StateInternalImpl::StateRock(s) => s.store_block(block).await,
			StateInternalImpl::StatePg(s) => s.store_block(block).await,
			StateInternalImpl::StateCas(s) => s.store_block(block).await,
		}
	}

	/// Store a batch of blocks. This should be more efficient than storing them one by one.
	/// Assumes that the blocks are already sorted by block number.
	pub async fn batch_store_blocks(&self, blocks: Vec<Block>) -> Result<(), Error> {
		match &*self.state {
			StateInternalImpl::StateRock(_s) => todo!(),
			StateInternalImpl::StatePg(s) => s.batch_store_blocks(blocks).await,
			StateInternalImpl::StateCas(s) => s.batch_store_blocks(blocks).await,
		}
	}

	pub async fn store_transaction(
		&self,
		block_number: BlockNumber,
		block_hash: BlockHash,
		transaction: Transaction,
		tx_sequence: TransactionSequence,
	) -> Result<(), Error> {
		match &*self.state {
			StateInternalImpl::StateRock(s) =>
				s.store_transaction(block_number, block_hash, transaction, tx_sequence).await,
			StateInternalImpl::StatePg(s) =>
				s.store_transaction(block_number, block_hash, transaction, tx_sequence).await,
			StateInternalImpl::StateCas(s) =>
				s.store_transaction(block_number, block_hash, transaction, tx_sequence).await,
		}
	}

	pub async fn batch_store_transactions(
		&self,
		transactions: Vec<(BlockNumber, BlockHash, Transaction)>,
	) -> Result<(), Error> {
		match &*self.state {
			StateInternalImpl::StateRock(s) => s.batch_store_transactions(transactions).await,
			StateInternalImpl::StatePg(s) => s.batch_store_transactions(transactions).await,
			StateInternalImpl::StateCas(s) => s.batch_store_transactions(transactions).await,
		}
	}

	// FIXME: mitigate "ALLOW FILTERING" with partition keys/secondary indexes
	pub async fn get_transaction_count(
		&self,
		verifying_key: &Address,
		block_number: Option<BlockNumber>,
	) -> Result<u64, Error> {
		match &*self.state {
			StateInternalImpl::StateRock(s) =>
				s.get_transaction_count(verifying_key, block_number).await,
			StateInternalImpl::StatePg(s) =>
				s.get_transaction_count(verifying_key, block_number).await,
			StateInternalImpl::StateCas(s) =>
				s.get_transaction_count(verifying_key, block_number).await,
		}
	}

	pub async fn load_block(
		&self,
		block_number: BlockNumber,
		cluster_address: &Address,
	) -> Result<Block, Error> {
		match &*self.state {
			StateInternalImpl::StateRock(s) => s.load_block(block_number, cluster_address).await,
			StateInternalImpl::StatePg(s) => s.load_block(block_number, cluster_address).await,
			StateInternalImpl::StateCas(s) => s.load_block(block_number, cluster_address).await,
		}
	}

	pub async fn load_block_response(
		&self,
		block_number: BlockNumber,
		cluster_address: &Address,
	) -> Result<BlockResponse, Error> {
		match &*self.state {
			StateInternalImpl::StateRock(s) =>
				s.load_block_response(block_number, cluster_address).await,
			StateInternalImpl::StatePg(s) =>
				s.load_block_response(block_number, cluster_address).await,
			StateInternalImpl::StateCas(s) =>
				s.load_block_response(block_number, cluster_address).await,
		}
	}

	pub async fn store_block_header(&self, block_header: BlockHeader) -> Result<(), Error> {
		match &*self.state {
			StateInternalImpl::StateRock(s) => s.store_block_header(block_header).await,
			StateInternalImpl::StatePg(s) => s.store_block_header(block_header).await,
			StateInternalImpl::StateCas(s) => s.store_block_header(block_header).await,
		}
	}

	pub async fn load_block_header(
		&self,
		block_number: BlockNumber,
		cluster_address: &Address,
	) -> Result<BlockHeader, Error> {
		match &*self.state {
			StateInternalImpl::StateRock(s) =>
				s.load_block_header(block_number, cluster_address).await,
			StateInternalImpl::StatePg(s) =>
				s.load_block_header(block_number, cluster_address).await,
			StateInternalImpl::StateCas(s) =>
				s.load_block_header(block_number, cluster_address).await,
		}
	}

	pub async fn load_transaction(
		&self,
		transaction_hash: TransactionHash,
	) -> Result<Option<Transaction>, Error> {
		match &*self.state {
			StateInternalImpl::StateRock(s) => s.load_transaction(transaction_hash).await,
			StateInternalImpl::StatePg(s) => s.load_transaction(transaction_hash).await,
			StateInternalImpl::StateCas(s) => s.load_transaction(transaction_hash).await,
		}
	}

	pub async fn load_transaction_receipt(
		&self,
		transaction_hash: TransactionHash,
	) -> Result<Option<TransactionReceiptResponse>, Error> {
		match &*self.state {
			StateInternalImpl::StateRock(s) => s.load_transaction_receipt(transaction_hash).await,
			StateInternalImpl::StatePg(s) => s.load_transaction_receipt(transaction_hash).await,
			StateInternalImpl::StateCas(s) => s.load_transaction_receipt(transaction_hash).await,
		}
	}

	pub async fn load_transaction_receipt_by_address(
		&self,
		address: Address,
	) -> Result<Vec<TransactionReceiptResponse>, Error> {
		match &*self.state {
			StateInternalImpl::StateRock(s) => s.load_transaction_receipt_by_address(address).await,
			StateInternalImpl::StatePg(s) => s.load_transaction_receipt_by_address(address).await,
			StateInternalImpl::StateCas(s) => s.load_transaction_receipt_by_address(address).await,
		}
	}

	/// Load the latest x number of block headers
	pub async fn load_latest_block_headers(
		&self,
		num_blocks: u32,
		cluster_address: Address,
	) -> Result<Vec<rpc_model::BlockHeader>, Error> {
		match &*self.state {
			StateInternalImpl::StateRock(s) =>
				s.load_latest_block_headers(num_blocks, cluster_address).await,
			StateInternalImpl::StatePg(s) =>
				s.load_latest_block_headers(num_blocks, cluster_address).await,
			StateInternalImpl::StateCas(s) =>
				s.load_latest_block_headers(num_blocks, cluster_address).await,
		}
	}

	/// Load the latest x number of transactions
	pub async fn load_latest_transactions(
		&self,
		num_transactions: u32,
	) -> Result<Vec<TransactionResponse>, Error> {
		match &*self.state {
			StateInternalImpl::StateRock(s) => s.load_latest_transactions(num_transactions).await,
			StateInternalImpl::StatePg(s) => s.load_latest_transactions(num_transactions).await,
			StateInternalImpl::StateCas(s) => s.load_latest_transactions(num_transactions).await,
		}
	}

	/// Load the transactions for a given block number
	pub async fn load_transactions(
		&self,
		block_number: BlockNumber,
	) -> Result<Vec<Transaction>, Error> {
		match &*self.state {
			StateInternalImpl::StateRock(s) => s.load_transactions(block_number).await,
			StateInternalImpl::StatePg(s) => s.load_transactions(block_number).await,
			StateInternalImpl::StateCas(s) => s.load_transactions(block_number).await,
		}
	}

	/// Load the transactions for a given block number
	pub async fn load_transactions_response(
		&self,
		block_number: BlockNumber,
	) -> Result<Vec<TransactionReceiptResponse>, Error> {
		match &*self.state {
			StateInternalImpl::StateRock(s) => s.load_transactions_response(block_number).await,
			StateInternalImpl::StatePg(s) => s.load_transactions_response(block_number).await,
			StateInternalImpl::StateCas(s) => s.load_transactions_response(block_number).await,
		}
	}

	pub async fn is_block_head(&self, cluster_address: &Address) -> Result<bool, Error> {
		match &*self.state {
			StateInternalImpl::StateRock(s) => s.is_block_head(cluster_address).await,
			StateInternalImpl::StatePg(s) => s.is_block_head(cluster_address).await,
			StateInternalImpl::StateCas(s) => s.is_block_head(cluster_address).await,
		}
	}

	pub async fn is_block_executed(
		&self,
		block_number: BlockNumber,
		cluster_address: &Address,
	) -> Result<bool, Error> {
		match &*self.state {
			StateInternalImpl::StateRock(s) =>
				s.is_block_executed(block_number, cluster_address).await,
			StateInternalImpl::StatePg(s) =>
				s.is_block_executed(block_number, cluster_address).await,
			StateInternalImpl::StateCas(s) =>
				s.is_block_executed(block_number, cluster_address).await,
		}
	}

	pub async fn set_block_executed(
		&self,
		block_number: BlockNumber,
		cluster_address: &Address,
	) -> Result<(), Error> {
		match &*self.state {
			StateInternalImpl::StateRock(s) =>
				s.set_block_executed(block_number, cluster_address).await,
			StateInternalImpl::StatePg(s) =>
				s.set_block_executed(block_number, cluster_address).await,
			StateInternalImpl::StateCas(s) =>
				s.set_block_executed(block_number, cluster_address).await,
		}
	}
}
