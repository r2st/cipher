use anyhow::{anyhow, Error};
use async_trait::async_trait;
use bigdecimal::ToPrimitive;
use chrono::NaiveDateTime;
use cipher_rpc::rpc_model::{self, TransactionResponse};
use db::postgres::{
	pg_models::{
		NewBlockHead, NewBlockHeader, NewBlockMetaInfo, NewBlockTransactions, QueryBlockHead,
		QueryBlockHeader, QueryBlockTransactions,
	},
	postgres::{PgConnectionType, PostgresDBConn},
};
use db_traits::{base::BaseState, block::BlockState};
use diesel::{self, prelude::*};
use log::info;
use primitives::{
	Address, BlockHash, BlockNumber, BlockTimeStamp, TransactionHash, TransactionSequence,
};
use system::block::BlockType;

use system::{
	account::Account,
	block::{Block, BlockResponse},
	block_header::BlockHeader,
	chain_state::ChainState,
	transaction::Transaction,
	transaction_receipt::TransactionReceiptResponse,
};
use util::{
	convert::{
		convert_to_big_decimal_balance, convert_to_big_decimal_block_number,
		convert_to_big_decimal_tx_sequence, to_proto_transaction,
	},
	generic::{
		block_timestamp_to_naive_datetime, convert_to_naive_datetime, get_postgres_timestamp,
	},
};

pub struct StatePg<'a> {
	pub(crate) pg: &'a PostgresDBConn<'a>,
}

#[async_trait]
// impl BaseState<Block> for StatePg {
impl<'a> BaseState<Block> for StatePg<'a> {
	async fn create_table(&self) -> Result<(), Error> {
		Ok(())
	}

	async fn create(&self, block: &Block) -> Result<(), Error> {
		self.store_block(block.clone()).await?;
		Ok(())
	}

	async fn update(&self, _block: &Block) -> Result<(), Error> {
		todo!()
	}

	async fn raw_query(&self, query: &str) -> Result<(), Error> {
		match &self.pg.conn {
			PgConnectionType::TxConn(conn) => diesel::sql_query(query).execute(*conn.lock().await),
			PgConnectionType::PgConn(conn) =>
				diesel::sql_query(query).execute(&mut *conn.lock().await),
		}?;
		Ok(())
	}

	async fn set_schema_version(&self, _version: u32) -> Result<(), Error> {
		todo!()
	}
}

#[async_trait]
impl<'a> BlockState for StatePg<'a> {
	async fn store_block_head(
		&self,
		_cluster_address: &Address,
		_block_number: BlockNumber,
		_block_hash: BlockHash,
	) -> Result<(), Error> {
		use db::postgres::schema::block_head::dsl::*;
		let block_num = convert_to_big_decimal_block_number(_block_number);
		let blk_hash = hex::encode(_block_hash);
		let cluster_add = hex::encode(_cluster_address);
		let new_block_head = NewBlockHead {
			cluster_address: cluster_add,
			block_number: Some(block_num),
			block_hash: Some(blk_hash),
		};

		match &self.pg.conn {
			PgConnectionType::TxConn(conn) => diesel::insert_into(block_head)
				.values(new_block_head)
				.on_conflict(cluster_address)
				.do_nothing()
				//.set((block_number.eq(excluded(block_number)), block_hash.eq(excluded(block_hash))))
				.execute(*conn.lock().await),
			PgConnectionType::PgConn(conn) => diesel::insert_into(block_head)
				.values(new_block_head)
				.on_conflict(cluster_address)
				.do_nothing()
				//.set((block_number.eq(excluded(block_number)), block_hash.eq(excluded(block_hash))))
				.execute(&mut *conn.lock().await),
		}?;
		Ok(())
	}

	async fn update_block_head(
		&self,
		_cluster_address: Address,
		_big_block_number: BlockNumber,
		_block_hash: BlockHash,
	) -> Result<(), Error> {
		use db::postgres::schema::block_head::dsl::*;
		if !self.is_block_head(&_cluster_address).await? {
			self.store_block_head(&_cluster_address, _big_block_number, _block_hash).await?
		} else {
			let cluster_add = hex::encode(_cluster_address);
			let block_num = convert_to_big_decimal_block_number(_big_block_number);
			let blk_hash = hex::encode(_block_hash);

			match &self.pg.conn {
				PgConnectionType::TxConn(conn) => {
					diesel::update(block_head.filter(cluster_address.eq(cluster_add)))
						.set((block_hash.eq(blk_hash), block_number.eq(block_num))) // set new values for balance and nonce
						.execute(*conn.lock().await)
				},
				PgConnectionType::PgConn(conn) => {
					diesel::update(block_head.filter(cluster_address.eq(cluster_add)))
						.set((block_hash.eq(blk_hash), block_number.eq(block_num))) // set new values for balance and nonce
						.execute(&mut *conn.lock().await)
				},
			}?;
		}
		Ok(())
	}

	async fn load_chain_state(&self, _cluster_address: Address) -> Result<ChainState, Error> {
		use db::postgres::schema::block_head::dsl::*;
		let cluster_addr = hex::encode(_cluster_address);

		// Execute Diesel query
		let res: Result<Vec<QueryBlockHead>, diesel::result::Error> = match &self.pg.conn {
			PgConnectionType::TxConn(conn) => block_head
				.filter(cluster_address.eq(cluster_addr.clone()))
				.load(*conn.lock().await),
			PgConnectionType::PgConn(conn) => block_head
				.filter(cluster_address.eq(cluster_addr.clone()))
				.load(&mut *conn.lock().await),
		};

		match res {
			Ok(query_results) => {
				if let Some(query_block_head) = query_results.first() {
					let mut cluster_add: Address = [0; 20];
					let mut blockhash: BlockHash = [0; 32];

					let _block_number =
						query_block_head.block_number.as_ref().unwrap().to_u128().unwrap();
					let vec = hex::decode(
						query_block_head.block_hash.clone().unwrap_or_else(|| "".to_string()),
					)?;

					if vec.len() == 32 {
						let mut array = [0u8; 32];
						array.copy_from_slice(&vec);
						blockhash = array;
					}

					// decode cluster address from string to byter array
					let vec = hex::decode(&cluster_addr.clone())?;
					if vec.len() == 20 {
						let mut array = [0u8; 20];
						array.copy_from_slice(&vec);
						cluster_add = array;
					}
					Ok(ChainState {
						cluster_address: cluster_add,
						block_number: _block_number,
						block_hash: blockhash,
					})
				} else {
					println!("No matching records found for cluster address ");
					Ok(ChainState {
						cluster_address: _cluster_address,
						block_number: 0,
						block_hash: [0u8; 32],
					})
				}
			},
			Err(e) => Err(anyhow::anyhow!("Diesel query failed: {}", e)),
		}
	}

	async fn block_head(&self, cluster_address: Address) -> Result<Block, Error> {
		let chain_state = self.load_chain_state(cluster_address).await?;
		self.load_block(chain_state.block_number, &cluster_address).await
	}

	async fn block_head_header(&self, cluster_address: Address) -> Result<BlockHeader, Error> {
		let chain_state = self.load_chain_state(cluster_address).await?;
		self.load_block_header(chain_state.block_number, &cluster_address).await
	}

	async fn store_block(&self, block: Block) -> Result<(), Error> {
		use db::postgres::schema::block_meta_info::dsl::*;
		let block_num = convert_to_big_decimal_block_number(block.block_header.block_number);
		let _num_transactions = u32::try_from((&block.transactions).len()).unwrap_or(u32::MAX);
		println!(
			"ℹ️ STORING BLOCK #{}, CLUSTER ADDRESS 0x{:?}",
			block_num, block.block_header.cluster_address
		);

		let cluster_add = hex::encode(block.block_header.cluster_address);
		let new_block_meta_info = NewBlockMetaInfo {
			cluster_address: cluster_add,
			block_number: Some(block_num),
			block_executed: Some(false),
		};
		let transactions = block.transactions.clone();
		for (_tx_sequence, _transaction) in transactions.iter().enumerate() {
			if let Err(e) = self
				.store_transaction(
					block.block_header.block_number.clone(),
					block.block_header.block_hash.clone(),
					_transaction.clone(),
					_tx_sequence as TransactionSequence,
				)
				.await
			{
				eprintln!("Failed to store transaction: {:?}", e);
				// Optionally, you can decide to do something else here, like collecting errors
			}
		}
		match self.store_block_header(block.block_header.clone()).await {
			Ok(_) => {
				println!("block header interted successfully")
			},
			Err(e) => return Err(anyhow!("Failed to store_block_header: {:?}", e)), /* return Err(anyhow::anyhow!("Failed to get a database connection from the pool")), */
		};
		if let Err(e) = self
			.update_block_head(
				block.block_header.cluster_address,
				block.block_header.block_number,
				block.block_header.block_hash.clone(),
			)
			.await
		{
			eprintln!("Failed to update block head: {:?}", e);
			// Handle the error, e.g., by returning it or taking some recovery action
			return Err(e);
		}
		// let conn: &mut PooledConnection<ConnectionManager<PgConnection>> =
		// 	&mut *self.pg.conn.pool_conn.lock().await;
		match &self.pg.conn {
			PgConnectionType::TxConn(conn) => {
				match diesel::insert_into(block_meta_info)
					.values(new_block_meta_info.clone())
					.on_conflict((cluster_address, block_number))
					.do_nothing()
					.execute(*conn.lock().await)
				{
					Ok(_) => Ok(()),
					Err(e) => Err(anyhow::anyhow!(
						"Failed to store_block::block_meta_info: {} : {:?}",
						e,
						new_block_meta_info
					)),
				}
			},
			PgConnectionType::PgConn(conn) => {
				match diesel::insert_into(block_meta_info)
					.values(new_block_meta_info.clone())
					.on_conflict((cluster_address, block_number))
					.do_nothing()
					.execute(&mut *conn.lock().await)
				{
					Ok(_) => Ok(()),
					Err(e) => Err(anyhow::anyhow!(
						"Failed to store_block::block_meta_info: {} : {:?}",
						e,
						new_block_meta_info
					)),
				}
			},
		}?;
		Ok(())
	}

	/// Store a batch of blocks. This should be more efficient than storing them one by one.
	/// Assumes that the blocks are already sorted by block number.
	async fn batch_store_blocks(&self, blocks: Vec<Block>) -> Result<(), Error> {
		use db::postgres::schema::block_meta_info::dsl::*;

		let last_block = match blocks.last() {
			Some(block) => block.clone(),
			None => return Err(anyhow!("No blocks to store")),
		};

		let mut all_block_transactions: Vec<(BlockNumber, BlockHash, Transaction)> = vec![];
		let mut block_headers: Vec<BlockHeader> = vec![];
		let mut new_block_meta_infos: Vec<NewBlockMetaInfo> = vec![];

		for block in blocks {
			let block_num = convert_to_big_decimal_block_number(block.block_header.block_number);
			let _num_transactions = u32::try_from((&block.transactions).len()).unwrap_or(u32::MAX);
			info!(
				"ℹ️ STORING BLOCK #{}, CLUSTER ADDRESS 0x{:?}",
				block_num, block.block_header.cluster_address
			);

			let cluster_add = hex::encode(block.block_header.cluster_address);
			let new_block_meta_info = NewBlockMetaInfo {
				cluster_address: cluster_add,
				block_number: Some(block_num),
				block_executed: Some(false),
			};
			let transactions = block.transactions.clone();
			for transaction in transactions {
				all_block_transactions.push((
					block.block_header.block_number,
					block.block_header.block_hash,
					transaction,
				));
			}

			block_headers.push(block.block_header);
			new_block_meta_infos.push(new_block_meta_info);
		}

		self.batch_store_transactions(all_block_transactions).await?;
		self.batch_store_block_headers(block_headers).await?;

		match &self.pg.conn {
			PgConnectionType::TxConn(conn) => diesel::insert_into(block_meta_info)
				.values(new_block_meta_infos.clone())
				.on_conflict((cluster_address, block_number))
				.do_nothing()
				.execute(*conn.lock().await),
			PgConnectionType::PgConn(conn) => diesel::insert_into(block_meta_info)
				.values(new_block_meta_infos.clone())
				.on_conflict((cluster_address, block_number))
				.do_nothing()
				.execute(&mut *conn.lock().await),
		}?;

		// Update block_head
		self.update_block_head(
			last_block.block_header.cluster_address,
			last_block.block_header.block_number,
			last_block.block_header.block_hash.clone(),
		)
		.await?;

		Ok(())
	}

	async fn store_transaction(
		&self,
		_block_number: BlockNumber,
		_block_hash: BlockHash,
		_transaction: Transaction,
		_tx_sequence: TransactionSequence,
	) -> Result<(), Error> {
		use db::postgres::schema::block_transaction::dsl::*;
		let block_num = convert_to_big_decimal_block_number(_block_number);
		let fee_used_bytes = convert_to_big_decimal_balance(_transaction.fee_limit);
		let transaction_bytes: Vec<u8> = bincode::serialize(&_transaction)?;
		let _transaction_hash = hex::encode(_transaction.transaction_hash()?);
		let _from_address = Account::address(&_transaction.verifying_key)?;
		let _timestamp = get_postgres_timestamp()?;
		let _tx_sequence = convert_to_big_decimal_tx_sequence(_tx_sequence);

		let new_transaction = NewBlockTransactions {
			transaction_hash: _transaction_hash,
			block_number: Some(block_num),
			block_hash: Some(hex::encode(_block_hash)),
			fee_used: Some(fee_used_bytes),
			from_address: Some(hex::encode(_from_address)),
			timestamp: Some(convert_to_naive_datetime(_timestamp)),
			transaction: Some(transaction_bytes),
			tx_sequence: Some(_tx_sequence),
		};
		match &self.pg.conn {
			PgConnectionType::TxConn(conn) => diesel::insert_into(block_transaction)
				.values(new_transaction)
				.on_conflict((transaction_hash, block_number))
				.do_nothing()
				.execute(*conn.lock().await),
			PgConnectionType::PgConn(conn) => diesel::insert_into(block_transaction)
				.values(new_transaction)
				.on_conflict((transaction_hash, block_number))
				.do_nothing()
				.execute(&mut *conn.lock().await),
		}?;
		Ok(())
	}

	/// Store a batch of transactions. This should be more efficient than storing them one by one.
	async fn batch_store_transactions(
		&self,
		transactions: Vec<(BlockNumber, BlockHash, Transaction)>,
	) -> Result<(), Error> {
		use db::postgres::schema::block_transaction::dsl::*;

		let mut new_transactions: Vec<NewBlockTransactions> = vec![];

		for (_tx_sequence, (_block_number, _block_hash, _transaction)) in
			transactions.iter().enumerate()
		{
			let block_num = convert_to_big_decimal_block_number(_block_number.clone());
			let fee_used_bytes = convert_to_big_decimal_balance(_transaction.fee_limit);
			let transaction_bytes: Vec<u8> = bincode::serialize(&_transaction)?;
			let _transaction_hash = hex::encode(_transaction.transaction_hash()?);
			let _from_address = Account::address(&_transaction.verifying_key)?;
			let _timestamp = get_postgres_timestamp()?;
			let _tx_sequence =
				convert_to_big_decimal_tx_sequence(_tx_sequence as TransactionSequence);

			let new_transaction = NewBlockTransactions {
				transaction_hash: _transaction_hash,
				block_number: Some(block_num),
				block_hash: Some(hex::encode(_block_hash)),
				fee_used: Some(fee_used_bytes),
				from_address: Some(hex::encode(_from_address)),
				timestamp: Some(convert_to_naive_datetime(_timestamp)),
				transaction: Some(transaction_bytes),
				tx_sequence: Some(_tx_sequence),
			};

			new_transactions.push(new_transaction);
		}

		// let conn: &mut PooledConnection<ConnectionManager<PgConnection>> =
		// 	&mut *self.pg.conn.pool_conn.lock().await;
		match &self.pg.conn {
			PgConnectionType::TxConn(conn) => diesel::insert_into(block_transaction)
				.values(new_transactions)
				.on_conflict((transaction_hash, block_number))
				.do_nothing()
				.execute(*conn.lock().await),
			PgConnectionType::PgConn(conn) => diesel::insert_into(block_transaction)
				.values(new_transactions)
				.on_conflict((transaction_hash, block_number))
				.do_nothing()
				.execute(&mut *conn.lock().await),
		}?;
		Ok(())
	}

	async fn get_transaction_count(
		&self,
		verifying_key: &Address,
		_block_number: Option<BlockNumber>,
	) -> Result<u64, Error> {
		use db::postgres::schema::block_transaction::dsl::*;

		let addr = hex::encode(verifying_key);
		let tx_count: i64 = match _block_number {
			Some(_block_number) => {
				let block_num = convert_to_big_decimal_block_number(_block_number.clone());
				match &self.pg.conn {
					PgConnectionType::TxConn(conn) => block_transaction
						.filter(from_address.eq(addr.clone()))
						.filter(block_number.le(block_num))
						.count()
						.get_result(*conn.lock().await),
					PgConnectionType::PgConn(conn) => block_transaction
						.filter(from_address.eq(addr.clone()))
						.filter(block_number.le(block_num))
						.count()
						.get_result(&mut *conn.lock().await),
				}?
			},
			None => match &self.pg.conn {
				PgConnectionType::TxConn(conn) => block_transaction
					.filter(from_address.eq(addr.clone()))
					.count()
					.get_result(*conn.lock().await),
				PgConnectionType::PgConn(conn) => block_transaction
					.filter(from_address.eq(addr.clone()))
					.count()
					.get_result(&mut *conn.lock().await),
			}?,
		};
		Ok(tx_count as u64)
	}

	async fn load_block(
		&self,
		_block_number: BlockNumber,
		_cluster_address: &Address,
	) -> Result<Block, Error> {
		let block_header: BlockHeader =
			self.load_block_header(_block_number, _cluster_address).await?;
		let transactions: Vec<Transaction> = self.load_transactions(_block_number).await?;
		Ok(Block { block_header, transactions })
	}

	async fn load_block_response(
		&self,
		_block_number: BlockNumber,
		_cluster_address: &Address,
	) -> Result<BlockResponse, Error> {
		let block_header: BlockHeader =
			self.load_block_header(_block_number, _cluster_address).await?;
		let transactions: Vec<TransactionReceiptResponse> =
			self.load_transactions_response(_block_number).await?;
		Ok(BlockResponse { block_header, transactions })
	}

	async fn store_block_header(&self, blockheader: BlockHeader) -> Result<(), Error> {
		use db::postgres::schema::block_header::dsl::*;
		let block_num = convert_to_big_decimal_block_number(blockheader.block_number);
		let blockhash = hex::encode(blockheader.block_hash);
		let parenthash = hex::encode(blockheader.parent_hash);
		let _timestamp = blockheader.timestamp;
		let blocktype: i8 = match blockheader.block_type {
			BlockType::CIPHERTokenBlock => 0,
			BlockType::CIPHERContractBlock => 1,
			BlockType::XTalkTokenBlock => 2,
			BlockType::XTalkContractBlock => 3,
			BlockType::SuperBlock => 4,
		};
		let new_header = NewBlockHeader {
			block_number: block_num,
			block_hash: Some(blockhash),
			parent_hash: Some(parenthash),
			timestamp: Some(block_timestamp_to_naive_datetime(_timestamp)),
			block_type: Some(blocktype as i16),
			num_transactions: Some(blockheader.num_transactions),
			cluster_address: hex::encode(blockheader.cluster_address),
		};
		match &self.pg.conn {
			PgConnectionType::TxConn(conn) => diesel::insert_into(block_header)
				.values(new_header)
				.on_conflict(block_number)
				.do_nothing()
				.execute(*conn.lock().await),
			PgConnectionType::PgConn(conn) => diesel::insert_into(block_header)
				.values(new_header)
				.on_conflict(block_number)
				.do_nothing()
				.execute(&mut *conn.lock().await),
		}?;
		Ok(())
	}

	async fn batch_store_block_headers(
		&self,
		block_headers: Vec<BlockHeader>,
	) -> Result<(), Error> {
		use db::postgres::schema::block_header::dsl::*;
		let mut new_headers: Vec<NewBlockHeader> = vec![];

		for blockheader in block_headers {
			let block_num = convert_to_big_decimal_block_number(blockheader.block_number);
			let blockhash = hex::encode(blockheader.block_hash);
			let parenthash = hex::encode(blockheader.parent_hash);
			let _timestamp = get_postgres_timestamp()?;
			let blocktype: i8 = match blockheader.block_type {
				BlockType::CIPHERTokenBlock => 0,
				BlockType::CIPHERContractBlock => 1,
				BlockType::XTalkTokenBlock => 2,
				BlockType::XTalkContractBlock => 3,
				BlockType::SuperBlock => 4,
			};
			let new_header = NewBlockHeader {
				block_number: block_num,
				block_hash: Some(blockhash),
				parent_hash: Some(parenthash),
				timestamp: Some(convert_to_naive_datetime(_timestamp)),
				block_type: Some(blocktype as i16),
				num_transactions: Some(blockheader.num_transactions),
				cluster_address: hex::encode(blockheader.cluster_address),
			};

			new_headers.push(new_header);
		}

		match &self.pg.conn {
			PgConnectionType::TxConn(conn) => diesel::insert_into(block_header)
				.values(new_headers)
				.on_conflict(block_number)
				.do_nothing()
				.execute(*conn.lock().await),
			PgConnectionType::PgConn(conn) => diesel::insert_into(block_header)
				.values(new_headers)
				.on_conflict(block_number)
				.do_nothing()
				.execute(&mut *conn.lock().await),
		}?;
		Ok(())
	}

	async fn load_block_header(
		&self,
		_block_number: BlockNumber,
		_cluster_address: &Address,
	) -> Result<BlockHeader, Error> {
		use db::postgres::schema::block_header::dsl::*;
		let block_num = convert_to_big_decimal_block_number(_block_number);
		// let conn: &mut PooledConnection<ConnectionManager<PgConnection>> =
		// 	&mut *self.pg.conn.pool_conn.lock().await;

		let res: Result<Vec<QueryBlockHeader>, diesel::result::Error> = match &self.pg.conn {
			PgConnectionType::TxConn(conn) =>
				block_header.filter(block_number.eq(block_num.clone())).load(*conn.lock().await),
			PgConnectionType::PgConn(conn) => block_header
				.filter(block_number.eq(block_num.clone()))
				.load(&mut *conn.lock().await),
		};
		match res {
			Ok(query_results) => {
				if let Some(query_header) = query_results.first() {
					let mut cluster_add: Address = [0; 20];
					let mut blockhash: BlockHash = [0; 32];
					let mut parenthash = [0; 32];
					let bh = hex::decode(
						query_header.block_hash.clone().unwrap_or_else(|| "".to_string()),
					)?;

					if bh.len() == 32 {
						let mut array = [0u8; 32];
						array.copy_from_slice(&bh);
						blockhash = array;
					}
					let ph = hex::decode(
						query_header.parent_hash.clone().unwrap_or_else(|| "".to_string()),
					)?;
					if ph.len() == 32 {
						let mut array = [0u8; 32];
						array.copy_from_slice(&ph);
						parenthash = array;
					}

					let ch = hex::decode(query_header.cluster_address.clone().unwrap())?;
					if ch.len() == 20 {
						let mut array = [0u8; 20];
						array.copy_from_slice(&ch);
						cluster_add = array;
					}

					let blocktype = match query_header.block_type.unwrap() {
						0 => BlockType::CIPHERTokenBlock,
						1 => BlockType::CIPHERContractBlock,
						2 => BlockType::XTalkTokenBlock,
						3 => BlockType::XTalkContractBlock,
						4 => BlockType::SuperBlock,
						_ => BlockType::CIPHERTokenBlock,
					};

					let blk_timestamp =
						query_header.timestamp.unwrap_or_else(|| NaiveDateTime::MAX).timestamp()
							as BlockTimeStamp;
					let blockheader = BlockHeader {
						block_number: _block_number,
						block_hash: blockhash,
						parent_hash: parenthash,
						block_type: blocktype,
						cluster_address: cluster_add,
						timestamp: blk_timestamp,
						num_transactions: query_header.num_transactions.unwrap(),
					};
					return Ok(blockheader);
				} else {
					return Err(anyhow::anyhow!(
						"No matching records found for block #{}",
						_block_number
					));
				};
			},
			Err(e) => return Err(anyhow::anyhow!("Diesel query failed: {}", e)),
		}
	}

	async fn load_transaction(
		&self,
		_transaction_hash: TransactionHash,
	) -> Result<Option<Transaction>, Error> {
		use db::postgres::schema::block_transaction::dsl::*;
		let trx_hash = hex::encode(_transaction_hash);
		// let conn: &mut PooledConnection<ConnectionManager<PgConnection>> =
		// 	&mut *self.pg.conn.pool_conn.lock().await;

		let res: Result<QueryBlockTransactions, diesel::result::Error> = match &self.pg.conn {
			PgConnectionType::TxConn(conn) => block_transaction
				.filter(transaction_hash.eq(&trx_hash))
				.first(*conn.lock().await),
			PgConnectionType::PgConn(conn) => block_transaction
				.filter(transaction_hash.eq(&trx_hash))
				.first(&mut *conn.lock().await),
		};
		match res {
			Ok(query_results) => {
				let sys_transaction: Transaction =
					bincode::deserialize(&query_results.transaction.unwrap())?;
				Ok(Some(sys_transaction))
			},
			Err(e) => return Err(anyhow::anyhow!("Diesel query failed: {}", e)),
		}
	}

	async fn load_transaction_receipt(
		&self,
		_transaction_hash: TransactionHash,
	) -> Result<Option<TransactionReceiptResponse>, Error> {
		use db::postgres::schema::block_transaction::dsl::*;
		let trx_hash = hex::encode(_transaction_hash);
		// let conn: &mut PooledConnection<ConnectionManager<PgConnection>> =
		// 	&mut *self.pg.conn.pool_conn.lock().await;

		let res: Result<QueryBlockTransactions, diesel::result::Error> = match &self.pg.conn {
			PgConnectionType::TxConn(conn) => block_transaction
				.filter(transaction_hash.eq(&trx_hash))
				.first(*conn.lock().await),
			PgConnectionType::PgConn(conn) => block_transaction
				.filter(transaction_hash.eq(&trx_hash))
				.first(&mut *conn.lock().await),
		};
		match res {
			Ok(query_results) => {
				let mut from_add: Address = [0; 20];
				let mut blockhash: BlockHash = [0; 32];
				let mut tx_hash = [0; 32];

				let bh = hex::decode(
					query_results.block_hash.clone().unwrap_or_else(|| "".to_string()),
				)?;

				if bh.len() == 32 {
					let mut array = [0u8; 32];
					array.copy_from_slice(&bh);
					blockhash = array;
				}

				let tx = hex::decode(query_results.transaction_hash.as_str())?;
				if tx.len() == 32 {
					let mut array = [0u8; 32];
					array.copy_from_slice(&tx);
					tx_hash = array;
				}

				let from = hex::decode(
					query_results.from_address.clone().unwrap_or_else(|| "".to_string()),
				)?;

				if from.len() == 20 {
					let mut array = [0u8; 20];
					array.copy_from_slice(&from);
					from_add = array;
				}

				let block_number_u128: u128 = match query_results.block_number.to_u128() {
					Some(u128_val) => u128_val,
					None => return Err(anyhow::anyhow!("Failed to convert BigDecimal to u128")),
				};

				let fee_used_u128: u128 = match query_results.fee_used {
					Some(decimal) => match decimal.to_u128() {
						Some(u128_val) => u128_val,
						None => return Err(anyhow::anyhow!("Failed to convert BigDecimal to u128")),
					},
					None => return Err(anyhow::anyhow!("Block number is None")),
				};

				let sys_transaction: Transaction =
					bincode::deserialize(&query_results.transaction.unwrap())?;
				let trx_recepit = TransactionReceiptResponse {
					transaction: sys_transaction,
					tx_hash,
					status: false,
					from: from_add,
					block_number: block_number_u128,
					block_hash: blockhash,
					fee_used: fee_used_u128,
					timestamp: 0,
				};

				Ok(Some(trx_recepit))
			},
			Err(e) => return Err(anyhow::anyhow!("Diesel query failed: {}", e)),
		}
	}

	async fn load_transaction_receipt_by_address(
		&self,
		_address: Address,
	) -> Result<Vec<TransactionReceiptResponse>, Error> {
		use db::postgres::schema::block_transaction::dsl::*;
		let addr = hex::encode(_address);

		let res: Result<Vec<QueryBlockTransactions>, diesel::result::Error> = match &self.pg.conn {
			PgConnectionType::TxConn(conn) =>
				block_transaction.filter(from_address.eq(&addr)).load(*conn.lock().await),
			PgConnectionType::PgConn(conn) =>
				block_transaction.filter(from_address.eq(&addr)).load(&mut *conn.lock().await),
		};
		let mut transactions: Vec<TransactionReceiptResponse> = vec![];
		match res {
			Ok(query_res) => {
				for query_results in query_res {
					let mut from_add: Address = [0; 20];
					let mut blockhash: BlockHash = [0; 32];
					let mut tx_hash = [0; 32];

					let bh = hex::decode(
						query_results.block_hash.clone().unwrap_or_else(|| "".to_string()),
					)?;

					if bh.len() == 32 {
						let mut array = [0u8; 32];
						array.copy_from_slice(&bh);
						blockhash = array;
					}

					let tx = hex::decode(query_results.transaction_hash.as_str())?;
					if tx.len() == 32 {
						let mut array = [0u8; 32];
						array.copy_from_slice(&tx);
						tx_hash = array;
					}

					let from = hex::decode(
						query_results.from_address.clone().unwrap_or_else(|| "".to_string()),
					)?;

					if from.len() == 20 {
						let mut array = [0u8; 20];
						array.copy_from_slice(&from);
						from_add = array;
					}

					let block_number_u128: u128 = match query_results.block_number.to_u128() {
						Some(u128_val) => u128_val,
						None => return Err(anyhow::anyhow!("Failed to convert BigDecimal to u128")),
					};

					let fee_used_u128: u128 = match query_results.fee_used {
						Some(decimal) => match decimal.to_u128() {
							Some(u128_val) => u128_val,
							None =>
								return Err(anyhow::anyhow!("Failed to convert BigDecimal to u128")),
						},
						None => return Err(anyhow::anyhow!("Block number is None")),
					};

					let sys_transaction: Transaction =
						bincode::deserialize(&query_results.transaction.unwrap())?;

					let trx_recepit = TransactionReceiptResponse {
						transaction: sys_transaction,
						tx_hash,
						status: true,
						from: from_add,
						block_number: block_number_u128,
						block_hash: blockhash,
						fee_used: fee_used_u128,
						timestamp: 0,
					};

					transactions.push(trx_recepit)
				}
				Ok(transactions)
			},
			Err(e) => return Err(anyhow::anyhow!("Diesel query failed: {}", e)),
		}
	}

	async fn load_latest_block_headers(
		&self,
		num_blocks: u32,
		_cluster_address: Address,
	) -> Result<Vec<cipher_rpc::rpc_model::BlockHeader>, Error> {
		use db::postgres::schema::block_header::dsl::*;

		let current_block_number = self.load_chain_state(_cluster_address).await?.block_number;
		let lower_block_number = current_block_number - num_blocks as u128;
		let lower_block_number = convert_to_big_decimal_block_number(lower_block_number);

		// let conn: &mut PooledConnection<ConnectionManager<PgConnection>> =
		// 	&mut *self.pg.conn.pool_conn.lock().await;
		let results: Result<Vec<QueryBlockHeader>, diesel::result::Error> = match &self.pg.conn {
			PgConnectionType::TxConn(conn) => block_header
				.filter(block_number.gt(lower_block_number))
				.load(*conn.lock().await),
			PgConnectionType::PgConn(conn) => block_header
				.filter(block_number.gt(lower_block_number))
				.load(&mut *conn.lock().await),
		};
		let mut blockheader: Vec<cipher_rpc::rpc_model::BlockHeader> = vec![];
		match results {
			Ok(query) => {
				for query_header in query {
					let block_number_u64 = query_header
						.block_number
						.to_u64()
						.expect("Failed to convert block_number to u64");
					let header = cipher_rpc::rpc_model::BlockHeader {
						block_number: block_number_u64,
						block_hash: query_header
							.block_hash
							.clone()
							.unwrap_or_else(|| "".to_string()),
						parent_hash: query_header
							.parent_hash
							.clone()
							.unwrap_or_else(|| "".to_string()),
						block_type: query_header.block_type.unwrap_or(0) as i32,
						cluster_address: query_header.cluster_address.unwrap(),
						num_transactions: query_header.num_transactions.unwrap() as u32,
						timestamp: 0,
					};

					blockheader.push(header)
				}
				return Ok(blockheader);
			},
			Err(e) => return Err(anyhow::anyhow!("Diesel query failed: {}", e)),
		}
	}

	async fn load_latest_transactions(
		&self,
		num_transactions: u32,
	) -> Result<Vec<TransactionResponse>, Error> {
		use db::postgres::schema::block_transaction::dsl::*;

		let num_transactions = i32::try_from(num_transactions).unwrap_or(i32::MAX);
		let results = {
			let results: Result<Vec<QueryBlockTransactions>, diesel::result::Error> =
				match &self.pg.conn {
					PgConnectionType::TxConn(conn) =>
						block_transaction.limit(num_transactions as i64).load(*conn.lock().await),
					PgConnectionType::PgConn(conn) => block_transaction
						.limit(num_transactions as i64)
						.load(&mut *conn.lock().await),
				};
			results
		};

		let mut transactions: Vec<TransactionResponse> = vec![];
		match results {
			Ok(query_results) => {
				for query_trx in query_results {
					let _from_add: Address = [0; 20];
					let _blockhash: BlockHash = [0; 32];
					let _tx_hash = [0; 32];
					let blockhash = hex::decode(
						query_trx.block_hash.clone().unwrap_or_else(|| "".to_string()),
					)?;

					let tx_hash = hex::decode(query_trx.transaction_hash.as_str())?;

					let from_add = hex::decode(
						query_trx.from_address.clone().unwrap_or_else(|| "".to_string()),
					)?;

					let block_number_i64: i64 = match query_trx.block_number.to_i64() {
						Some(u128_val) => u128_val,
						None => return Err(anyhow::anyhow!("Failed to convert BigDecimal to u128")),
					};

					let _fee_used: String = match query_trx.fee_used {
						Some(decimal) => decimal.to_string(),
						None => return Err(anyhow::anyhow!("Block number is None")),
					};

					let sys_transaction: Transaction =
						bincode::deserialize(&query_trx.transaction.unwrap())?;
					let rpc_transaction: rpc_model::Transaction =
						to_proto_transaction(sys_transaction).await?;
					let response = TransactionResponse {
						transaction: Some(rpc_transaction),
						transaction_hash: tx_hash,
						from: from_add,
						block_number: block_number_i64,
						block_hash: blockhash,
						fee_used: _fee_used,
						timestamp: 0,
					};

					transactions.push(response);
				}
				Ok(transactions)
			},
			Err(e) => return Err(anyhow::anyhow!("Diesel query failed: {}", e)),
		}
	}

	async fn load_transactions(
		&self,
		_block_number: BlockNumber,
	) -> Result<Vec<Transaction>, Error> {
		use db::postgres::schema::block_transaction::dsl::*;
		let block_num = convert_to_big_decimal_block_number(_block_number);

		let res: Result<Vec<QueryBlockTransactions>, diesel::result::Error> = match &self.pg.conn {
			PgConnectionType::TxConn(conn) => block_transaction
				.filter(block_number.eq(block_num.clone()))
				.order(tx_sequence.asc())
				.load(*conn.lock().await),
			PgConnectionType::PgConn(conn) => block_transaction
				.filter(block_number.eq(block_num.clone()))
				.order(tx_sequence.asc())
				.load(&mut *conn.lock().await),
		};

		let mut transactions: Vec<Transaction> = vec![];
		match res {
			Ok(query_results) =>
				for query_trx in query_results {
					let sys_transaction: Transaction = match query_trx.transaction {
						Some(ref vec) => bincode::deserialize(vec)?,
						None => return Err(anyhow::anyhow!("Transaction data is None")),
					};
					transactions.push(sys_transaction);
				},
			Err(e) => return Err(anyhow::anyhow!("Diesel query failed: {}", e)),
		};
		Ok(transactions)
	}

	async fn load_transactions_response(
		&self,
		_block_number: BlockNumber,
	) -> Result<Vec<TransactionReceiptResponse>, Error> {
		use db::postgres::schema::block_transaction::dsl::*;
		let block_num = convert_to_big_decimal_block_number(_block_number);

		let res: Result<Vec<QueryBlockTransactions>, diesel::result::Error> = match &self.pg.conn {
			PgConnectionType::TxConn(conn) => block_transaction
				.filter(block_number.eq(block_num.clone()))
				.load(*conn.lock().await),
			PgConnectionType::PgConn(conn) => block_transaction
				.filter(block_number.eq(block_num.clone()))
				.load(&mut *conn.lock().await),
		};

		let mut transactions: Vec<TransactionReceiptResponse> = vec![];
		match res {
			Ok(query_results) => {
				for query_trx in query_results {
					let mut from_add: Address = [0; 20];
					let mut blockhash: BlockHash = [0; 32];
					let mut tx_hash = [0; 32];
					let bh = hex::decode(
						query_trx.block_hash.clone().unwrap_or_else(|| "".to_string()),
					)?;

					if bh.len() == 32 {
						let mut array = [0u8; 32];
						array.copy_from_slice(&bh);
						blockhash = array;
					}

					let tx = hex::decode(query_trx.transaction_hash.as_str())?;
					if tx.len() == 32 {
						let mut array = [0u8; 32];
						array.copy_from_slice(&tx);
						tx_hash = array;
					}

					let from = hex::decode(
						query_trx.from_address.clone().unwrap_or_else(|| "".to_string()),
					)?;

					if from.len() == 20 {
						let mut array = [0u8; 20];
						array.copy_from_slice(&from);
						from_add = array;
					}

					let block_number_u128: u128 = match query_trx.block_number.to_u128() {
						Some(u128_val) => u128_val,
						None => return Err(anyhow::anyhow!("Failed to convert BigDecimal to u128")),
					};

					let fee_used_u128: u128 = match query_trx.fee_used {
						Some(decimal) => match decimal.to_u128() {
							Some(u128_val) => u128_val,
							None =>
								return Err(anyhow::anyhow!("Failed to convert BigDecimal to u128")),
						},
						None => return Err(anyhow::anyhow!("Block number is None")),
					};

					let sys_transaction: Transaction = match query_trx.transaction {
						Some(ref vec) => bincode::deserialize(vec)?,
						None => return Err(anyhow::anyhow!("Transaction data is None")),
					};
					let response = TransactionReceiptResponse {
						transaction: sys_transaction,
						tx_hash,
						status: true,
						from: from_add,
						block_number: block_number_u128,
						block_hash: blockhash,
						fee_used: fee_used_u128,
						timestamp: 0,
					};

					transactions.push(response);
				}
				Ok(transactions)
			},
			Err(e) => return Err(anyhow::anyhow!("Diesel query failed: {}", e)),
		}
	}

	async fn is_block_head(&self, _cluster_address: &Address) -> Result<bool, Error> {
		use db::postgres::schema::block_head::dsl::*;
		let cluster_addr = hex::encode(_cluster_address);
		// let conn: &mut PooledConnection<ConnectionManager<PgConnection>> =
		// 	&mut *self.pg.conn.pool_conn.lock().await;

		let res = match &self.pg.conn {
			PgConnectionType::TxConn(conn) => block_head
				.filter(cluster_address.eq(cluster_addr))
				.load::<QueryBlockHead>(*conn.lock().await),
			PgConnectionType::PgConn(conn) => block_head
				.filter(cluster_address.eq(cluster_addr))
				.load::<QueryBlockHead>(&mut *conn.lock().await),
		}?;

		Ok(!res.is_empty())
	}

	async fn is_block_executed(
		&self,
		_block_number: BlockNumber,
		_cluster_address: &Address,
	) -> Result<bool, Error> {
		use db::postgres::schema::block_meta_info::dsl::*;
		let cluster_addr = hex::encode(_cluster_address);

		let block_num = convert_to_big_decimal_block_number(_block_number);

		let result = match &self.pg.conn {
			PgConnectionType::TxConn(conn) => block_meta_info
				.select(block_executed)
				.filter(cluster_address.eq(cluster_addr))
				.filter(block_number.eq(block_num))
				.first::<Option<bool>>(*conn.lock().await),
			PgConnectionType::PgConn(conn) => block_meta_info
				.select(block_executed)
				.filter(cluster_address.eq(cluster_addr))
				.filter(block_number.eq(block_num))
				.first::<Option<bool>>(&mut *conn.lock().await),
		};

		match result {
			Ok(Some(executed)) => Ok(executed),
			Ok(None) => Ok(false),   // or Err(...) if you expect a row to always exist
			Err(e) => Err(e.into()), // Convert the error into your Error type
		}
	}

	async fn set_block_executed(
		&self,
		_block_number: BlockNumber,
		_cluster_address: &Address,
	) -> Result<(), Error> {
		use db::postgres::schema::block_meta_info::dsl::*;
		let block_num = convert_to_big_decimal_block_number(_block_number);
		let cluster_add = hex::encode(_cluster_address);
		match &self.pg.conn {
			PgConnectionType::TxConn(conn) => diesel::update(
				block_meta_info
					.filter(cluster_address.eq(cluster_add))
					.filter(block_number.eq(block_num)),
			)
			.set(block_executed.eq(true)) // set new values for balance and nonce
			.execute(*conn.lock().await),
			PgConnectionType::PgConn(conn) => diesel::update(
				block_meta_info
					.filter(cluster_address.eq(cluster_add))
					.filter(block_number.eq(block_num)),
			)
			.set(block_executed.eq(true)) // set new values for balance and nonce
			.execute(&mut *conn.lock().await),
		}?;

		Ok(())
	}
}
