use crate::execute_transaction::ExecuteTransaction;
use anyhow::{anyhow, Error};
use async_scoped::TokioScope;
use block::block_state::BlockState;
use db::{
	db::{Database, DbConnType, DbTxConn},
	postgres::postgres::{PgConnectionType, PostgresDBConn},
};
use diesel::{prelude::*, result::Error as DieselError, serialize::IsNull::No};
use log::info;
use primitives::*;
use std::{result, sync::Arc};
use system::{account::Account, block::Block, network::EventBroadcast, transaction::Transaction};
use tokio::sync::{broadcast, Mutex};

pub struct ExecuteBlock {}

impl<'a> ExecuteBlock {
	pub async fn execute_block(
		block: &Block,
		event_tx: broadcast::Sender<EventBroadcast>,
		db_pool_conn: &'a DbTxConn<'a>,
	) -> Result<Vec<EventData>, Error> {
		let block_number = block.block_header.block_number;
		let cluster_address = block.block_header.cluster_address.clone();
		info!("Beginning execution of block: {:?}", block_number);

		{
			let block_state = BlockState::new(&db_pool_conn).await?;
			if block_state.is_block_executed(block_number, &cluster_address).await? {
				//return Err(anyhow!("Block already executed for block_number: {}", block_number))
				return Ok(vec![])
			}
		}

		// Sort transactions by nonce
		let mut transactions = block.transactions.clone();
		transactions.sort_by_key(|transaction| transaction.nonce);
		let mut events = vec![];
		for transaction in &transactions {
			let event = Self::execute_transaction(
				transaction,
				&cluster_address,
				block.block_header.block_number,
				&block.block_header.block_hash,
				block.block_header.timestamp,
				db_pool_conn,
				event_tx.clone(),
			)
			.await?;
			if let Some(event) = event {
				events.push(event);
			}
		}
		let block_state = BlockState::new(&db_pool_conn).await?;
		block_state.set_block_executed(block_number, &cluster_address).await?;
		Ok(events)
	}

	pub async fn execute_transaction(
		transaction: &Transaction,
		cluster_address: &Address,
		block_number: BlockNumber,
		block_hash: &BlockHash,
		block_timestamp: BlockTimeStamp,
		db_pool_conn: &'a DbTxConn<'a>,
		event_tx: broadcast::Sender<EventBroadcast>,
	) -> Result<Option<EventData>, Error> {
		let account_address = Account::address(&transaction.verifying_key)?;
		let mut event = Some(vec![]);
		/*event = ExecuteTransaction::execute_transaction(
			&transaction,
			&account_address,
			&cluster_address,
			block_number,
			block_hash,
			block_timestamp,
			Arc::new(&db_pool_conn),
			event_tx.clone(),
		)
		.await?;*/
		let new_db_conn = Database::get_connection().await?;
		match new_db_conn.db {
			DbConnType::POSTGRES(mut pg) => {
				let (tx, rx) = tokio::sync::oneshot::channel();
				let mut new_db_pool_conn = Database::get_postgres_connection().await?;
				let event1 =
					match pg.conn.transaction::<_, DieselError, _>(|conn: &mut PgConnection| {
						let result = TokioScope::scope_and_block(|scope| {
							scope.spawn_blocking(move || {
								let p_conn = PostgresDBConn {
									conn: PgConnectionType::TxConn(Arc::new(Mutex::new(conn))),
									config: pg.config.clone(),
								};
								let db_tx_conn = DbTxConn::POSTGRES(p_conn);
								let new_db_pool_conn = PostgresDBConn {
									conn: PgConnectionType::TxConn(Arc::new(Mutex::new(
										&mut new_db_pool_conn.conn,
									))),
									config: pg.config,
								};
								let new_db_pool_conn = DbTxConn::POSTGRES(new_db_pool_conn);
								tokio::runtime::Runtime::new()
									.expect("error creating pg execute block runtime")
									.block_on(async {
										let result = ExecuteTransaction::execute_transaction(
											&transaction,
											&account_address,
											&cluster_address,
											block_number,
											block_hash,
											block_timestamp,
											Arc::new(&db_tx_conn),
											Arc::new(&new_db_pool_conn),
											event_tx.clone(),
										)
										.await;
										info!("result: {:?}", result);
										result
									})
							})
						});
						let (result, event) = result.1.into_iter().next().unwrap().unwrap();
						match result {
							Ok(_) => {
								info!("execute_transaction no error: {:?}", event.clone());
								info!(
									"execute_transaction no error: {:?}",
									hex::encode(event.clone().unwrap())
								);
								Ok(event)
							},
							Err(e) => {
								info!("execute_transaction error: {:?}", e);
								tx.send(event).expect("execute_transaction tx send error");
								Err(DieselError::RollbackTransaction)
							},
						}
					}) {
						Ok(event) => event,
						Err(e) => {
							info!("RollbackTransaction error: {:?}", e);
							Some(rx.await.expect("execute_transaction tx recv error").unwrap())
						},
					};
				event = event1;
			},
			DbConnType::CASSANDRA(session) => {
				let db_tx_conn = Arc::new(db_pool_conn);
				let (result, event1) = ExecuteTransaction::execute_transaction(
					&transaction,
					&account_address,
					&cluster_address,
					block_number,
					block_hash,
					block_timestamp,
					db_tx_conn.clone(),
					db_tx_conn.clone(),
					event_tx.clone(),
				)
				.await;
				event = event1;
				match result {
					Ok(_) => {},
					Err(e) => return Err(e),
				}
			},
			DbConnType::ROCKSDB(db_path) => {
				let db_tx_conn = Arc::new(db_pool_conn);
				let (result, event1) = ExecuteTransaction::execute_transaction(
					&transaction,
					&account_address,
					&cluster_address,
					block_number,
					block_hash,
					block_timestamp,
					db_tx_conn.clone(),
					db_tx_conn.clone(),
					event_tx.clone(),
				)
				.await;
				event = event1;
				match result {
					Ok(_) => {},
					Err(e) => return Err(e),
				}
			},
		};
		info!("execute_block: event: {:?}", event);
		Ok(event)
	}
}
