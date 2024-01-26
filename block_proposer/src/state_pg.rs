use anyhow::Error;
use async_trait::async_trait;
use bigdecimal::{BigDecimal, ToPrimitive};
use db::postgres::{
	pg_models::{NewBlockProposer, QueryBlockProposer},
	postgres::{PgConnectionType, PostgresDBConn},
	schema,
};
use db_traits::{base::BaseState, block_proposer::BlockProposerState};
use diesel::sql_types::Numeric;

use diesel::{self, prelude::*, QueryResult};
use primitives::{Address, BlockNumber};
use std::collections::HashMap;
use system::block_proposer::BlockProposer;
use util::convert::convert_to_big_decimal_block_number;

pub struct StatePg<'a> {
	pub(crate) pg: &'a PostgresDBConn<'a>,
}

#[async_trait]
// impl BaseState<BlockProposer> for StatePg {
impl<'a> BaseState<BlockProposer> for StatePg<'a> {
	async fn create_table(&self) -> Result<(), Error> {
		// Implementation for create_table method
		Ok(())
	}

	async fn create(&self, block_proposer: &BlockProposer) -> Result<(), Error> {
		// Implementation for create method

		let new_blk_proposer = NewBlockProposer {
			address: hex::encode(block_proposer.address),
			cluster_address: hex::encode(block_proposer.cluster_address),
			block_number: convert_to_big_decimal_block_number(block_proposer.block_number),
			selected_next: Some(false),
		};

		match &self.pg.conn {
			PgConnectionType::TxConn(conn) => diesel::insert_into(schema::block_proposer::table)
				.values(new_blk_proposer)
				.execute(*conn.lock().await),
			PgConnectionType::PgConn(conn) => diesel::insert_into(schema::block_proposer::table)
				.values(new_blk_proposer)
				.execute(&mut *conn.lock().await),
		};
		Ok(())
	}

	async fn update(&self, _block_proposer: &BlockProposer) -> Result<(), Error> {
		// Implementation for update method
		Ok(())
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
		// Implementation for set_schema_version method
		Ok(())
	}
}
#[async_trait]
impl<'a> BlockProposerState for StatePg<'a> {
	async fn store_block_proposer(
		&self,
		cluster_address: Address,
		block_number: BlockNumber,
		address: Address,
	) -> Result<(), Error> {
		let new_blk_proposer = BlockProposer { cluster_address, address, block_number };
		self.create(&new_blk_proposer).await?;
		Ok(())
	}

	async fn load_selectors_block_numbers(
		&self,
		address: &Address,
	) -> Result<Option<Vec<BlockNumber>>, Error> {
		let encoded_address = hex::encode(address);
		let mut block_numbers = Vec::new();

		let res: QueryResult<Vec<QueryBlockProposer>> = match &self.pg.conn {
			PgConnectionType::TxConn(conn) => schema::block_proposer::table
				.filter(schema::block_proposer::dsl::address.eq(encoded_address))
				.load(*conn.lock().await),
			PgConnectionType::PgConn(conn) => schema::block_proposer::table
				.filter(schema::block_proposer::dsl::address.eq(encoded_address))
				.load(&mut *conn.lock().await),
		};

		match res {
			Ok(results) => {
				for query_results in results {
					let block_number_u128: u128 = match query_results.block_number.to_u128() {
						Some(u128_val) => u128_val,
						None => return Err(anyhow::anyhow!("Failed to convert BigDecimal to u128")),
					};
					block_numbers.push(block_number_u128);
				}
				Ok(Some(block_numbers))
			},
			Err(e) => return Err(anyhow::anyhow!("Diesel query failed: {}", e)),
		}
	}

	async fn update_selected_next(
		&self,
		selector_address: &Address,
		cluster_address: &Address,
	) -> Result<(), Error> {
		if let Some(block_numbers) = self.load_selectors_block_numbers(selector_address).await? {
			for block_number in block_numbers {
				let block_number = convert_to_big_decimal_block_number(block_number);
				let cluster_address = hex::encode(cluster_address);
				let address = hex::encode(selector_address);

				match &self.pg.conn {
					PgConnectionType::TxConn(conn) => {
						diesel::update(
							schema::block_proposer::table
								.filter(
									schema::block_proposer::dsl::cluster_address
										.eq(cluster_address),
								)
								.filter(schema::block_proposer::dsl::address.eq(address))
								.filter(schema::block_proposer::dsl::block_number.eq(block_number)),
						)
						.set(schema::block_proposer::dsl::selected_next.eq(&true)) // set new values for balance and nonce
						.execute(*conn.lock().await)
					},
					PgConnectionType::PgConn(conn) => {
						diesel::update(
							schema::block_proposer::table
								.filter(
									schema::block_proposer::dsl::cluster_address
										.eq(cluster_address),
								)
								.filter(schema::block_proposer::dsl::address.eq(address))
								.filter(schema::block_proposer::dsl::block_number.eq(block_number)),
						)
						.set(schema::block_proposer::dsl::selected_next.eq(&true)) // set new values for balance and nonce
						.execute(&mut *conn.lock().await)
					},
				}?;
			}
		}
		Ok(())
	}

	async fn store_block_proposers(
		&self,
		block_proposers: &HashMap<Address, HashMap<BlockNumber, Address>>,
		selector_address: Option<Address>,
		cluster_address: Option<Address>,
	) -> Result<(), Error> {
		for (cluster_address, block_numbers) in block_proposers {
			for (block_number, address) in block_numbers {
				self.store_block_proposer(cluster_address.clone(), *block_number, address.clone())
					.await?;
			}
		}
		if let Some(selector_address) = selector_address {
			if let Some(cluster_address) = cluster_address {
				self.update_selected_next(&selector_address, &cluster_address).await?;
			}
		}
		Ok(())
	}

	async fn load_last_block_proposer_block_number(
		&self,
		cluster_address: Address,
	) -> Result<BlockNumber, Error> {
		// Implementation for set_schema_version method
		let encoded_cluster_address = hex::encode(cluster_address);

		let max_block_number: Option<BigDecimal> = match &self.pg.conn {
			PgConnectionType::TxConn(conn) => schema::block_proposer::table
				.filter(schema::block_proposer::dsl::cluster_address.eq(encoded_cluster_address))
				.select(diesel::dsl::sql::<Numeric>("MAX(block_number)"))
				.first(*conn.lock().await)
				.optional(),
			PgConnectionType::PgConn(conn) => schema::block_proposer::table
				.filter(schema::block_proposer::dsl::cluster_address.eq(encoded_cluster_address))
				.select(diesel::dsl::sql::<Numeric>("MAX(block_number)"))
				.first(&mut *conn.lock().await)
				.optional(),
		}?;

		let block_number_u128: u128 = match max_block_number {
			Some(decimal) => match decimal.to_u128() {
				Some(u128_val) => u128_val,
				None => return Err(anyhow::anyhow!("Failed to convert BigDecimal to u128")),
			},
			None => return Err(anyhow::anyhow!("Block number is None")),
		};

		Ok(block_number_u128)
	}

	async fn load_block_proposer(
		&self,
		cluster_addr: Address,
		block_numbr: BlockNumber,
	) -> Result<Option<BlockProposer>, Error> {
		use db::postgres::schema::block_proposer::dsl::*;
		// Implementation for set_schema_version method
		let encoded_cluster_address = hex::encode(cluster_addr);
		let block_num = convert_to_big_decimal_block_number(block_numbr);

		let res: QueryResult<QueryBlockProposer> = match &self.pg.conn {
			PgConnectionType::TxConn(conn) => block_proposer
				.filter(
					db::postgres::schema::block_proposer::cluster_address
						.eq(&encoded_cluster_address)
						.and(db::postgres::schema::block_proposer::block_number.eq(&block_num)),
				)
				.first(*conn.lock().await),
			PgConnectionType::PgConn(conn) => block_proposer
				.filter(
					db::postgres::schema::block_proposer::cluster_address
						.eq(&encoded_cluster_address)
						.and(db::postgres::schema::block_proposer::block_number.eq(&block_num)),
				)
				.first(&mut *conn.lock().await),
		};

		match res {
			Ok(results) => {
				let block_number_u128: u128 = match results.block_number.to_u128() {
					Some(u128_val) => u128_val,
					None => return Err(anyhow::anyhow!("Failed to convert BigDecimal to u128")),
				};
				let mut cluster_addr: Address = [0; 20];
				let mut bp_address: Address = [0; 20];

				let cluster = hex::decode(&results.cluster_address.clone())?;
				if cluster.len() == 20 {
					let mut array = [0u8; 20];
					array.copy_from_slice(&cluster);
					cluster_addr = array;
				}

				let addr = hex::decode(&results.address.clone())?;
				if addr.len() == 20 {
					let mut array = [0u8; 20];
					array.copy_from_slice(&addr);
					bp_address = array;
				}
				let block_proposer_data = BlockProposer {
					cluster_address: cluster_addr,
					block_number: block_number_u128,
					address: bp_address,
				};

				println!("{:?}", block_proposer_data);
				Ok(Some(block_proposer_data))
			},
			Err(e) => return Err(anyhow::anyhow!("Diesel query failed: {}", e)),
		}
	}

	async fn load_block_proposers(
		&self,
		cluster_addr: &Address,
	) -> Result<HashMap<BlockNumber, BlockProposer>, Error> {
		use db::postgres::schema::block_proposer::dsl::*;
		// Implementation for set_schema_version method
		let mut block_proposers = HashMap::new();

		let encoded_cluster_address = hex::encode(cluster_addr);

		let res: QueryResult<Vec<QueryBlockProposer>> = match &self.pg.conn {
			PgConnectionType::TxConn(conn) => block_proposer
				.filter(cluster_address.eq(encoded_cluster_address))
				.load(*conn.lock().await),
			PgConnectionType::PgConn(conn) => block_proposer
				.filter(cluster_address.eq(encoded_cluster_address))
				.load(&mut *conn.lock().await),
		};

		match res {
			Ok(results) => {
				for query_result in results {
					let block_number_u128: u128 = match query_result.block_number.to_u128() {
						Some(u128_val) => u128_val,
						None => return Err(anyhow::anyhow!("Failed to convert BigDecimal to u128")),
					};

					let mut cluster_addr: Address = [0; 20];
					let mut addrss: Address = [0; 20];

					let cluster = hex::decode(&query_result.cluster_address.clone())?;
					if cluster.len() == 20 {
						let mut array = [0u8; 20];
						array.copy_from_slice(&cluster);
						cluster_addr = array;
					}

					let addr = hex::decode(&query_result.address.clone())?;
					if addr.len() == 20 {
						let mut array = [0u8; 20];
						array.copy_from_slice(&addr);
						addrss = array;
					}

					block_proposers.insert(
						block_number_u128,
						BlockProposer {
							cluster_address: cluster_addr,
							block_number: block_number_u128,
							address: addrss,
						},
					);
				}
				Ok(block_proposers)
			},
			Err(e) => return Err(anyhow::anyhow!("Diesel query failed: {}", e)),
		}
	}

	async fn find_cluster_address(&self, address: Address) -> Result<Address, Error> {
		let encoded_address = hex::encode(address);
		let result: QueryResult<String> = match &self.pg.conn {
			PgConnectionType::TxConn(conn) => schema::block_proposer::table
				.select(schema::block_proposer::dsl::cluster_address)
				.filter(schema::block_proposer::dsl::address.eq(&encoded_address))
				.get_result::<String>(*conn.lock().await),
			PgConnectionType::PgConn(conn) => schema::block_proposer::table
				.select(schema::block_proposer::dsl::cluster_address)
				.filter(schema::block_proposer::dsl::address.eq(&encoded_address))
				.get_result::<String>(&mut *conn.lock().await),
		};

		match result {
			Ok(res) => {
				let mut cluster_addr: Address = [0; 20];
				let cluster = hex::decode(&res)?;
				if cluster.len() == 20 {
					cluster_addr.copy_from_slice(&cluster);
				}
				Ok(cluster_addr)
			},
			Err(e) => Err(anyhow::anyhow!("Diesel query failed: {}", e)),
		}
	}
	async fn is_selected_next(&self, address: &Address) -> Result<bool, Error> {
		let encoded_address = hex::encode(address);
		let count: i64 = match &self.pg.conn {
			PgConnectionType::TxConn(conn) => schema::block_proposer::table
				.filter(
					schema::block_proposer::dsl::address
						.eq(encoded_address)
						.and(schema::block_proposer::dsl::selected_next.eq(false)),
				)
				.count()
				.get_result(*conn.lock().await),
			PgConnectionType::PgConn(conn) => schema::block_proposer::table
				.filter(
					schema::block_proposer::dsl::address
						.eq(encoded_address)
						.and(schema::block_proposer::dsl::selected_next.eq(false)),
				)
				.count()
				.get_result(&mut *conn.lock().await),
		}?;

		Ok(!(count > 0))
	}
}
