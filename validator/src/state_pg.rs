use anyhow::Error;
use async_trait::async_trait;
use bigdecimal::{BigDecimal, ToPrimitive};
use db::postgres::{
	pg_models::{NewValidator, QueryValidator},
	postgres::{PgConnectionType, PostgresDBConn},
	schema,
};
use db_traits::{base::BaseState, validator::ValidatorState};
use diesel::{self, prelude::*};
use primitives::{Address, BlockNumber};
use system::validator::Validator;
use util::convert::convert_to_big_decimal_block_number;

pub struct StatePg<'a> {
	pub(crate) pg: &'a PostgresDBConn<'a>,
}

#[async_trait]
impl<'a> BaseState<Validator> for StatePg<'a> {
	async fn create_table(&self) -> Result<(), Error> {
		Ok(())
	}

	async fn create(&self, _validator: &Validator) -> Result<(), Error> {
		let block_num: BigDecimal = convert_to_big_decimal_block_number(_validator.block_number);
		let _stake: BigDecimal = convert_to_big_decimal_block_number(_validator.stake);

		let new_vlaidator = NewValidator {
			address: hex::encode(_validator.address),
			block_number: Some(block_num),
			cluster_address: Some(hex::encode(_validator.cluster_address)),
			stake: Some(_stake),
		};

		use db::postgres::schema::validator::dsl::*;

		// let conn: &mut PooledConnection<ConnectionManager<PgConnection>> =
		// 	&mut *self.pg.conn.pool_conn.lock().await;

		match &self.pg.conn {
			PgConnectionType::TxConn(conn) => diesel::insert_into(validator)
				.values(new_vlaidator)
				.on_conflict((address, block_number))
				.do_nothing()
				.execute(*conn.lock().await),
			PgConnectionType::PgConn(conn) => diesel::insert_into(validator)
				.values(new_vlaidator)
				.on_conflict((address, block_number))
				.do_nothing()
				.execute(&mut *conn.lock().await),
		}?;
		Ok(())
	}

	async fn update(&self, _validator: &Validator) -> Result<(), Error> {
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
impl<'a> ValidatorState for StatePg<'a> {
	async fn load_validator(&self, _address: &Address) -> Result<Validator, Error> {
		let encoded_address = hex::encode(_address);

		// let conn: &mut PooledConnection<ConnectionManager<PgConnection>> =
		// 	&mut *self.pg.conn.pool_conn.lock().await;
		let res: Result<QueryValidator, diesel::result::Error> = match &self.pg.conn {
			PgConnectionType::TxConn(conn) => schema::validator::table
				.filter(schema::validator::dsl::address.eq(encoded_address.clone()))
				.first::<QueryValidator>(*conn.lock().await),
			PgConnectionType::PgConn(conn) => schema::validator::table
				.filter(schema::validator::dsl::address.eq(encoded_address.clone()))
				.first::<QueryValidator>(&mut *conn.lock().await),
		};

		// let res: Result<QueryValidator, diesel::result::Error> = schema::validator::table
		// 	.filter(schema::validator::dsl::address.eq(encoded_address.clone()))
		// 	.first(*self.pg.conn.lock().await);

		match res {
			Ok(query_results) => {
				let mut cluster_add: Address = [0; 20];
				let mut address: Address = [0; 20];

				let cluster = hex::decode(
					query_results.cluster_address.clone().unwrap_or_else(|| "".to_string()),
				)?;

				if cluster.len() == 20 {
					let mut array = [0u8; 20];
					array.copy_from_slice(&cluster);
					cluster_add = array;
				}

				let addr = hex::decode(query_results.address.clone())?;

				if addr.len() == 20 {
					let mut array = [0u8; 20];
					array.copy_from_slice(&addr);
					address = array;
				}

				let block_number_u128: u128 = match query_results.block_number.to_u128() {
					Some(u128_val) => u128_val,
					None => return Err(anyhow::anyhow!("Failed to convert BigDecimal to u128")),
				};

				let stake_u128: u128 = match query_results.stake.unwrap().to_u128() {
					Some(u128_val) => u128_val,
					None => return Err(anyhow::anyhow!("Failed to convert BigDecimal to u128")),
				};
				let data = Validator {
					address,
					block_number: block_number_u128,
					stake: stake_u128,
					cluster_address: cluster_add,
				};

				Ok(data)
			},
			Err(e) => return Err(anyhow::anyhow!("Diesel query failed: {}", e)),
		}
	}

	async fn load_all_validators(
		&self,
		_block_number: BlockNumber,
	) -> Result<Option<Vec<Validator>>, Error> {
		use db::postgres::schema::validator::dsl::*;
		let block_num: BigDecimal = convert_to_big_decimal_block_number(_block_number);
		let mut validator_list: Vec<Validator> = vec![];

		// let conn: &mut PooledConnection<ConnectionManager<PgConnection>> =
		// 	&mut *self.pg.conn.pool_conn.lock().await;

		let res: Result<Vec<QueryValidator>, diesel::result::Error> = match &self.pg.conn {
			PgConnectionType::TxConn(conn) => validator
				.filter(block_number.eq(block_num.clone()))
				.load::<QueryValidator>(*conn.lock().await),
			PgConnectionType::PgConn(conn) => validator
				.filter(block_number.eq(block_num.clone()))
				.load::<QueryValidator>(&mut *conn.lock().await),
		};

		// let res: Result<Vec<QueryValidator>, diesel::result::Error> = validator
		// 	.filter(block_number.eq(block_num.clone()))
		// 	.load(*self.pg.conn.lock().await);

		match res {
			Ok(results) => {
				for query_results in results {
					let mut cluster_add: Address = [0; 20];
					let mut validator_address: Address = [0; 20];

					let cluster = hex::decode(
						query_results.cluster_address.clone().unwrap_or_else(|| "".to_string()),
					)?;

					if cluster.len() == 20 {
						let mut array = [0u8; 20];
						array.copy_from_slice(&cluster);
						cluster_add = array;
					}

					let addr = hex::decode(query_results.address.clone())?;

					if addr.len() == 20 {
						let mut array = [0u8; 20];
						array.copy_from_slice(&addr);
						validator_address = array;
					}

					let block_number_u128: u128 = match query_results.block_number.to_u128() {
						Some(u128_val) => u128_val,
						None => return Err(anyhow::anyhow!("Failed to convert BigDecimal to u128")),
					};

					let stake_u128: u128 = match query_results.stake.unwrap().to_u128() {
						Some(u128_val) => u128_val,
						None => return Err(anyhow::anyhow!("Failed to convert BigDecimal to u128")),
					};
					let data = Validator {
						address: validator_address,
						block_number: block_number_u128,
						stake: stake_u128,
						cluster_address: cluster_add,
					};

					validator_list.push(data)
				}

				Ok(Some(validator_list))
			},
			Err(e) => return Err(anyhow::anyhow!("Diesel query failed: {}", e)),
		}
	}

	async fn is_validator(
		&self,
		address: &Address,
		block_number: BlockNumber,
	) -> Result<bool, Error> {
		let block_num: BigDecimal = convert_to_big_decimal_block_number(block_number);
		let encoded_address = hex::encode(address);

		// let conn: &mut PooledConnection<ConnectionManager<PgConnection>> =
		// 	&mut *self.pg.conn.pool_conn.lock().await;

		let res = match &self.pg.conn {
			PgConnectionType::TxConn(conn) => schema::validator::table
				.filter(
					schema::validator::dsl::address
						.eq(encoded_address)
						.and(schema::validator::dsl::block_number.eq(block_num)),
				)
				.load::<QueryValidator>(*conn.lock().await),
			PgConnectionType::PgConn(conn) => schema::validator::table
				.filter(
					schema::validator::dsl::address
						.eq(encoded_address)
						.and(schema::validator::dsl::block_number.eq(block_num)),
				)
				.load::<QueryValidator>(&mut *conn.lock().await),
		}?;

		// let res = schema::validator::table
		// 	.filter(schema::validator::dsl::address .eq(encoded_address)
		// .and(schema::validator::dsl::block_number.eq(block_num)),) 	.load::<QueryValidator>(*self.
		// pg.conn.lock().await)?;

		Ok(!res.is_empty())
	}
}
