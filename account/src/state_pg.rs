extern crate anyhow;
use anyhow::{anyhow, Error, Result};
use async_trait::async_trait;
use bigdecimal::ToPrimitive;
use db::postgres::{
	pg_models::{self, AccountType, QueryAccount},
	postgres::{PgConnectionType, PostgresDBConn},
	schema::account::dsl as account_dsl,
};
use db_traits::{account::AccountState, base::BaseState};
use diesel::{self, prelude::*};
use hex;
use primitives::{Address, Balance, Nonce};
use system::account::Account;
use util::convert::convert_to_big_decimal_balance;

#[derive(Clone)]
pub struct StatePg<'a> {
	pub(crate) pg: &'a PostgresDBConn<'a>,
}

#[async_trait]
impl<'a> BaseState<Account> for StatePg<'a> {
	async fn create_table(&self) -> Result<(), Error> {
		Ok(())
	}

	async fn create(&self, acc: &Account) -> Result<(), Error> {
		use db::postgres::schema::account::dsl::*;
		let balance_u128 = convert_to_big_decimal_balance(acc.balance);
		let nonce_u128 = convert_to_big_decimal_balance(acc.nonce);
		let account_type_str = acc.account_type.as_str();
		let account_type_enum: AccountType = match account_type_str {
			"System" => AccountType::System,
			"User" => AccountType::User,
			_ => return Err(anyhow!("Invalid account type: {}", account_type_str)),
		};
		let new_account = pg_models::NewAccount {
			address: hex::encode(acc.address),
			account_type: Some(account_type_enum),
			balance: Some(balance_u128),
			nonce: Some(nonce_u128),
		};
		match &self.pg.conn {
			PgConnectionType::TxConn(conn) => {
				match diesel::insert_into(account).values(new_account).execute(*conn.lock().await) {
					Ok(_) => Ok(()),
					Err(e) => Err(anyhow::anyhow!("Failed to insert new account: {}", e)),
				}
			},
			PgConnectionType::PgConn(conn) => {
				match diesel::insert_into(account)
					.values(new_account)
					.execute(&mut *conn.lock().await)
				{
					Ok(_) => Ok(()),
					Err(e) => Err(anyhow::anyhow!("Failed to insert new account: {}", e)),
				}
			},
		}
	}

	async fn update(&self, acc: &Account) -> Result<(), Error> {
		use db::postgres::schema::account::dsl::*;

		if !self.is_valid_account(&acc.address).await? {
			self.create(&acc).await?
		} else {
			let new_balance = convert_to_big_decimal_balance(acc.balance);
			let new_nonce = convert_to_big_decimal_balance(acc.nonce);
			let addr = hex::encode(acc.address);
			match &self.pg.conn {
				PgConnectionType::TxConn(conn) => {
					diesel::update(account.filter(address.eq(addr)))
						.set((balance.eq(new_balance), nonce.eq(new_nonce))) // set new values for balance and nonce
						.execute(*conn.lock().await)?;
				},
				PgConnectionType::PgConn(conn) => {
					diesel::update(account.filter(address.eq(addr)))
						.set((balance.eq(new_balance), nonce.eq(new_nonce))) // set new values for balance and nonce
						.execute(&mut *conn.lock().await)?;
				},
			}
		}
		Ok(())
	}

	async fn raw_query(&self, query: &str) -> std::result::Result<(), Error> {
		match &self.pg.conn {
			PgConnectionType::TxConn(conn) => {
				diesel::sql_query(query).execute(*conn.lock().await)?;
			},
			PgConnectionType::PgConn(conn) => {
				diesel::sql_query(query).execute(&mut *conn.lock().await)?;
			},
		}
		Ok(())
	}

	async fn set_schema_version(&self, _version: u32) -> Result<(), Error> {
		todo!()
	}
}

#[async_trait]
impl<'a> AccountState for StatePg<'a> {
	async fn update_balance(&self, acc: &Account) -> std::result::Result<(), Error> {
		use db::postgres::schema::account::dsl::*;

		if !self.is_valid_account(&acc.address).await? {
			self.create(&acc).await?
		} else {
			let new_balance = convert_to_big_decimal_balance(acc.balance);
			let addr = hex::encode(acc.address);
			let updated_rows = match &self.pg.conn {
				PgConnectionType::TxConn(conn) => diesel::update(account.filter(address.eq(addr)))
					.set(balance.eq(new_balance))
					.execute(*conn.lock().await),
				PgConnectionType::PgConn(conn) => diesel::update(account.filter(address.eq(addr)))
					.set(balance.eq(new_balance))
					.execute(&mut *conn.lock().await),
			}?;

			if updated_rows != 1 {
				return Err(anyhow!(
					"Account update failed: expected to update balance, updated {}",
					updated_rows
				));
			}
		}
		Ok(())
	}

	async fn increment_nonce(&self, addr: &Address) -> Result<(), Error> {
		use db::postgres::schema::account::dsl::*;

		let result_nonce = self.get_nonce(addr).await? + 1;
		let new_nonce = convert_to_big_decimal_balance(result_nonce);
		let encoded_address = hex::encode(addr);

		match &self.pg.conn {
			PgConnectionType::TxConn(conn) => {
				diesel::update(account.filter(address.eq(encoded_address)))
					.set(nonce.eq(new_nonce))
					.execute(*conn.lock().await)?;
			},
			PgConnectionType::PgConn(conn) => {
				diesel::update(account.filter(address.eq(encoded_address)))
					.set(nonce.eq(new_nonce))
					.execute(&mut *conn.lock().await)?;
			},
		}
		Ok(())
	}
	async fn get_account(&self, addr: &Address) -> Result<Account, Error> {
		use db::postgres::schema::account::dsl::*;
		let encoded_address = hex::encode(addr);
		let res = match &self.pg.conn {
			PgConnectionType::TxConn(conn) => account
				.filter(account_dsl::address.eq(encoded_address))
				.load::<QueryAccount>(*conn.lock().await),
			PgConnectionType::PgConn(conn) => account
				.filter(account_dsl::address.eq(encoded_address))
				.load::<QueryAccount>(&mut *conn.lock().await),
		}?;

		match res.get(0) {
			Some(response) => {
				let acc_type = match response.account_type.clone() {
					Some(raw_acc_type) => system::account::AccountType::from(raw_acc_type.clone()),
					None => {
						anyhow::bail!("acc_type does not exist")
					},
				};
				let mut address_bytes = [0u8; 20]; // Declare it here to be used later

				// Decode it back to a byte array
				match hex::decode(&response.address) {
					Ok(decoded) => {
						// Now `decoded` is a Vec<u8> containing 20 bytes.
						address_bytes.copy_from_slice(&decoded);
						println!("Decoded bytes: {:?}", hex::encode(address_bytes));
					},
					Err(e) => {
						println!("Failed to decode hex string: {:?}", e);
					},
				}

				let acc = Account {
					address: address_bytes,
					balance: response.balance.as_ref().unwrap().to_u128().unwrap(),
					nonce: response.nonce.as_ref().unwrap().to_u128().unwrap(),
					account_type: acc_type,
				};

				Ok(acc)
			},
			None => {
				anyhow::bail!("account does not exist")
			},
		}
	}

	async fn get_nonce(&self, addr: &Address) -> Result<Nonce, Error> {
		use db::postgres::schema::account::dsl::*;
		if !self.is_valid_account(&addr).await? {
			// let account = Account::new(*address);
			// self.create(self, &account).await?;
			Ok(0)
		} else {
			let add = hex::encode(addr);
			let res = match &self.pg.conn {
				PgConnectionType::TxConn(conn) =>
					account.filter(address.eq(add)).load::<QueryAccount>(*conn.lock().await),
				PgConnectionType::PgConn(conn) =>
					account.filter(address.eq(add)).load::<QueryAccount>(&mut *conn.lock().await),
			}?;

			match res.get(0) {
				Some(acc) => match &acc.nonce {
					Some(big_decimal_nonce) => {
						let nonce_u128: u128 = big_decimal_nonce.to_u128().ok_or_else(|| {
							anyhow!("Failed to convert BigDecimal to u128 for nonce")
						})?;
						Ok(nonce_u128)
					},
					None => Err(anyhow!("Balance is None")),
				},
				None => Err(anyhow!("Account not found")),
			}
		}
	}

	async fn get_balance(&self, addr: &Address) -> Result<Balance, Error> {
		use db::postgres::schema::account::dsl::*;
		let add = hex::encode(addr);
		let res = match &self.pg.conn {
			PgConnectionType::TxConn(conn) =>
				account.filter(address.eq(add)).load::<QueryAccount>(*conn.lock().await),
			PgConnectionType::PgConn(conn) =>
				account.filter(address.eq(add)).load::<QueryAccount>(&mut *conn.lock().await),
		}?;
		match res.get(0) {
			Some(acc) => match &acc.balance {
				Some(big_decimal_balance) => {
					let balance_u128: u128 = big_decimal_balance.to_u128().ok_or_else(|| {
						anyhow!("Failed to convert BigDecimal to u128 for balance")
					})?;
					Ok(balance_u128)
				},
				None => Err(anyhow!("Balance is None")),
			},
			None => Err(anyhow!("Account not found")),
		}
	}

	async fn is_valid_account(&self, addr: &Address) -> anyhow::Result<bool> {
		use db::postgres::schema::account::dsl::*;
		let add = hex::encode(addr);
		let query_result = match &self.pg.conn {
			PgConnectionType::TxConn(conn) =>
				account.filter(address.eq(add)).load::<QueryAccount>(*conn.lock().await),
			PgConnectionType::PgConn(conn) =>
				account.filter(address.eq(add)).load::<QueryAccount>(&mut *conn.lock().await),
		};

		match query_result {
			Ok(res) => Ok(!res.is_empty()),
			Err(e) => Err(anyhow::anyhow!("Failed to execute query: {:?}", e)),
		}
	}
}
