use anyhow::{anyhow, Error};
use bincode;
use db::{
	cassandra::DatabaseManager,
	utils::{FromByteArray, ToByteArray},
};
use lazy_static::lazy_static;
use primitives::{arithmetic::ScalarBig, Address, Balance, Nonce};
use scylla::{Session, _macro_internal::CqlValue};
use std::sync::Arc;
use system::account::{Account, AccountType};
use tokio::sync::Mutex;

lazy_static! {
	static ref TABLE_CREATION_LOCK: Mutex<()> = Mutex::new(());
}
pub struct AccountState {
	pub session: Arc<Session>,
}

impl AccountState {
	pub async fn new() -> Result<Self, Error> {
		let db_session = DatabaseManager::get_session().await?;
		let account_state = AccountState { session: db_session.clone() };
		account_state.create_table().await?;
		Ok(account_state)
	}

	pub async fn get_state(session: Session) -> Result<Self, Error> {
		let db_session = DatabaseManager::get_session().await?;
		let account_state = AccountState { session: db_session.clone() };
		account_state.create_table().await?;
		Ok(account_state)
	}

	pub async fn create_table(&self) -> Result<(), Error> {
		let _guard = TABLE_CREATION_LOCK.lock().await;
		match self
			.session
			.query(
				"CREATE TABLE IF NOT EXISTS cipher.account (
                    address blob,
                    balance blob,
                    nonce blob,
                    account_type blob,
                    PRIMARY KEY (address)
                );",
				&[],
			)
			.await
		{
			Ok(_) => {},
			Err(_) => return Err(anyhow!("Create table failed")),
		};
		Ok(())
	}

	pub async fn create_account(&self, account: &Account) -> Result<(), Error> {
		let balance_u128: ScalarBig = account.balance.to_byte_array_le(ScalarBig::default());
		let nonce_u128: ScalarBig = account.nonce.to_byte_array_le(ScalarBig::default());
		let account_type: Vec<u8> = bincode::serialize(&account.account_type)
			.map_err(|_| anyhow!("Unserializable account type - {:?}", account.account_type))?;
		match self
            .session
            .query(
                "INSERT INTO cipher.account (address, balance, nonce, account_type) VALUES (?, ?, ?, ?)",
                (&account.address, &balance_u128, &nonce_u128, &account_type),
            )
            .await
        {
            Ok(_) => {}
            Err(e) => return Err(anyhow!("Account: Create account failed - {}", e)),
        };
		Ok(())
	}

	pub async fn update_account(&self, account: &Account) -> Result<(), Error> {
		if !self.is_valid_account(&account.address).await? {
			self.create_account(&account).await
		} else {
			let balance_u128: ScalarBig = account.balance.to_byte_array_le(ScalarBig::default());
			let nonce_u128: ScalarBig = account.nonce.to_byte_array_le(ScalarBig::default());
			match self
				.session
				.query(
					"UPDATE cipher.account SET balance = ?, nonce = ? WHERE address = ?;",
					(&balance_u128, &nonce_u128, &account.address),
				)
				.await
			{
				Ok(_) => Ok(()),
				Err(e) => return Err(anyhow!("Account: Update account balance failed - {}", e)),
			}
		}
	}

	pub async fn update_balance(&self, account: &Account) -> Result<(), Error> {
		if !self.is_valid_account(&account.address).await? {
			self.create_account(&account).await
		} else {
			let balance_u128: ScalarBig = account.balance.to_byte_array_le(ScalarBig::default());
			match self
				.session
				.query(
					"UPDATE cipher.account SET balance = ? WHERE address = ?;",
					(&balance_u128, &account.address),
				)
				.await
			{
				Ok(_) => Ok(()),
				Err(e) => return Err(anyhow!("Account: Update account balance failed - {}", e)),
			}
		}
	}

	pub async fn increment_nonce(&self, address: &Address) -> Result<(), Error> {
		let nonce = self.get_nonce(address).await? + 1;
		let nonce_u128: ScalarBig = nonce.to_byte_array_le(ScalarBig::default());
		match self
			.session
			.query("UPDATE cipher.account SET nonce = ? WHERE address = ?;", (&nonce_u128, &address))
			.await
		{
			Ok(_) => {},
			Err(_) => return Err(anyhow!("Invalid address")),
		};
		Ok(())
	}

	pub async fn get_account(&self, address: &Address) -> Result<Account, Error> {
		if !self.is_valid_account(&address).await? {
			let account = Account::new(*address);
			self.create_account(&account).await?;
		}
		let query_result = match self
			.session
			.query(
				"SELECT balance, nonce, account_type FROM cipher.account WHERE address = ?;",
				(&address,),
			)
			.await
		{
			Ok(q) => q,
			Err(_) => return Err(anyhow!("Invalid address")),
		};

		let (balance_idx, _) = query_result
			.get_column_spec("balance")
			.ok_or_else(|| anyhow!("No balance column found"))?;
		let (nonce_idx, _) = query_result
			.get_column_spec("nonce")
			.ok_or_else(|| anyhow!("No nonce column found"))?;
		let (account_type_idx, _) = query_result
			.get_column_spec("account_type")
			.ok_or_else(|| anyhow!("No account_type column found"))?;

		let rows = query_result.rows.ok_or_else(|| anyhow!("No rows found"))?;

		let balance: Balance = if let Some(row) = rows.get(0) {
			if let Some(balance_value) = &row.columns[balance_idx] {
				if let CqlValue::Blob(balance) = balance_value {
					u128::from_byte_array(balance)
				} else {
					return Err(anyhow!("Unable to convert to Balance type"))
				}
			} else {
				return Err(anyhow!("Unable to read balance column"))
			}
		} else {
			return Err(anyhow!("account_state: balance 178 - Unable to read row"))
		};

		let nonce: Nonce = if let Some(row) = rows.get(0) {
			if let Some(nonce_value) = &row.columns[nonce_idx] {
				if let CqlValue::Blob(nonce) = nonce_value {
					u128::from_byte_array(nonce)
				} else {
					return Err(anyhow!("Unable to convert to Nonce type"))
				}
			} else {
				return Err(anyhow!("Unable to read nonce column"))
			}
		} else {
			return Err(anyhow!("account_state: nonce 192 - Unable to read row"))
		};

		let account_type: AccountType = if let Some(row) = rows.get(0) {
			if let Some(account_type_value) = &row.columns[account_type_idx] {
				if let CqlValue::Blob(account_type) = account_type_value {
					match bincode::deserialize(&account_type) {
						Ok(account_type) => account_type,
						Err(e) =>
							return Err(anyhow!("Unable to convert to AccountType type - {}", e)),
					}
				} else {
					return Err(anyhow!("Unable to get bytes for AccountType"))
				}
			} else {
				return Err(anyhow!("Unable to read account_type column"))
			}
		} else {
			return Err(anyhow!("account_state: account_type 211 - Unable to read row"))
		};

		Ok(Account { address: *address, balance, nonce, account_type })
	}

	pub async fn get_nonce(&self, address: &Address) -> Result<Nonce, Error> {
		if !self.is_valid_account(&address).await? {
			let account = Account::new(*address);
			self.create_account(&account).await?;
		}
		let query_result = match self
			.session
			.query("SELECT nonce FROM cipher.account WHERE address = ?;", (&address,))
			.await
		{
			Ok(q) => q,
			Err(_) => return Err(anyhow!("Invalid address")),
		};

		let (nonce_idx, _) = query_result
			.get_column_spec("nonce")
			.ok_or_else(|| anyhow!("No nonce column found"))?;

		let rows = query_result.rows.ok_or_else(|| anyhow!("No rows found"))?;
		let nonce: Nonce = if let Some(row) = rows.get(0) {
			if let Some(nonce_value) = &row.columns[nonce_idx] {
				if let CqlValue::Blob(nonce) = nonce_value {
					u128::from_byte_array(nonce)
				} else {
					return Err(anyhow!("Unable to convert to Nonce type"))
				}
			} else {
				return Err(anyhow!("Unable to read nonce column"))
			}
		} else {
			return Err(anyhow!("account_state: get_nonce 255: Unable to read row"))
		};
		Ok(nonce)
	}

	pub async fn get_balance(&self, address: &Address) -> Result<Balance, Error> {
		if !self.is_valid_account(&address).await? {
			let account = Account::new(*address);
			self.create_account(&account).await?;
		}
		let query_result = match self
			.session
			.query("SELECT balance FROM cipher.account WHERE address = ?;", (&address,))
			.await
		{
			Ok(q) => q,
			Err(_) => return Err(anyhow!("Invalid address")),
		};

		let (balance_idx, _) = query_result
			.get_column_spec("balance")
			.ok_or_else(|| anyhow!("No balance column found"))?;

		let rows = query_result.rows.ok_or_else(|| anyhow!("No rows found"))?;
		let balance: Balance = if let Some(row) = rows.get(0) {
			if let Some(balance_value) = &row.columns[balance_idx] {
				if let CqlValue::Blob(balance) = balance_value {
					u128::from_byte_array(balance)
				} else {
					return Err(anyhow!("Unable to convert to Balance type"))
				}
			} else {
				return Err(anyhow!("Unable to read balance column"))
			}
		} else {
			return Err(anyhow!("account_state: balance 293 - Unable to read row"))
		};
		Ok(balance)
	}

	pub async fn is_valid_account(&self, address: &Address) -> Result<bool, Error> {
		let query_result = match self
			.session
			.query("SELECT COUNT(*) AS count FROM cipher.account WHERE address = ?;", (&address,))
			.await
		{
			Ok(q) => q,
			Err(e) => {
				let message = format!("Invalid address - {}", e);
				return Err(anyhow!(message))
			},
		};

		let (count_idx, _) = query_result
			.get_column_spec("count")
			.ok_or_else(|| anyhow!("No count column found"))?;

		let rows = query_result.rows.ok_or_else(|| anyhow!("No rows found"))?;

		if let Some(row) = rows.get(0) {
			if let Some(count_value) = &row.columns[count_idx] {
				if let CqlValue::BigInt(count) = count_value {
					return Ok(count > &0)
				} else {
					return Err(anyhow!("Unable to convert to Nonce type"))
				}
			}
		}
		Ok(false)
	}
}