use anyhow::Error;
use async_trait::async_trait;
use db_traits::{account::AccountState, base::BaseState};
use primitives::{Address, Balance, Nonce};
use rocksdb::DB;
use system::account::Account;

pub struct StateRock {
	pub(crate) db_path: String,
	pub db: DB,
}

#[async_trait]
impl BaseState<Account> for StateRock {
	async fn create_table(&self) -> Result<(), Error> {
		Ok(())
	}

	async fn create(&self, account: &Account) -> Result<(), Error> {
		let key = format!("account:{:?}", account.address);
		let value = serde_json::to_string(account)?;
		self.db.put(&key, &value)?;
		Ok(())
	}

	async fn update(&self, account: &Account) -> Result<(), Error> {
		let key = format!("account:{:?}", account.address);
		let value = serde_json::to_string(account)?;
		self.db.put(key, value)?;
		Ok(())
	}

	async fn raw_query(&self, _query: &str) -> Result<(), Error> {
		Ok(())
	}

	async fn set_schema_version(&self, _version: u32) -> Result<(), Error> {
		// Implement setting schema version in RocksDB here
		Ok(())
	}
}

#[async_trait]
impl AccountState for StateRock {
	async fn update_balance(&self, account: &Account) -> Result<(), Error> {
		let key = format!("account:{:?}", account.address);
		let mut account_update: Account = self.get_account(&account.address).await?;
		account_update.balance = account.balance;
		let value = serde_json::to_string(&account_update)?;
		self.db.put(key, value)?;
		Ok(())
	}

	async fn increment_nonce(&self, address: &Address) -> Result<(), Error> {
		let key = format!("account:{:?}", address);
		let mut account: Account = self.get_account(address).await?;
		account.nonce = account.nonce + 1;
		let value = serde_json::to_string(&account)?;
		self.db.put(&key, value)?;
		Ok(())
	}

	async fn get_account(&self, address: &Address) -> Result<Account, Error> {
		let key = format!("account:{:?}", address);
		if !self.is_valid_account(&address).await? {
			let account = Account::new(*address);
			self.create(&account).await?;
		}

		match self.db.get(&key)? {
			Some(value) => {
				let value_str = String::from_utf8_lossy(&value);
				Ok(serde_json::from_str(&value_str)?)
			},
			None => Err(Error::msg("Account not found")),
		}
	}

	async fn get_nonce(&self, address: &Address) -> Result<Nonce, Error> {
		let account: Account = self.get_account(address).await?;
		Ok(account.nonce)
	}

	async fn get_balance(&self, address: &Address) -> Result<Balance, Error> {
		let account = self.get_account(address).await?;
		Ok(account.balance)
	}

	async fn is_valid_account(&self, address: &Address) -> Result<bool, Error> {
		let key = format!("account:{:?}", address);
		Ok(self.db.get(&key)?.is_some())
	}
}
