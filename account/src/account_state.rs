use crate::{state_cas::StateCas, state_pg::StatePg, state_rock::StateRock};
use anyhow::Error;

use db::db::DbTxConn;
use db_traits::{account::AccountState as AccountStateInternal, base::BaseState};

use primitives::{Address, Balance, Nonce};
use rocksdb::DB;
use std::sync::Arc;
use system::account::Account;

pub enum StateInternalImpl<'a> {
	StateRock(StateRock),
	StatePg(StatePg<'a>),
	StateCas(StateCas),
}

pub struct AccountState<'a> {
	pub state: Arc<StateInternalImpl<'a>>,
}

impl<'a> AccountState<'a> {
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
				let db_path = format!("{}/account", db_path);
				state = StateInternalImpl::StateRock(StateRock {
					db_path: db_path.clone(),
					db: DB::open_default(db_path)?,
				});
			},
		}

		let state = AccountState { state: Arc::new(state) };

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

	pub async fn create_account(&self, account: &Account) -> Result<(), Error> {
		match &*self.state {
			StateInternalImpl::StateRock(s) => s.create(account).await,
			StateInternalImpl::StatePg(s) => s.create(account).await,
			StateInternalImpl::StateCas(s) => s.create(account).await,
		}
	}

	pub async fn raw_query(&self, query: &str) -> Result<(), Error> {
		match &*self.state {
			StateInternalImpl::StateRock(s) => s.raw_query(query).await,
			StateInternalImpl::StatePg(s) => s.raw_query(query).await,
			StateInternalImpl::StateCas(s) => s.raw_query(query).await,
		}
	}

	pub async fn update_account(&self, account: &Account) -> Result<(), Error> {
		match &*self.state {
			StateInternalImpl::StateRock(s) => s.update(account).await,
			StateInternalImpl::StatePg(s) => s.update(account).await,
			StateInternalImpl::StateCas(s) => s.update(account).await,
		}
	}

	pub async fn update_balance(&self, account: &Account) -> Result<(), Error> {
		match &*self.state {
			StateInternalImpl::StateRock(s) => s.update_balance(account).await,
			StateInternalImpl::StatePg(s) => s.update_balance(account).await,
			StateInternalImpl::StateCas(s) => s.update_balance(account).await,
		}
	}

	pub async fn increment_nonce(&self, address: &Address) -> Result<(), Error> {
		match &*self.state {
			StateInternalImpl::StateRock(s) => s.increment_nonce(address).await,
			StateInternalImpl::StatePg(s) => s.increment_nonce(address).await,
			StateInternalImpl::StateCas(s) => s.increment_nonce(address).await,
		}
	}

	pub async fn get_account(&self, address: &Address) -> Result<Account, Error> {
		match &*self.state {
			StateInternalImpl::StateRock(s) => s.get_account(address).await,
			StateInternalImpl::StatePg(s) => s.get_account(address).await,
			StateInternalImpl::StateCas(s) => s.get_account(address).await,
		}
	}

	pub async fn get_nonce(&self, address: &Address) -> Result<Nonce, Error> {
		match &*self.state {
			StateInternalImpl::StateRock(s) => s.get_nonce(address).await,
			StateInternalImpl::StatePg(s) => s.get_nonce(address).await,
			StateInternalImpl::StateCas(s) => s.get_nonce(address).await,
		}
	}

	pub async fn get_balance(&self, address: &Address) -> Result<Balance, Error> {
		match &*self.state {
			StateInternalImpl::StateRock(s) => s.get_balance(address).await,
			StateInternalImpl::StatePg(s) => s.get_balance(address).await,
			StateInternalImpl::StateCas(s) => s.get_balance(address).await,
		}
	}

	pub async fn is_valid_account(&self, address: &Address) -> Result<bool, Error> {
		match &*self.state {
			StateInternalImpl::StateRock(s) => s.is_valid_account(address).await,
			StateInternalImpl::StatePg(s) => s.is_valid_account(address).await,
			StateInternalImpl::StateCas(s) => s.is_valid_account(address).await,
		}
	}
}
