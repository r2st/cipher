use crate::{state_cas::StateCas, state_pg::StatePg, state_rock::StateRock};
use anyhow::Error;
use db::db::DbTxConn;
use db_traits::{base::BaseState, validator::ValidatorState as ValidatorStateInternal};
use primitives::*;
use rocksdb::DB;
use std::sync::Arc;
use system::validator::Validator;

pub enum StateInternalImpl<'a> {
	StateRock(StateRock),
	StatePg(StatePg<'a>),
	StateCas(StateCas),
}
pub struct ValidatorState<'a> {
	pub state: Arc<StateInternalImpl<'a>>,
}

impl<'a> ValidatorState<'a> {
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
				let db_path = format!("{}/validator", db_path);
				state = StateInternalImpl::StateRock(StateRock {
					db_path: db_path.clone(),
					db: DB::open_default(db_path)?,
				});
			},
		}

		let state = ValidatorState { state: Arc::new(state) };

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

	pub async fn store_validator(&self, validator: &Validator) -> Result<(), Error> {
		match &*self.state {
			StateInternalImpl::StateRock(s) => s.create(validator).await,
			StateInternalImpl::StatePg(s) => s.create(validator).await,
			StateInternalImpl::StateCas(s) => s.create(validator).await,
		}
	}

	pub async fn load_validator(&self, address: &Address) -> Result<Validator, Error> {
		match &*self.state {
			StateInternalImpl::StateRock(s) => s.load_validator(address).await,
			StateInternalImpl::StatePg(s) => s.load_validator(address).await,
			StateInternalImpl::StateCas(s) => s.load_validator(address).await,
		}
	}

	pub async fn load_all_validators(
		&self,
		block_number: BlockNumber,
	) -> Result<Option<Vec<Validator>>, Error> {
		match &*self.state {
			StateInternalImpl::StateRock(s) => s.load_all_validators(block_number).await,
			StateInternalImpl::StatePg(s) => s.load_all_validators(block_number).await,
			StateInternalImpl::StateCas(s) => s.load_all_validators(block_number).await,
		}
	}

	pub async fn is_validator(
		&self,
		address: &Address,
		block_number: BlockNumber,
	) -> Result<bool, Error> {
		match &*self.state {
			StateInternalImpl::StateRock(s) => s.is_validator(address, block_number).await,
			StateInternalImpl::StatePg(s) => s.is_validator(address, block_number).await,
			StateInternalImpl::StateCas(s) => s.is_validator(address, block_number).await,
		}
	}
}
