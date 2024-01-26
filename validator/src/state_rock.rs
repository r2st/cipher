use anyhow::Error;
use async_trait::async_trait;
use primitives::{Address, BlockNumber};
use rocksdb::DB;
use std::fs::remove_dir_all;
use system::validator::Validator;

use db_traits::{base::BaseState, validator::ValidatorState};

pub struct StateRock {
	pub(crate) db_path: String,
	pub db: DB,
}

#[async_trait]
impl BaseState<Validator> for StateRock {
	async fn create_table(&self) -> Result<(), Error> {
		Ok(())
	}

	async fn create(&self, _validator: &Validator) -> Result<(), Error> {
		todo!()
	}

	async fn update(&self, _validator: &Validator) -> Result<(), Error> {
		todo!()
	}

	async fn raw_query(&self, _query: &str) -> Result<(), Error> {
		// Explicitly drop the DB to close any open connections or file handles
		drop(&self.db);

		// Remove the database directory
		remove_dir_all(&self.db_path);

		Ok(())
	}

	async fn set_schema_version(&self, _version: u32) -> Result<(), Error> {
		todo!()
	}
}

#[async_trait]
impl ValidatorState for StateRock {
	async fn load_validator(&self, _address: &Address) -> Result<Validator, Error> {
		todo!()
	}

	async fn load_all_validators(
		&self,
		_block_number: BlockNumber,
	) -> Result<Option<Vec<Validator>>, Error> {
		todo!()
	}

	async fn is_validator(
		&self,
		_address: &Address,
		_block_number: BlockNumber,
	) -> Result<bool, Error> {
		todo!()
	}
}
