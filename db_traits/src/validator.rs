use anyhow::Error;
use async_trait::async_trait;
use primitives::*;
use system::validator::Validator;

#[async_trait]
pub trait ValidatorState {
	async fn load_validator(&self, address: &Address) -> Result<Validator, Error>;

	async fn load_all_validators(
		&self,
		block_number: BlockNumber,
	) -> Result<Option<Vec<Validator>>, Error>;

	async fn is_validator(
		&self,
		address: &Address,
		block_number: BlockNumber,
	) -> Result<bool, Error>;
}
