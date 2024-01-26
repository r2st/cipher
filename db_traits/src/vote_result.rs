use anyhow::Error;
use async_trait::async_trait;
use primitives::*;
use system::vote_result::VoteResult;

#[async_trait]
pub trait VoteResultState {
	async fn load_vote_result(&self, block_hash: &BlockHash) -> Result<VoteResult, Error>;
}
