use anyhow::Error;
use system::vote_result::VoteResult;

pub struct ValidateVoteResult;

impl ValidateVoteResult {
	pub async fn validate_vote_result(vote_result: &VoteResult) -> Result<(), Error> {
		vote_result.verify_signature().await
	}
}
