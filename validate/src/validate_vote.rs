use anyhow::Error;
use system::vote::Vote;

pub struct ValidateVote;

impl ValidateVote {
	pub async fn validate_vote(vote: &Vote) -> Result<(), Error> {
		vote.verify_signature().await
	}
}
