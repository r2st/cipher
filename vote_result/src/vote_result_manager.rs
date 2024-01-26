use account::account_state::AccountState;
use anyhow::{anyhow, Error, Result};
use log::{info, warn};
use primitives::*;
use secp256k1::{hashes::sha256, Message, PublicKey, SecretKey};
use staking::*;
use staking_state::StakingState;
use system::{network::BroadcastNetwork, vote_result::VoteResult};
use validator::validator_state::ValidatorState;

use crate::vote_result_state::VoteResultState;
use db::db::DbTxConn;
use system::vote_result::VoteResultSignPayload;
use tokio::sync::mpsc;
use vote::vote_state::VoteState;
pub struct VoteResultManager {
	pub network_client_tx: mpsc::Sender<BroadcastNetwork>,
	pub multinode_mode: bool,
}

impl<'a> VoteResultManager {
	pub fn new(
		network_client_tx: mpsc::Sender<BroadcastNetwork>,
		multinode_mode: bool,
	) -> VoteResultManager {
		VoteResultManager { network_client_tx, multinode_mode }
	}
	pub async fn vote_result(
		&self,
		block_number: BlockNumber,
		block_hash: &BlockHash,
		pool_address: &Address,
		validator_address: &Address,
		cluster_address: &Address,
		secret_key: &SecretKey,
		verifying_key: &PublicKey,
		db_pool_conn: &'a DbTxConn<'a>,
	) -> Result<bool, Error> {
		let validator_state = ValidatorState::new(db_pool_conn).await?;
		let validators = match validator_state.load_all_validators(block_number).await? {
			Some(validators) => validators,
			None => return Ok(false), //Err(anyhow!("No validators found for the block_hash")),
		};

		let mut validator_print = String::new();
		for v in validators.clone() {
			let s = format!("\t{}\n", v);
			validator_print.push_str(&s);
		}
		// println!("SELECTED {} VALIDATORS for block {}: \n{}", selected_validators.len(),
		// block_number, validator_print);
		info!("VALIDATORS LOADED for block {}: \n{}", block_number, validator_print);

		let pool_account = {
			let account_state = AccountState::new(db_pool_conn).await?;
			let pool_account = account_state.get_account(&pool_address).await?;
			pool_account
		};
		let pool_balance = pool_account.balance;

		let mut voting_string = format!("Vote info for block #{}\n", block_number);
		voting_string.push_str(&format!("\tPool balance: {}\n", pool_balance));

		let all_votes_hashmap = {
			let vote_state = VoteState::new(db_pool_conn).await?;
			vote_state.load_all_votes_hashmap(&block_hash).await?
		};

		if let Some(votes) = all_votes_hashmap {
			// Calculate the minimum number of votes required (70% of validators)
			let min_votes = (validators.len() as f64 * 0.7) as usize;

			// Check if the number of votes is less than the required minimum
			// We assume 30% of validators will be unresponsive or not available
			if votes.len() < min_votes {
				return Ok(false) //Err(anyhow!("Number of votes is less than 70% of validators"));
			}
			let total_favoured_stake = futures::future::join_all(votes.iter().map(
				|(validator_address, &vote)| async move {
					if vote {
						let staking_state = StakingState::new(db_pool_conn)
							.await
							.expect("error getting staking state conn");
						let stake_result = staking_state
							.get_staking_account(validator_address, pool_address)
							.await;

						match stake_result {
							Ok(stake) => {
								println!("Stake result: {}", stake);
								stake.balance as f64
							},
							Err(err) => {
								warn!("Error fetching stake: {:?}", err);
								0.0
							},
						}
					} else {
						0.0
					}
				},
			))
			.await
			.into_iter()
			.sum::<f64>();

			// println!("TOTAL FAVOURED STAKE: {:?}", total_favoured_stake);
			voting_string.push_str(&format!("\tTotal favoured stake: {}\n", total_favoured_stake));

			// If 50% of the vote is in favour of the block, the block is accepted
			let vote_passed = (total_favoured_stake / pool_balance as f64) > 0.5;
			// info!("VOTE PASSED for block #{}? {:?}", block_number, vote_passed);
			voting_string.push_str(&format!("\t❔VOTE PASSED? {}\n", vote_passed));

			info!("{}", voting_string);

			let vote_result_sign_payload = VoteResultSignPayload::new(
				block_number,
				*block_hash,
				*cluster_address,
				vote_passed,
			);

			let json_str = serde_json::to_string(&vote_result_sign_payload).map_err(|e| {
				anyhow!(format!("Failed to serialize the vote signature payload: {:?}", e))
			})?;
			let message = Message::from_hashed_data::<sha256::Hash>(json_str.as_bytes());
			let sig = secret_key.sign_ecdsa(message);

			let vote_result = VoteResult::new(
				block_number,
				*block_hash,
				*cluster_address,
				*validator_address,
				sig.serialize_compact().to_vec(),
				verifying_key.serialize().to_vec(),
				vote_passed,
			);

			{
				let vote_result_state = VoteResultState::new(db_pool_conn).await?;
				vote_result_state.store_vote_result(&vote_result).await?;
			}
			if self.multinode_mode {
				if let Err(e) = self
					.network_client_tx
					.send(BroadcastNetwork::BroadcastVoteResult(vote_result))
					.await
				{
					warn!("Unable to write transaction to network_client_tx channel: {:?}", e)
				}
			}

			return Ok(vote_passed)
		} else {
			return Err(anyhow!("No votes found for the block_hash"))
		}
	}
}
