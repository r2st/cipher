use crate::validator_state::ValidatorState;
use anyhow::Error;

use db::db::DbTxConn;
use log::info;
use primitives::{Address, BlockNumber};
use staking::staking_state::StakingState;
use system::validator::Validator;

pub struct ValidatorManager;

impl<'a> ValidatorManager {
	/// Function to select validators based on stake
	pub async fn select_validators(
		&self,
		cluster_address: &Address,
		pool_address: &Address,
		block_number: BlockNumber,
		num_validators: usize,
		db_pool_conn: &'a DbTxConn<'a>,
	) -> Result<(), Error> {
		let validators = self
			.get_all_validators(cluster_address, pool_address, block_number, db_pool_conn)
			.await?;
		let validator_state = ValidatorState::new(db_pool_conn).await?;
		let mut selected_validators: Vec<&Validator> = Vec::new();

		// Sort validators in descending order of stake
		let mut sorted_validators: Vec<&Validator> = validators.iter().collect();
		sorted_validators.sort_by(|a, b| b.stake.cmp(&a.stake));

		// Select validators with the highest stakes
		for i in 0..num_validators {
			if let Some(validator) = sorted_validators.get(i) {
				selected_validators.push(validator);
			} else {
				break;
			}
		}
		let mut validator_print = String::new();
		for v in selected_validators.clone() {
			let s = format!("\t{}\n", v);
			validator_print.push_str(&s);
		}
		info!(
			"Selected {} validators for block {}: \n{}",
			selected_validators.len(),
			block_number,
			validator_print
		);

		for validator in selected_validators {
			validator_state.store_validator(&validator.clone()).await?;
		}
		Ok(())
	}

	/// Get all validators who are staked in a given pool
	pub async fn get_all_validators(
		&self,
		cluster_address: &Address,
		pool_address: &Address,
		block_number: BlockNumber,
		db_pool_conn: &'a DbTxConn<'a>,
	) -> Result<Vec<Validator>, Error> {
		let mut validators = Vec::new();
		// let node_info_state = NodeInfoState::new().await?;
		// let node_info_list = node_info_state.load_nodes(cluster_address).await?;

		let staking_state = StakingState::new(db_pool_conn).await?;
		let validator_staking_info = staking_state.get_all_pool_stakers(pool_address).await?;

		for account in validator_staking_info {
			// let stake = staking_state.get_staking_account(address, pool_address).await?.balance;
			validators.push(Validator {
				address: account.account_address,
				cluster_address: cluster_address.clone(),
				block_number,
				stake: account.balance,
			});
		}
		Ok(validators)
	}
}
