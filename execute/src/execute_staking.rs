use account::account_state::AccountState;
use anyhow::Error;
use db::db::DbTxConn;
use primitives::*;
use staking::{staking_manager::StakingManager, staking_state::StakingState};
use std::sync::Arc;
use system::account::Account;
pub struct ExecuteStaking {}

impl<'a> ExecuteStaking {
	pub async fn execute_native_staking_create_pool(
		&self,
		account_address: &Address,
		cluster_address: &Address,
		nonce: Nonce,
		created_block_number: BlockNumber,
		contract_instance_address: Option<Address>,
		min_stake: Option<Balance>,
		max_stake: Option<Balance>,
		min_pool_balance: Option<Balance>,
		max_pool_balance: Option<Balance>,
		staking_period: Option<BlockNumber>,
		db_tx_conn: Arc<&'a DbTxConn<'a>>,
		_db_pool_conn: Arc<&'a DbTxConn<'a>>,
	) -> Result<Address, Error> {
		let pool_address = Account::pool_address(account_address, cluster_address, nonce);
		let manager = StakingManager {};
		manager
			.create_pool(
				&pool_address,
				&account_address,
				contract_instance_address,
				cluster_address,
				created_block_number,
				min_stake,
				max_stake,
				min_pool_balance,
				max_pool_balance,
				staking_period,
				&db_tx_conn,
			)
			.await?;
		let account_state = AccountState::new(&db_tx_conn).await?;
		account_state.increment_nonce(&account_address).await?;
		Ok(pool_address)
	}

	pub async fn execute_native_staking_stake(
		&self,
		pool_address: &Address,
		account_address: &Address,
		block_number: BlockNumber,
		amount: Balance,
		db_tx_conn: Arc<&'a DbTxConn<'a>>,
		_db_pool_conn: Arc<&'a DbTxConn<'a>>,
	) -> Result<(), Error> {
		let manager = StakingManager {};
		manager
			.stake(account_address, pool_address, block_number, amount, &db_tx_conn)
			.await?;
		let account_state = AccountState::new(&db_tx_conn).await?;
		account_state.increment_nonce(&account_address).await?;
		Ok(())
	}

	pub async fn execute_native_staking_un_stake(
		&self,
		pool_address: &Address,
		account_address: &Address,
		block_number: BlockNumber,
		amount: Balance,
		db_tx_conn: Arc<&'a DbTxConn<'a>>,
		_db_pool_conn: Arc<&'a DbTxConn<'a>>,
	) -> Result<(), Error> {
		let manager = StakingManager {};
		manager
			.un_stake(account_address, pool_address, block_number, amount, &db_tx_conn)
			.await?;
		let account_state = AccountState::new(&db_tx_conn).await?;
		account_state.increment_nonce(&account_address).await?;
		Ok(())
	}

	pub async fn execute_native_staking_update_contract(
		&self,
		pool_address: &Address,
		contract_instance_address: &Address,
		account_address: &Address,
		block_number: BlockNumber,
		db_tx_conn: Arc<&'a DbTxConn<'a>>,
		_db_pool_conn: Arc<&'a DbTxConn<'a>>,
	) -> Result<(), Error> {
		{
			let staking_state = StakingState::new(&db_tx_conn).await?;
			staking_state
				.update_contract(contract_instance_address, pool_address, block_number)
				.await?;
		}
		let account_state = AccountState::new(&db_tx_conn).await?;
		account_state.increment_nonce(&account_address).await?;
		Ok(())
	}
}
