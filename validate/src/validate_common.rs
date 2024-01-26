use crate::{
	validate_contract_evm, validate_staking::ValidateStaking, validate_token::ValidateToken,
};
use account::account_state::AccountState;
use anyhow::{anyhow, Error};
use async_trait::async_trait;
use db::{db::DbTxConn, utils::big_int::ToBigInt};
use log::{debug, info};
use primitives::*;
use system::{
	account::Account,
	contract::ContractType,
	transaction::{Transaction, TransactionType},
};

pub struct ValidateCommon {}

impl<'a> ValidateCommon {
	pub async fn validate_common(
		transaction: &Transaction,
		address: &Address,
		db_pool_conn: &'a DbTxConn<'a>,
	) -> Result<Account, Error> {
		let account_state = AccountState::new(db_pool_conn).await?;
		// Check if the transaction signature is valid
		if !transaction.verify_signature()? {
			info!("VALIDATE_COMMON => Invalid transaction signature {:?}", transaction);
			return Err(anyhow!("Invalid transaction signature"))
		} else {
			let message = format!(
				r#"
                +--------------------------------------------------------------------+
                |
                |  VALIDATE_COMMON => 🎉🎉🎉 Signature verified 🎉🎉🎉
                |  Received Signature => {:?}
                |
                +--------------------------------------------------------------------+
                "#,
				hex::encode(transaction.clone().signature)
			);

			info!("{}", message);
		}

		debug!("Address: {:?}", address);
		info!("Address: {:?}", hex::encode(address));
		if !account_state.is_valid_account(address).await? {
			info!("VALIDATE_COMMON => Invalid transaction sender {:?}", hex::encode(address));
			return Err(anyhow!("Invalid transaction sender"))
		}

		let account = account_state.get_account(address).await?;

		let expected_nonce = account.nonce + 1;
		if transaction.nonce != expected_nonce {
			info!(
				"\nVALIDATE_COMMON => Invalid nonce. Received {:?} != expected {:?}\n",
				transaction.nonce, expected_nonce
			);
			return Err(anyhow!("Invalid nonce"))
		}

		Ok(account)
	}

	pub async fn validate_common_native_token_tx(
		transaction: &Transaction,
		address: &Address,
		db_pool_conn: &'a DbTxConn<'a>,
	) -> Result<Account, Error> {
		let account_state = AccountState::new(db_pool_conn).await?;
		// Check if the transaction signature is valid
		if !transaction.verify_signature_native_token_tx()? {
			info!("VALIDATE_COMMON => Invalid transaction signature {:?}", transaction);
			return Err(anyhow!("Invalid transaction signature"))
		} else {
			let message = format!(
				r#"
                +--------------------------------------------------------------------+
                |
                |  VALIDATE_COMMON => 🎉🎉🎉 Signature verified 🎉🎉🎉
                |  Received Signature => {:?}
                |
                +--------------------------------------------------------------------+
                "#,
				hex::encode(transaction.clone().signature)
			);

			info!("{}", message);
		}

		debug!("Address: {:?}", address);
		info!("Address: {:?}", hex::encode(address));
		if !account_state.is_valid_account(address).await? {
			info!("VALIDATE_COMMON => Invalid transaction sender {:?}", hex::encode(address));
			return Err(anyhow!("Invalid transaction sender"))
		}

		let account = account_state.get_account(address).await?;

		if transaction.nonce <= account.nonce {
			info!(
				"\nVALIDATE_COMMON => Invalid nonce {:?} > {:?}\n",
				transaction.nonce, account.nonce
			);
			return Err(anyhow!("Invalid nonce"))
		}

		Ok(account)
	}

	pub async fn validate_tx(
		transaction: &Transaction,
		sender: &Address,
		db_pool_conn: &'a DbTxConn<'a>,
	) -> Result<(), Error> {
		let cloned_transaction = transaction.clone();
		info!(
			"VALIDATE_COMMON => Transaction signature: {:?}",
			hex::encode(cloned_transaction.clone().signature)
		);
		info!(
			"VALIDATE_COMMON =>Transaction pub key: {:?}",
			hex::encode(cloned_transaction.clone().verifying_key)
		);
		let _ = match cloned_transaction.transaction_type {
			TransactionType::NativeTokenTransfer(_recipient, amount) =>
				ValidateToken::validate_native_token(&transaction, amount, &sender, db_pool_conn)
					.await?,
			TransactionType::SmartContractDeployment {
				access_type: _,
				contract_type,
				contract_code,
				value: _,
				salt: _,
			} => {
				let validator: &(dyn ContractValidator + Sync) = match contract_type {
					ContractType::EVM =>
						&validate_contract_evm::ValidateContract as &(dyn ContractValidator + Sync),
				};
				validator
					.validate_contract_deployment(
						&transaction,
						&contract_code,
						&sender,
						db_pool_conn,
					)
					.await?
			},
			TransactionType::SmartContractInit(contract_address, arguments) => {
				// FIXME: how to derive contract_type?
				let contract_type = ContractType::EVM;
				let validator: &(dyn ContractValidator + Sync) = match contract_type {
					ContractType::EVM =>
						&validate_contract_evm::ValidateContract as &(dyn ContractValidator + Sync),
				};
				validator
					.validate_contract_init(
						&transaction,
						&contract_address,
						arguments,
						&sender,
						db_pool_conn,
					)
					.await?
			},
			TransactionType::SmartContractFunctionCall {
				contract_instance_address,
				function,
				arguments,
			} => {
				// FIXME: how to derive contract_type?
				let contract_type = ContractType::EVM;
				let validator: &(dyn ContractValidator + Sync) = match contract_type {
					ContractType::EVM =>
						&validate_contract_evm::ValidateContract as &(dyn ContractValidator + Sync),
				};
				validator
					.validate_contract_function_call(
						&transaction,
						&contract_instance_address,
						&function,
						&arguments,
						&sender,
						db_pool_conn,
					)
					.await?
			},
			TransactionType::CreateStakingPool {
				contract_instance_address: _,
				min_stake: _,
				max_stake: _,
				min_pool_balance: _,
				max_pool_balance: _,
				staking_period: _,
			} => {},
			TransactionType::Stake { pool_address, amount } =>
				ValidateStaking::validate_stake(sender, &pool_address, amount, db_pool_conn).await?,
			TransactionType::UnStake { pool_address, amount } =>
				ValidateStaking::validate_unstake(sender, &pool_address, amount, db_pool_conn)
					.await?,
			TransactionType::StakingPoolContract {
				pool_address: _,
				contract_instance_address: _,
			} => {},
		};
		Ok(())
	}

	pub fn has_sufficient_balance_to_cover_fee(
		transaction: &Transaction,
		account: &Account,
	) -> Result<(), Error> {
		// Check if the account has sufficient balance for the transaction fee

		let fee_limit = transaction.fee_limit.get_big_int();
		if account.balance.get_big_int().lt(&fee_limit) {
			return Err(anyhow!("Insufficient balance to cover the transaction fee"))
		}

		Ok(())
	}
}

#[async_trait]
pub(crate) trait ContractValidator<'a> {
	async fn validate_contract_deployment(
		&self,
		_transaction: &Transaction,
		_contract_code: &ContractCode,
		_sender: &Address,
		_db_conn: &'a DbTxConn<'a>,
	) -> Result<(), Error> {
		unimplemented!()
	}

	async fn validate_contract_init(
		&self,
		_transaction: &Transaction,
		_contract_address: &Address,
		_arguments: ContractArgument,
		_sender: &Address,
		_db_conn: &'a DbTxConn<'a>,
	) -> Result<(), Error> {
		unimplemented!()
	}

	async fn validate_contract_function_call(
		&self,
		_transaction: &Transaction,
		_contract_instance_address: &Address,
		_function_name: &ContractFunction,
		_arguments: &ContractArgument,
		_sender: &Address,
		_db_conn: &'a DbTxConn<'a>,
	) -> Result<(), Error> {
		unimplemented!()
	}
}
