use crate::{
	execute_common::ContractExecutor, execute_contract_evm,
	execute_contract_evm::ExecuteContract as EVMExecutor, execute_staking::ExecuteStaking,
	execute_token::ExecuteToken,
};
use account::account_state::AccountState;
use anyhow::{anyhow, Error};
use contract::contract_state::ContractState;
use std::{borrow::Cow, sync::Arc};

use contract_instance::contract_instance_state::ContractInstanceState;
use db::db::DbTxConn;
use event::event_state::EventState;
use evm_helper::tagged_runtime::create_evm_address;
use evm_runtime::{Capture, Context, CreateScheme, ExitError, ExitReason, ExitSucceed};
use log::info;
use primitive_types::{H160, H256, U256};
use primitives::*;
use rand::Rng;
use serde_json::json;
use sha3::{Digest, Keccak256};
use staking::staking_state::StakingState;
use system::{
	contract::ContractType,
	network::EventBroadcast,
	transaction::{Transaction, TransactionType},
};
use tokio::sync::broadcast;

fn buy_gas(balance: Balance) -> Gas {
	let gas_price = 1000;
	let gas = balance.checked_mul(gas_price).unwrap_or(Gas::MAX.into());

	Gas::try_from(gas).unwrap_or(Gas::MAX)
}

pub struct ExecuteTransaction {}

impl ExecuteTransaction {
	pub async fn execute_transaction<'a>(
		transaction: &Transaction,
		account_address: &Address,
		cluster_address: &Address,
		block_number: BlockNumber,
		block_hash: &BlockHash,
		block_timestamp: BlockTimeStamp,
		db_tx_conn: Arc<&'a DbTxConn<'a>>,
		db_pool_conn: Arc<&'a DbTxConn<'a>>,
		event_tx: broadcast::Sender<EventBroadcast>,
	) -> (anyhow::Result<()>, Option<EventData>) {
		let cloned_transaction = transaction.clone();
		let transaction_hash = match transaction.transaction_hash() {
			Ok(v) => v,
			Err(e) => return (Err(e), None),
		};
		let nonce = transaction.nonce;
		let (result, event) = match cloned_transaction.transaction_type {
			TransactionType::NativeTokenTransfer(recipient, amount) =>
				match ExecuteToken::execute_native_token_transfer(
					account_address,
					&recipient,
					amount,
					db_tx_conn,
					db_pool_conn,
				)
				.await
				{
					Ok(contract_instance_address) => {
						let val = match serde_json::to_vec(&json!({
							"event_type": "native_token_transferred",
							"from" : account_address,
							"to" : &recipient,
							"amount" : amount,
						})) {
							Ok(v) => v,
							Err(e) => return (Err(anyhow!("{:?}", e)), None),
						};
						(Ok(()), Some(val))
					},
					Err(e) => {
						let msg = format!("Error executing NativeTokenTransfer - {}", e);
						(Err(anyhow!(msg)), None)
					},
				},
			TransactionType::SmartContractDeployment {
				access_type,
				contract_type,
				contract_code,
				value,
				salt,
			} => {
				let salt = H256::from_slice(&salt);
				let executor: Box<dyn ContractExecutor + Send + Sync> = match contract_type {
					ContractType::EVM => Box::new(EVMExecutor),
				};
				let caller = account_address;
				let code_hash = H256::from_slice(Keccak256::digest(&contract_code).as_slice());
				let scheme = if contract_type == ContractType::EVM {
					let schm = CreateScheme::Legacy { caller: caller.into() };
					schm
				} else {
					let schm = CreateScheme::Create2 { caller: caller.into(), code_hash, salt };
					schm
				};
				let contract_instance_address = create_evm_address(scheme, nonce);
				let context = Context {
					/// Execution address.
					address: contract_instance_address, // Does not affect pool id
					/// Caller of the EVM.
					caller: H160(*account_address), // Does not affect pool id
					/// Apparent value of the EVM.
					apparent_value: value.into(),
				};
				match executor
					.execute_contract_deployment(
						account_address,
						scheme,
						context,
						cluster_address,
						access_type as i8,
						&contract_code,
						transaction.nonce,
						&transaction_hash,
						block_number,
						block_hash,
						block_timestamp,
						db_tx_conn,
						db_pool_conn,
						event_tx,
					)
					.await
				{
					Ok(contract_address) => {
						let val = match serde_json::to_vec(&json!({
							"event_type": "smart_contract_deployed",
							"contract_type": match contract_type {
								ContractType::EVM => "evm",
								},
							"contract_address" : contract_address,
						})) {
							Ok(v) => v,
							Err(e) => return (Err(anyhow!("{:?}", e)), None),
						};
						(Ok(()), Some(val))
					},
					Err(e) =>
						(Err(anyhow!("Error executing SmartContractDeployment - {}", e)), None),
				}
			},
			TransactionType::SmartContractInit(contract_address, arguments) => {
				let contract = {
					let contract_state = match ContractState::new(&db_tx_conn).await {
						Ok(v) => v,
						Err(e) => return (Err(e), None),
					};
					match contract_state.get_contract(&contract_address).await {
						Ok(v) => v,
						Err(e) => return (Err(e), None),
					}
				};
				let contract_type = match contract.r#type.try_into() {
					Ok(v) => v,
					Err(e) => return (Err(anyhow!("{:?}", e)), None),
				};
				let executor: Box<dyn ContractExecutor + Send + Sync> = match contract_type {
					ContractType::EVM => Box::new(EVMExecutor),
				};
				let context = Context {
					/// Execution address.
					address: H160(*account_address),
					/// Caller of the EVM.
					caller: H160(*account_address), /* This is where the vault address is taken
					                                 * for pool id */
					/// Apparent value of the EVM.
					apparent_value: U256::zero(),
				};

				let gas_limit = buy_gas(transaction.fee_limit);
				match executor
					.execute_contract_init(
						account_address,
						context,
						gas_limit,
						cluster_address,
						&contract,
						arguments,
						transaction.nonce,
						&transaction_hash,
						block_number,
						block_hash,
						block_timestamp,
						db_tx_conn,
						db_pool_conn,
						event_tx.clone(),
					)
					.await
				{
					// TODO: Charge Fee for the `burnt_gas`
					Ok((contract_instance, _burnt_gas)) => {
						let val = match serde_json::to_vec(&json!({
							"event_type": "smart_contract_initialized",
							"contract_instance_address" : contract_instance,
						})) {
							Ok(v) => v,
							Err(e) => return (Err(anyhow!("{:?}", e)), None),
						};
						(Ok(()), Some(val))
					},
					Err(e) => (Err(anyhow!("Error executing SmartContractInit - {}", e)), None),
				}
			},
			TransactionType::SmartContractFunctionCall {
				contract_instance_address,
				function,
				arguments,
			} => {
				let contract_instance = {
					let contract_instance_state =
						match ContractInstanceState::new(&db_tx_conn).await {
							Ok(v) => v,
							Err(e) => return (Err(e), None),
						};
					match contract_instance_state
						.get_contract_instance(&contract_instance_address)
						.await
					{
						Ok(v) => v,
						Err(e) => return (Err(e), None),
					}
				};
				let contract = {
					let contract_state = match ContractState::new(&db_tx_conn).await {
						Ok(v) => v,
						Err(e) => return (Err(e), None),
					};
					match contract_state.get_contract(&contract_instance.contract_address).await {
						Ok(v) => v,
						Err(e) => return (Err(e), None),
					}
				};

				let executor: Box<dyn ContractExecutor + Send + Sync> =
					match match contract.r#type.try_into() {
						Ok(v) => v,
						Err(e) => return (Err(e), None),
					} {
						ContractType::EVM => Box::new(EVMExecutor),
					};
				let context = Context {
					/// Execution address.
					address: H160(contract_instance_address),
					/// Caller of the EVM.
					caller: H160(*account_address), /* This is where the vault address is taken
					                                 * for pool id */
					/// Apparent value of the EVM.
					apparent_value: U256::zero(),
				};
				let gas_limit = buy_gas(transaction.fee_limit);
				let (result, gas) = executor
					.execute_contract_function_call(
						account_address,
						context,
						gas_limit,
						&cluster_address,
						&contract,
						&contract_instance,
						&function,
						&arguments,
						nonce,
						&transaction_hash,
						false,
						block_number,
						block_hash,
						block_timestamp,
						db_tx_conn,
						db_pool_conn,
						event_tx,
					)
					.await;
				let (reason, result) = match result {
					Capture::Exit(reason) => {
						let (reason, result) = reason;
						(reason, result)
					},
					Capture::Trap(resolve) => (
						ExitReason::Error(ExitError::Other(Cow::Owned(
							"execute_transaction: Capture::Trap(resolve)".to_string(),
						))),
						Arc::new(vec![]),
					),
				};
				/*Ok(Some(serde_json::to_vec(&json!({
					"event_type": "smart_contract_executed",
					"event" : (*result).clone(),
				}))?))*/

				//Ok(Some((*result).clone()))
				Self::analyze_reason(&reason, (*result).clone())
			},
			TransactionType::CreateStakingPool {
				contract_instance_address,
				min_stake,
				max_stake,
				min_pool_balance,
				max_pool_balance,
				staking_period,
			} => {
				let execute_staking = ExecuteStaking {};
				match execute_staking
					.execute_native_staking_create_pool(
						account_address,
						cluster_address,
						transaction.nonce,
						block_number,
						contract_instance_address,
						min_stake,
						max_stake,
						min_pool_balance,
						max_pool_balance,
						staking_period,
						db_tx_conn,
						db_pool_conn,
					)
					.await
				{
					Ok(_) => (Ok(()), None),
					Err(e) => (Err(anyhow!("Error executing CreateStakingPool - {}", e)), None),
				}
			},
			TransactionType::Stake { pool_address, amount } => {
				let execute_staking = ExecuteStaking {};
				match execute_staking
					.execute_native_staking_stake(
						&pool_address,
						account_address,
						block_number,
						amount,
						db_tx_conn,
						db_pool_conn,
					)
					.await
				{
					Ok(_) => (Ok(()), None),
					Err(e) => (Err(anyhow!("Error executing Stake - {}", e)), None),
				}
			},
			TransactionType::UnStake { pool_address, amount } => {
				let execute_staking = ExecuteStaking {};
				match execute_staking
					.execute_native_staking_un_stake(
						&pool_address,
						account_address,
						block_number,
						amount,
						db_tx_conn,
						db_pool_conn,
					)
					.await
				{
					Ok(_) => (Ok(()), None),
					Err(e) => (Err(anyhow!("Error executing UnStake - {}", e)), None),
				}
			},
			TransactionType::StakingPoolContract { pool_address, contract_instance_address } => {
				let execute_staking = ExecuteStaking {};
				match execute_staking
					.execute_native_staking_update_contract(
						&pool_address,
						&contract_instance_address,
						account_address,
						block_number,
						db_tx_conn,
						db_pool_conn,
					)
					.await
				{
					Ok(_) => (Ok(()), None),
					Err(e) => (Err(anyhow!("Error executing StakingPoolContract - {}", e)), None),
				}
			},
		};
		(result, event)
	}

	fn analyze_reason(
		reason: &ExitReason,
		return_data: Vec<u8>,
	) -> (anyhow::Result<()>, Option<Vec<u8>>) {
		match reason {
			ExitReason::Succeed(s) => {
				info!("analyse_reason: ExitReason::Succeed: {:?}", s);
				(Ok(()), Some(return_data))
			},
			ExitReason::Error(e) => {
				info!("analyse_reason: ExitReason::Error: {:?}", e);
				(Err(anyhow::anyhow!("{:?}", e)), Some(return_data))
			},
			ExitReason::Revert(e) => {
				info!("analyse_reason: ExitReason::Revert: {:?}", e);
				(Err(anyhow::anyhow!("{:?}", e)), Some(return_data))
			},
			ExitReason::Fatal(e) => {
				info!("analyse_reason: ExitReason::Fatal: {:?}", e);
				(Err(anyhow::anyhow!("{:?}", e)), Some(return_data))
			},
		}
	}
}
