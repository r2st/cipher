use anyhow::{anyhow, Error};
use async_trait::async_trait;
use db::db::DbTxConn;
use evm_helper::tagged_runtime::RuntimeInterrupt;
use evm_runtime::{Capture, Context, CreateScheme, ExitReason, ExitSucceed};
use primitives::*;
use std::sync::Arc;
use system::{contract::Contract, contract_instance::ContractInstance, network::EventBroadcast};
use tokio::sync::broadcast;

#[async_trait]
pub trait ContractExecutor<'a> {
	async fn execute_contract_deployment(
		&self,
		account_address: &Address,
		scheme: CreateScheme,
		context: Context,
		cluster_address: &Address,
		access_type: AccessType,
		contract_code: &ContractCode,
		nonce: Nonce,
		transaction_hash: &TransactionHash,
		block_number: BlockNumber,
		block_hash: &BlockHash,
		block_timestamp: BlockTimeStamp,
		db_tx_conn: Arc<&'a DbTxConn<'a>>,
		db_pool_conn: Arc<&'a DbTxConn<'a>>,
		event_tx: broadcast::Sender<EventBroadcast>,
	) -> Result<Address, Error> {
		Err(anyhow!("execute_contract_deployment not implemented"))
	}

	async fn execute_contract_init(
		&self,
		account_address: &Address,
		context: Context,
		gas_limit: Gas,
		cluster_address: &Address,
		contract: &Contract,
		arguments: ContractArgument,
		nonce: Nonce,
		transaction_hash: &TransactionHash,
		block_number: BlockNumber,
		block_hash: &BlockHash,
		block_timestamp: BlockTimeStamp,
		db_tx_conn: Arc<&'a DbTxConn<'a>>,
		db_pool_conn: Arc<&'a DbTxConn<'a>>,
		event_tx: broadcast::Sender<EventBroadcast>,
	) -> Result<(ContractInstance, Gas), Error> {
		Err(anyhow!("execute_contract_init not implemented"))
	}

	async fn execute_contract_function_call(
		&self,
		account_address: &Address,
		context: Context,
		gas_limit: Gas,
		cluster_address: &Address,
		contract: &Contract,
		contract_instance: &ContractInstance,
		function: &ContractFunction,
		arguments: &ContractArgument,
		nonce: Nonce,
		transaction_hash: &TransactionHash,
		is_read_only: bool,
		block_number: BlockNumber,
		block_hash: &BlockHash,
		block_timestamp: BlockTimeStamp,
		db_tx_conn: Arc<&'a DbTxConn<'a>>,
		db_pool_conn: Arc<&'a DbTxConn<'a>>,
		event_tx: broadcast::Sender<EventBroadcast>,
	) -> (Capture<(ExitReason, Arc<Vec<u8>>), RuntimeInterrupt>, Gas) {
		(Capture::Exit((ExitReason::Succeed(ExitSucceed::Returned), Arc::new(vec![]))), 0)
	}

	async fn execute_contract_function_call_read_only(
		&self,
		contract: &Contract,
		contract_instance: &ContractInstance,
		function: &ContractFunction,
		arguments: &ContractArgument,
		cluster_address: &Address,
		nonce: Nonce,
		block_number: BlockNumber,
		block_hash: &BlockHash,
		block_timestamp: BlockTimeStamp,
		db_tx_conn: Arc<&'a DbTxConn<'a>>,
		db_pool_conn: Arc<&'a DbTxConn<'a>>,
		event_tx: broadcast::Sender<EventBroadcast>,
	) -> (Capture<(ExitReason, Arc<Vec<u8>>), RuntimeInterrupt>, Gas) {
		(Capture::Exit((ExitReason::Succeed(ExitSucceed::Returned), Arc::new(vec![]))), 0)
	}
}
