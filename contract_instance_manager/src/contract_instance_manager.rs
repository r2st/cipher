use anyhow::Error;
use db::db::DbTxConn;
use evm_helper::{evm_call::EVMContractCallTrait, tagged_runtime::RuntimeInterrupt};
use evm_runtime::{Capture, Context, CreateScheme, ExitReason};
use primitives::*;
use std::{cell::RefCell, sync::Arc};
use system::{contract::Contract, contract_instance::ContractInstance, network::EventBroadcast};
use tokio::sync::broadcast;

pub struct ContractInstanceManager<'a> {
	pub contract_instance: Option<ContractInstance>,
	pub cluster_address: Address,
	pub account_address: Address,
	pub nonce: Nonce,
	pub transaction_hash: TransactionHash,
	pub block_number: BlockNumber,
	pub block_hash: BlockHash,
	pub block_timestamp: BlockTimeStamp,
	pub context: Context,
	pub evm: EVMSyncApi<'a>,
	pub rt: tokio::runtime::Runtime,
	pub db_tx_conn: Arc<&'a DbTxConn<'a>>,
	pub db_pool_conn: Arc<&'a DbTxConn<'a>>,
	pub event_tx: broadcast::Sender<EventBroadcast>,
}

impl<'a> ContractInstanceManager<'a> {
	pub fn new(
		contract_instance: Option<ContractInstance>,
		cluster_address: Address,
		account_address: Address,
		nonce: Nonce,
		transaction_hash: TransactionHash,
		block_number: BlockNumber,
		block_hash: BlockHash,
		block_timestamp: BlockTimeStamp,
		context: Context,
		evm: Arc<RefCell<dyn EVMContractCallTrait<'a>>>,
		db_tx_conn: Arc<&'a DbTxConn<'a>>,
		db_pool_conn: Arc<&'a DbTxConn<'a>>,
		event_tx: broadcast::Sender<EventBroadcast>,
	) -> Result<ContractInstanceManager<'a>, Error> {
		let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build()?;
		let manager = ContractInstanceManager {
			contract_instance,
			cluster_address,
			account_address,
			nonce,
			transaction_hash,
			block_number,
			block_hash,
			block_timestamp,
			context,
			evm: EVMSyncApi::new(evm),
			rt,
			db_tx_conn,
			db_pool_conn,
			event_tx,
		};
		Ok(manager)
	}
}

pub struct EVMSyncApi<'a> {
	pub evm: Arc<RefCell<dyn EVMContractCallTrait<'a>>>,
}

impl<'a> EVMSyncApi<'a> {
	pub fn new(evm: Arc<RefCell<dyn EVMContractCallTrait<'a>>>) -> Self {
		Self { evm }
	}

	pub fn execute_contract_deployment(
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
		let evm = self.evm.borrow();
		evm.execute_evm_contract_deployment(
			account_address,
			scheme,
			context,
			cluster_address,
			access_type,
			contract_code,
			nonce,
			transaction_hash,
			block_number,
			block_hash,
			block_timestamp,
			db_tx_conn,
			db_pool_conn,
			event_tx,
		)
	}

	pub fn execute_contract_function_call(
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
		let evm = self.evm.borrow();
		evm.execute_evm_contract_call(
			account_address,
			context,
			gas_limit,
			cluster_address,
			contract,
			contract_instance,
			function,
			arguments,
			nonce,
			transaction_hash,
			is_read_only,
			block_number,
			block_hash,
			block_timestamp,
			db_tx_conn,
			db_pool_conn,
			event_tx,
		)
	}

	pub fn execute_evm_function_call_readonly(
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
		let evm = self.evm.borrow();
		evm.execute_evm_function_call_readonly(
			contract,
			contract_instance,
			function,
			arguments,
			cluster_address,
			nonce,
			block_number,
			block_hash,
			block_timestamp,
			db_tx_conn,
			db_pool_conn,
			event_tx,
		)
	}
}
