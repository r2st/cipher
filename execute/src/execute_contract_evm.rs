use crate::{execute_common::ContractExecutor, execute_transaction::ExecuteTransaction};
use account::{account_manager::AccountManager, account_state::AccountState};
use anyhow::{anyhow, Error};
use async_scoped::TokioScope;
use async_trait::async_trait;
use contract::contract_state::ContractState;
use contract_instance::contract_instance_state::ContractInstanceState;
use contract_instance_manager::contract_instance_manager::ContractInstanceManager;
use db::db::DbTxConn;
use event::event_state::EventState;
use evm::{executor::stack::PrecompileSet, maybe_borrowed::MaybeBorrowed};
use evm_helper::{
	evm_call::EVMContractCallTrait,
	tagged_runtime::{create_evm_address, RuntimeInterrupt, RuntimeKind, TaggedRuntime},
};
use evm_precompile::{
	standard::{Etable, EtableResolver, Invoker, TransactArgs},
	StandardPrecompileSet,
};
use evm_runtime::{
	Capture, Config, Context, CreateScheme, ExitError, ExitReason, ExitSucceed, Opcode, Resolve,
	Runtime,
};
use log::{error, info};
use primitive_types::{H160, H256, U256};
use primitives::*;
use rand::Rng;
use sha3::{Digest, Keccak256};
use std::{borrow::Cow, cell::RefCell, rc::Rc, sync::Arc, thread::Scope};
use system::{
	account::Account,
	contract::{Contract, ContractType},
	contract_instance::ContractInstance,
	event::{Event, EventType},
	network::EventBroadcast,
	vm_result::VmResult,
};
use tokio::{
	sync::{broadcast, oneshot},
	task,
};

pub struct ExecuteContract;

impl<'a> ExecuteContract {
	fn cleanup_for_create(
		config: Config,
		created_address: H160,
		reason: ExitReason,
		return_data: Vec<u8>,
	) -> (ExitReason, Option<H160>, Vec<u8>) {
		fn check_first_byte(config: &Config, code: &[u8]) -> Result<(), ExitError> {
			if config.disallow_executable_format && Some(&Opcode::EOFMAGIC.as_u8()) == code.first()
			{
				return Err(ExitError::InvalidCode(Opcode::EOFMAGIC))
			}
			Ok(())
		}

		log::debug!(target: "evm", "Create execution using address {}: {:?}", created_address, reason);

		match reason {
			ExitReason::Succeed(s) => {
				let out = return_data.clone();
				let address = created_address;
				// As of EIP-3541 code starting with 0xef cannot be deployed
				if let Err(e) = check_first_byte(&config, &out) {
					return (e.into(), None, vec![])
				}

				if let Some(limit) = config.create_contract_limit {
					if out.len() > limit {
						return (ExitError::CreateContractLimit.into(), None, vec![])
					}
				}
				(ExitReason::Succeed(s), Some(address), return_data)
			},
			ExitReason::Error(e) => (ExitReason::Error(e), None, return_data),
			ExitReason::Revert(e) => (ExitReason::Revert(e), None, return_data),
			ExitReason::Fatal(e) => (ExitReason::Fatal(e), None, return_data),
		}
	}

	fn cleanup_for_call(
		config: Config,
		code_address: H160,
		reason: &ExitReason,
		return_data: Vec<u8>,
	) -> (anyhow::Result<()>, Vec<u8>) {
		log::debug!(target: "evm", "Call execution using address {}: {:?}", code_address, reason);
		match reason {
			ExitReason::Succeed(s) => {
				info!("cleanup_for_call: ExitReason::Succeed: {:?}", s);
				(Ok(()), return_data)
			},
			ExitReason::Error(e) => {
				info!("cleanup_for_call: ExitReason::Error: {:?}", e);
				(Err(anyhow::anyhow!("{:?}", e)), return_data)
			},
			ExitReason::Revert(e) => {
				info!("cleanup_for_call: ExitReason::Revert: {:?}", e);
				(Err(anyhow::anyhow!("{:?}", e)), return_data)
			},
			ExitReason::Fatal(e) => {
				info!("cleanup_for_call: ExitReason::Fatal: {:?}", e);
				(Err(anyhow::anyhow!("{:?}", e)), return_data)
			},
		}
	}

	pub async fn execute_evm_runtime(
		&self,
		account_address: &Address,
		cluster_address: &Address,
		access_type: AccessType,
		contract_code: &ContractCode,
		arguments: &ContractArgument,
		nonce: Nonce,
		transaction_hash: &TransactionHash,
		is_read_only: bool,
		block_number: BlockNumber,
		block_hash: &BlockHash,
		block_timestamp: BlockTimeStamp,
		runtime_kind: RuntimeKind,
		context: Context,
		db_tx_conn: Arc<&'a DbTxConn<'a>>,
		db_pool_conn: Arc<&'a DbTxConn<'a>>,
		event_tx: broadcast::Sender<EventBroadcast>,
	) -> (Capture<(ExitReason, Arc<Vec<u8>>), RuntimeInterrupt>, Gas) {
		info!("**** BEGINGS execute_evm_runtime ****");
		let arguments = arguments.clone();
		let account_address = account_address.clone();
		let cluster_address = cluster_address.clone();
		let access_type = access_type.clone();
		let contract_code = contract_code.clone();
		let transaction_hash = transaction_hash.clone();
		let block_hash = block_hash.clone();
		let config = Config::shanghai();
		let (tx, rx) = tokio::sync::oneshot::channel();
		let db_tx_conn1 = db_tx_conn.clone();
		let db_pool_conn1 = db_pool_conn.clone();
		unsafe {
			TokioScope::scope_and_collect(|scope| {
				scope.spawn_blocking(move || {
					let evm = Arc::new(RefCell::new(ExecuteContract {}));
					let ciphervm = Arc::new(RefCell::new(ExecuteContract {}));
					let ciphervm_stake =
						Arc::new(RefCell::new(crate::execute_staking::ExecuteStaking {}));
					let mut contract_instance_manager = match ContractInstanceManager::new(
						None,
						cluster_address,
						account_address,
						nonce,
						transaction_hash,
						block_number,
						block_hash,
						block_timestamp,
						context.clone(),
						evm as Arc<RefCell<dyn EVMContractCallTrait>>,
						db_tx_conn1,
						db_pool_conn1,
						event_tx,
					) {
						Ok(cim) => cim,
						Err(e) => {
							tx.send(Err(e)).unwrap();
							return
						},
					};
					let runtime_inner = Runtime::new(
						Rc::new(contract_code.clone()),
						Rc::new(arguments.clone()),
						context.clone(),
						1024,
						100000,
					);
					let mut runtime = TaggedRuntime {
						kind: runtime_kind,
						inner: MaybeBorrowed::Owned(runtime_inner),
					};
					let reason = {
						let inner_runtime = &mut runtime.inner;
						println!("Before evm run");
						match inner_runtime.run(&mut contract_instance_manager) {
							Capture::Exit(reason) => {
								info!("inner_runtime.run: Capture::Exit(reason): {:?}", reason);
								reason
							},
							Capture::Trap(resolve) => match resolve {
								Resolve::Call(interrupt, _resolve) => {
									error!("inner_runtime.run: Resolve::Call(interrupt, resolve)");
									// Handle the Call resolve trap if needed
									ExitReason::Error(ExitError::Other(Cow::Owned(
										"inner_runtime.run: Resolve::Call(interrupt, resolve)"
											.to_string(),
									)))
								},
								Resolve::Create(interrupt, _resolve) => {
									error!(
										"inner_runtime.run: Resolve::Create(interrupt, resolve)"
									);
									// Handle the Create resolve trap if needed
									ExitReason::Error(ExitError::Other(Cow::Owned(
										"inner_runtime.run: Resolve::Create(interrupt, resolve)"
											.to_string(),
									)))
								},
							},
						}
					};
					let runtime_kind = runtime.kind;
					let (reason, maybe_address, (result, return_data)) = match runtime_kind {
						RuntimeKind::Create(created_address) => {
							let (reason, maybe_address, return_data) = Self::cleanup_for_create(
								config,
								created_address,
								reason,
								runtime.inner.machine().return_value(),
							);
							(reason, maybe_address, (Ok(()), return_data))
						},
						RuntimeKind::Call(code_address) => {
							let return_data = Self::cleanup_for_call(
								config,
								code_address,
								&reason,
								runtime.inner.machine().return_value(),
							);
							(reason, None, return_data)
						}, /*RuntimeKind::Execute => (reason, None,
						    * runtime.inner.machine().return_value()), */
					};
					info!("cleanup_for_call, return_data: {:?}", return_data);

					let return_data = match result {
						Ok(_) => return_data,
						Err(e) => {
							info!("cleanup_for_call, return_data error {:?}", e);
							tx.send(Ok((
								runtime_kind,
								return_data,
								ExitReason::Error(ExitError::Other(Cow::Owned(format!("{:?}", e)))),
							)))
							.expect("tx send");
							return;
							return_data
						},
					};
					let inner_runtime = &mut runtime.inner;
					let maybe_error = match runtime_kind {
						RuntimeKind::Create(_) =>
							inner_runtime.finish_create(reason, maybe_address, return_data.clone()),
						RuntimeKind::Call(_) =>
							inner_runtime.finish_call(reason, return_data.clone()),
					};
					// Early exit if passing on the result caused an error
					info!("cleanup_for_call, maybe_error: {:?}", maybe_error);
					if let Err(e) = maybe_error {
						tx.send(Ok((
							runtime_kind,
							return_data,
							ExitReason::Error(ExitError::Other(Cow::Owned(format!("{:?}", e)))),
						)))
						.expect("tx send");
					} else {
						tx.send(Ok((
							runtime_kind,
							return_data,
							ExitReason::Succeed(ExitSucceed::Returned),
						)))
						.expect("tx send");
					}
				});
			})
			.await;
		};
		let res = match rx.await {
			Ok(r) => r,
			Err(e) =>
				return (
					Capture::Exit((
						ExitReason::Error(ExitError::Other(Cow::Owned(format!("{:?}", e)))),
						format!("{:?}", e).into_bytes().into(),
					)),
					0,
				),
		};
		let (runtime_kind, return_data, exit_reason) = match res {
			Ok(v) => v,
			Err(e) =>
				return (
					Capture::Exit((
						ExitReason::Error(ExitError::Other(Cow::Owned(format!("{:?}", e)))),
						format!("{:?}", e).into_bytes().into(),
					)),
					0,
				),
		};
		match runtime_kind {
			RuntimeKind::Create(contract_instance_address) => {
				let contract_code_runtime = return_data.clone();
				match Self::store_evm_contract(
					&account_address,
					&cluster_address,
					&contract_instance_address.into(),
					access_type,
					nonce,
					&contract_code_runtime,
					&transaction_hash,
					block_number,
					db_tx_conn.clone(),
					db_pool_conn.clone(),
				)
				.await
				{
					Ok(contract_instance_address) => {
						info!(
							"*********** EVM DEPLOYED CONTRACT ADDRESS {:?}",
							hex::encode(contract_instance_address.clone())
						);
						//contract_instance_address
					},
					Err(e) => {
						return (
							Capture::Exit((
								ExitReason::Error(ExitError::Other(Cow::Owned(format!("{:?}", e)))),
								format!("{:?}", e).into_bytes().into(),
							)),
							0,
						);
					},
				}
			},
			RuntimeKind::Call(code_address) => {
				info!("RuntimeKind::Call(code_address): code_address: {:?}", code_address);
				info!("RuntimeKind::Call(code_address): return_data: {:?}", return_data);
				let event = Event::new(
					transaction_hash.clone(),
					return_data.clone(),
					block_number,
					EventType::EVM as i8,
					account_address,
					None,
				);
				info!("RuntimeKind::Call(code_address): event: {:?}", event);
				{
					let event_state = match EventState::new(&db_pool_conn).await {
						Ok(es) => es,
						Err(e) => {
							return (
								Capture::Exit((
									ExitReason::Error(ExitError::Other(Cow::Owned(format!(
										"{:?}",
										e
									)))),
									format!("{:?}", e).into_bytes().into(),
								)),
								0,
							);
						},
					};
					match event_state.create_event(&event).await {
						Ok(_) => {
							info!(
								"RuntimeKind::Call(code_address): create_event success: {:?}",
								event
							);
						},
						Err(e) => {
							return (
								Capture::Exit((
									ExitReason::Error(ExitError::Other(Cow::Owned(format!(
										"{:?}",
										e
									)))),
									format!("{:?}", e).into_bytes().into(),
								)),
								0,
							);
						},
					}
				}
			},
		};
		{
			let account_state = match AccountState::new(&db_tx_conn).await {
				Ok(ast) => ast,
				Err(e) => {
					return (
						Capture::Exit((
							ExitReason::Error(ExitError::Other(Cow::Owned(format!("{:?}", e)))),
							format!("{:?}", e).into_bytes().into(),
						)),
						0,
					);
				},
			};
			match account_state.increment_nonce(&account_address).await {
				Ok(_) => {},
				Err(e) => {
					return (
						Capture::Exit((
							ExitReason::Error(ExitError::Other(Cow::Owned(format!("{:?}", e)))),
							format!("{:?}", e).into_bytes().into(),
						)),
						0,
					);
				},
			};
		}
		info!("*********** EVM RESULT: {:?}", hex::encode(return_data.clone()));
		(Capture::Exit((exit_reason, return_data.into())), 0)
	}

	async fn store_evm_contract(
		account_address: &Address,
		cluster_address: &Address,
		contract_instance_address: &Address,
		access_type: AccessType,
		nonce: Nonce,
		contract_code: &ContractCode,
		transaction_hash: &TransactionHash,
		block_number: BlockNumber,
		db_tx_conn: Arc<&'a DbTxConn<'a>>,
		db_pool_conn: Arc<&'a DbTxConn<'a>>,
	) -> Result<EventData, Error> {
		let contract = Contract {
			address: contract_instance_address.clone(),
			access: access_type.clone(),
			code: contract_code.clone(),
			r#type: ContractType::EVM as i8,
			owner_address: account_address.clone(),
		};
		{
			let contract_state = ContractState::new(&db_tx_conn).await?;
			contract_state.store_contract(&contract).await?;
		}

		let contract_instance = ContractInstance {
			instance_address: contract_instance_address.clone(),
			contract_address: contract.address,
			owner_address: account_address.clone(),
		};
		{
			let contract_instance_state = ContractInstanceState::new(&db_tx_conn).await?;
			contract_instance_state.store_contract_instance(&contract_instance).await?;
		}
		info!(
			"*******EXECUTING EVM CONTRACT DEPLOYMENT******** CONTRACT ADDRESS: {:?}",
			hex::encode(contract_instance_address)
		);
		let event_data = "EVM Contract deployment succeeded";
		let event = Event::new(
			transaction_hash.clone(),
			event_data.as_bytes().to_vec(),
			block_number,
			EventType::EVM as i8,
			*contract_instance_address,
			None,
		);
		{
			let event_state = EventState::new(&db_tx_conn).await?;
			event_state.create_event(&event).await?;
		}
		Ok(contract_instance_address.to_vec())
	}
}

#[async_trait]
impl<'a> ContractExecutor<'a> for ExecuteContract {
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
		let contract_instance_address = create_evm_address(scheme, nonce);
		let kind = RuntimeKind::Create(contract_instance_address);
		let (result, gas) = self
			.execute_evm_runtime(
				account_address,
				cluster_address,
				access_type,
				contract_code,
				&vec![],
				nonce,
				&transaction_hash,
				false,
				block_number,
				block_hash,
				block_timestamp,
				kind,
				context,
				db_tx_conn.clone(),
				db_pool_conn.clone(),
				event_tx,
			)
			.await;
		let contract_instance_address: Address = contract_instance_address.into();
		let result = TokioScope::scope_and_block(|scope| {
			scope.spawn_blocking(move || {
				// Create a new Tokio runtime for the async block
				let rt = tokio::runtime::Runtime::new().expect("Failed to create a runtime");
				// Use the runtime to block on the async operation
				rt.block_on(async {
					let account_state = AccountState::new(&db_tx_conn.clone()).await?;
					let _ = AccountManager::new_system(&contract_instance_address, &account_state)
						.await?;
					Ok::<(), Error>(())
				})
			})
		});
		result.1.into_iter().next().unwrap().unwrap()?;
		Ok(contract_instance_address)
	}

	// FIXME: should take out for EVM
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
		Err(anyhow!("execute_contract_init not supported!"))
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
		let kind = RuntimeKind::Call(H160(contract_instance.instance_address));

		self.execute_evm_runtime(
			account_address,
			cluster_address,
			contract.access,
			&contract.code,
			arguments,
			nonce,
			transaction_hash,
			false,
			block_number,
			block_hash,
			block_timestamp,
			kind,
			context,
			db_tx_conn,
			db_pool_conn,
			event_tx,
		)
		.await
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
		let context = Context {
			// Execution address.
			address: H160(contract_instance.instance_address), // Does not affect pool id
			// Caller of the EVM.
			caller: H160(contract_instance.owner_address), // Does not affect pool id
			// Apparent value of the EVM.
			apparent_value: U256::zero(),
		};
		self.execute_contract_function_call(
			&contract_instance.instance_address,
			context,
			0,               //READONLY_CALL_DEFAULT_GAS_LIMIT
			cluster_address, //cluster address
			contract,
			contract_instance,
			function,
			arguments,
			nonce,
			block_hash,
			true,
			block_number,
			block_hash,
			block_timestamp,
			db_tx_conn,
			db_pool_conn,
			event_tx,
		)
		.await
	}
}

impl<'a> EVMContractCallTrait<'a> for ExecuteContract {
	fn execute_evm_contract_deployment(
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
		let rt = tokio::runtime::Builder::new_multi_thread()
			.enable_all()
			.build()
			.expect("error new_multi_thread");
		let handle = rt.handle();
		handle.block_on(async move {
			self.execute_contract_deployment(
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
			.await
		})
	}

	fn execute_evm_contract_call(
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
		let rt = tokio::runtime::Builder::new_multi_thread()
			.enable_all()
			.build()
			.expect("error new_multi_thread");
		let handle = rt.handle();
		handle.block_on(async move {
			self.execute_contract_function_call(
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
			.await
		})
	}

	fn execute_evm_function_call_readonly(
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
		let rt = tokio::runtime::Builder::new_multi_thread()
			.enable_all()
			.build()
			.expect("error new_multi_thread");
		let handle = rt.handle();
		// Spawn an async task
		handle.block_on(async move {
			self.execute_contract_function_call_read_only(
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
			.await
		})
	}
}
