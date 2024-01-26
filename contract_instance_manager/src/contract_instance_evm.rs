use crate::contract_instance_manager::ContractInstanceManager;
use account::{account_manager::AccountManager, account_state::AccountState};
use anyhow::{anyhow, Error};
use contract::contract_state::ContractState;
use contract_instance::contract_instance_state::ContractInstanceState;
use db::db::DbTxConn;
use event::event_state::EventState;
use evm_helper::tagged_runtime::{RuntimeInterrupt, RuntimeKind, TaggedRuntime};
use evm_interpreter::{RuntimeState, TransactionContext};
use evm_precompile::{
	standard::{InvokerState, State},
	Blake2F, Bn128Add, Bn128Mul, Bn128Pairing, ECRecover, Identity, Modexp, PurePrecompile,
	Ripemd160, Sha256,
};
use evm_runtime::{
	Capture, Config, Context, CreateScheme, ExitError, ExitReason, ExitSucceed, ExternalOperation,
	Handler, Opcode, Stack, Transfer,
};
use generic_array::{typenum::U32, GenericArray};
use log::{info, warn};
use num_bigint::BigUint;
use num_traits::FromPrimitive;
use primitive_types::{H160, H256, U256};
use primitives::*;
use sha3::{Digest, Keccak256};
use std::{
	borrow::Cow,
	convert::{Infallible, TryFrom},
	rc::Rc,
	sync::Arc,
};
use system::{
	access::AccessType,
	contract::{Contract, ContractType},
	contract_instance::ContractInstance,
	event::{Event, EventType},
	network::EventBroadcast,
};
use util::convert::{get_U256_value, u256_to_balance};
const fn address(last: u8) -> H160 {
	H160([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, last])
}
pub async fn transer_token<'a>(
	transfer: Transfer,
	db_tx_conn: &'a DbTxConn<'a>,
) -> Result<(), Error> {
	info!("transer_token 1 - {:?}", transfer);
	if transfer.value <= get_U256_value(0) {
		return Ok(())
	}
	let from_address: Address = transfer.source.into();
	let to_address: Address = transfer.target.into();
	info!("transer_token 2 - {:?} : {:?}", from_address, to_address);
	if let Some(amount) = u256_to_balance(&transfer.value) {
		let account_state = AccountState::new(db_tx_conn).await?;
		let account = account_state.get_account(&from_address).await?;
		let mut account_manager = AccountManager { account };
		account_manager.transfer(&to_address, &amount, &account_state).await?;
	}
	Ok::<(), Error>(())
}

impl<'a> Handler for ContractInstanceManager<'a> {
	type CreateInterrupt = evm_helper::tagged_runtime::CreateInterrupt;
	type CreateFeedback = evm_helper::tagged_runtime::CreateFeedback;
	type CallInterrupt = evm_helper::tagged_runtime::CallInterrupt;
	type CallFeedback = evm_helper::tagged_runtime::CallFeedback;

	//Get balance of address.
	fn balance(&self, address_evm: H160) -> U256 {
		info!("Handler: balance: {:?}", address_evm);
		let address: Address = address_evm.into();
		let handle = self.rt.handle();
		let db_tx_conn = self.db_tx_conn.clone();
		let balance: Balance = handle.block_on(async move {
			let account_state =
				AccountState::new(&db_tx_conn).await.expect("Unable to connect to DB");
			match account_state.get_balance(&address).await {
				Ok(b) => b,
				Err(_e) => 0,
			}
		});

		get_U256_value(balance)
	}

	//Get code size of address.
	fn code_size(&self, contract_evm_address: H160) -> U256 {
		info!("Handler: code_size: {:?}", contract_evm_address);
		let contract_instance_address: Address = contract_evm_address.into();
		let handle = self.rt.handle();
		let db_tx_conn = self.db_tx_conn.clone();
		let code_size: U256 = match handle.block_on(async move {
			let contract_instance_state = ContractInstanceState::new(&db_tx_conn).await?;
			let contract_instance = contract_instance_state
				.get_contract_instance(&contract_instance_address)
				.await?;
			let contract_state = ContractState::new(&db_tx_conn).await?;
			let contract = contract_state.get_contract(&contract_instance.contract_address).await?;
			info!("Handler: code_size: len: {:?}", contract.code.len());
			Ok::<usize, Error>(contract.code.len())
		}) {
			Ok(code_size) => U256::try_from(code_size).unwrap_or_default(),
			Err(e) => {
				info!("Handler: code_size: error: {:?}", e);
				U256::zero()
			},
		};

		code_size
	}

	//Get code hash of address.
	fn code_hash(&self, contract_evm_address: H160) -> H256 {
		info!("Handler: code_hash: {:?}", contract_evm_address);
		let contract_instance_address: Address = contract_evm_address.into();
		let handle = self.rt.handle();
		let db_tx_conn = self.db_tx_conn.clone();
		let code_hash: H256 = match handle.block_on(async move {
			let contract_instance_state = ContractInstanceState::new(&db_tx_conn).await?;
			let contract_instance = contract_instance_state
				.get_contract_instance(&contract_instance_address)
				.await?;
			let contract_state = ContractState::new(&db_tx_conn).await?;
			let contract = contract_state.get_contract(&contract_instance.contract_address).await?;

			let code_hash = H256::from_slice(Keccak256::digest(&contract.code).as_slice());
			Ok::<H256, Error>(code_hash)
		}) {
			Ok(code_hash) => code_hash,
			Err(_) => H256::zero(), // Default value for H256
		};

		code_hash
	}

	//Get code of address.
	fn code(&self, contract_evm_address: H160) -> Vec<u8> {
		info!("Handler: code: {:?}", contract_evm_address);
		let contract_instance_address: Address = contract_evm_address.into();
		let handle = self.rt.handle();
		let db_tx_conn = self.db_tx_conn.clone();
		let (code, _contract_instance) = match handle.block_on(async move {
			let contract_instance_state = ContractInstanceState::new(&db_tx_conn).await?;
			let contract_instance = contract_instance_state
				.get_contract_instance(&contract_instance_address)
				.await?;
			let contract_state = ContractState::new(&db_tx_conn).await?;
			let contract = contract_state.get_contract(&contract_instance.contract_address).await?;

			Ok::<(Vec<u8>, ContractInstance), Error>((contract.code, contract_instance))
		}) {
			Ok((code, contract_instance)) => (code, contract_instance),
			Err(_) => (ContractCode::default(), ContractInstance::default()),
		};
		code
	}

	//Get storage value of address at index.
	fn storage(&self, contract_evm_address: H160, key_evm: H256) -> H256 {
		info!("Handler: storage: {:?}", contract_evm_address);
		let contract_instance_address: Address = contract_evm_address.into();
		let key: Vec<u8> = key_evm.as_bytes().to_vec();
		let handle = self.rt.handle();
		let key = key.clone();
		let contract_address = contract_instance_address.clone();
		let db_tx_conn = self.db_tx_conn.clone();
		let value = match handle.block_on(async move {
			let contract_instance_state = ContractInstanceState::new(&db_tx_conn).await?;
			contract_instance_state.get_state_key_value(&contract_address, &key).await
		}) {
			Ok(v) => v,
			Err(_) => Some(ContractInstanceKey::default()),
		};
		match value {
			Some(v) => H256::from_slice(&v),
			None => H256::from([0; 32]),
		}
	}

	//Get original storage value of address at index.
	fn original_storage(&self, contract_instance_address_evm: H160, key_evm: H256) -> H256 {
		info!("Handler: original_storage: {:?}", contract_instance_address_evm);
		let contract_instance_address: Address = contract_instance_address_evm.into();
		let key: Vec<u8> = key_evm.as_bytes().to_vec();
		let handle = self.rt.handle();
		let key = key.clone();
		let contract_address = contract_instance_address.clone();
		let db_tx_conn = self.db_tx_conn.clone();
		let value = match handle.block_on(async move {
			let contract_instance_state = ContractInstanceState::new(&db_tx_conn).await?;
			contract_instance_state.get_state_key_value(&contract_address, &key).await
		}) {
			Ok(v) => v,
			Err(_) => Some(ContractInstanceKey::default()),
		};
		match value {
			Some(v) => H256::from_slice(&v),
			None => H256::from([0; 32]),
		}
	}

	//Get the gas left value.
	fn gas_left(&self) -> U256 {
		info!("Handler: gas_left");
		get_U256_value(u128::MAX)
	}

	//Get the gas price value.
	fn gas_price(&self) -> U256 {
		info!("Handler: gas_price");
		get_U256_value(0)
	}

	//Get execution origin.
	fn origin(&self) -> H160 {
		info!("Handler: origin");
		if let Some(contract_instance) = &self.contract_instance {
			H160(contract_instance.instance_address)
		} else {
			H160(self.account_address)
		}
	}

	//Get environmental block hash.
	fn block_hash(&self, tx_hash_evm: U256) -> H256 {
		info!("Handler: block_hash: {:?}", tx_hash_evm);
		self.block_hash.into()
	}

	//Get environmental block number.
	fn block_number(&self) -> U256 {
		info!("Handler: block_number");
		let block_number_biguint: BigUint =
			BigUint::from_u128(self.block_number).expect("Convert to BigUint failed");
		let block_number_u256: U256 = U256::from_big_endian(&block_number_biguint.to_bytes_be());

		block_number_u256
	}

	//Get environmental coinbase.
	fn block_coinbase(&self) -> H160 {
		info!("Handler: block_coinbase");
		H160([0u8; 20])
	}

	//Get environmental block timestamp.
	fn block_timestamp(&self) -> U256 {
		/*let block_timestamp_biguint: BigUint =
			BigUint::from_u64(self.block_timestamp / 1000000).expect("Convert to BigUint failed");
		let block_timestamp_u256: U256 =
			U256::from_big_endian(&block_timestamp_biguint.to_bytes_be());*/
		let block_timestamp_u256 = U256::from(self.block_timestamp);
		info!("Handler: block_timestamp: {:?}", block_timestamp_u256);
		block_timestamp_u256
	}

	//Get environmental block difficulty.
	fn block_difficulty(&self) -> U256 {
		info!("Handler: block_difficulty");
		get_U256_value(0)
	}

	//Get environmental block randomness.
	fn block_randomness(&self) -> Option<H256> {
		info!("Handler: block_randomness");
		None
	}

	//Get environmental gas limit.
	fn block_gas_limit(&self) -> U256 {
		info!("Handler: block_gas_limit");
		get_U256_value(0)
	}

	//Environmental block base fee.
	fn block_base_fee_per_gas(&self) -> U256 {
		info!("Handler: block_base_fee_per_gas");
		get_U256_value(0)
	}

	//Get environmental chain ID.
	fn chain_id(&self) -> U256 {
		info!("Handler: chain_id");
		get_U256_value(0)
	}

	//Check whether an address exists.
	fn exists(&self, address_evm: H160) -> bool {
		info!("Handler: exists: {:?}", address_evm);
		let address: Address = address_evm.into();
		let handle = self.rt.handle();
		let db_tx_conn = self.db_tx_conn.clone();
		let exists = match handle.block_on(async move {
			let account_state = AccountState::new(&db_tx_conn).await?;
			account_state.is_valid_account(&address).await
		}) {
			Ok(v) => v,
			Err(_) => false,
		};
		exists
	}

	//Check whether an address has already been deleted.
	fn deleted(&self, address_evm: H160) -> bool {
		info!("Handler: deleted: {:?}", address_evm);
		let address: Address = address_evm.into();
		let handle = self.rt.handle();
		let db_tx_conn = self.db_tx_conn.clone();
		let exists = match handle.block_on(async move {
			let account_state = AccountState::new(&db_tx_conn).await?;
			account_state.is_valid_account(&address).await
		}) {
			Ok(v) => v,
			Err(_) => false,
		};
		!exists
	}

	/*
	Checks if the address or (address, index) pair has been previously accessed (or set in accessed_addresses / accessed_storage_keys via an access list transaction). References:

	https://eips.ethereum.org/EIPS/eip-2929
	https://eips.ethereum.org/EIPS/eip-2930
	*/
	fn is_cold(
		&mut self,
		contract_evm_address: H160,
		_: Option<H256>,
	) -> std::result::Result<bool, ExitError> {
		info!("Handler: is_cold: {:?}", contract_evm_address);
		Ok(false)
	}

	//Set storage value of address at index.
	fn set_storage(
		&mut self,
		contract_instance_address_evm: H160,
		key_evm: H256,
		value_evm: H256,
	) -> Result<(), ExitError> {
		info!("Handler: set_storage: {:?}", contract_instance_address_evm);
		let contract_instance_address: Address = contract_instance_address_evm.into();
		let key: Vec<u8> = key_evm.as_bytes().to_vec();
		let value: Vec<u8> = value_evm.as_bytes().to_vec();
		let handle = self.rt.handle();
		let db_tx_conn = self.db_tx_conn.clone();
		match handle.block_on(async move {
			let contract_instance_state = ContractInstanceState::new(&db_tx_conn)
				.await
				.map_err(|err| ExitError::Other(Cow::Owned(format!("{:?}", err))))?;
			contract_instance_state
				.store_state_key_value(&contract_instance_address, &key, &value)
				.await
				.map_err(|err| ExitError::Other(Cow::Owned(format!("{:?}", err))))
		}) {
			Ok(_) => Ok(()),
			Err(e) => Err(ExitError::Other(Cow::Owned(format!("{:?}", e)))),
		}
	}

	//Create a log owned by address with given topics and data.
	fn log(
		&mut self,
		contract_evm_address: H160,
		topics: Vec<H256>,
		event_data: Vec<u8>,
	) -> std::result::Result<(), ExitError> {
		info!("Handler: log: contract_evm_address: {:?}", contract_evm_address);
		// info!(
		//     "Handler: log: event_data: {:?}",
		//     std::str::from_utf8(&event_data).unwrap()
		// );
		info!("Handler: log: topics: {:?}", topics);
		let handle = self.rt.handle();
		let transaction_hash_arr = self.transaction_hash.clone();

		let event_tx = self.event_tx.clone();

		let block_number = self.block_number;
		let block_hash = self.block_hash;
		let transaction_hash = self.transaction_hash;
		let db_tx_conn = self.db_tx_conn.clone();
		match handle.block_on(async move {
			if let Err(e) = event_tx.send(EventBroadcast::Evm(
				contract_evm_address,
				topics.clone(),
				event_data.clone(),
				block_number,
				block_hash,
				transaction_hash,
				0, // FIXME: txn_index
				0, // FIXME: log_index
			)) {
				return Err(ExitError::Other(Cow::Owned(format!("{:?}", e))))
			}

			let event_state = EventState::new(&db_tx_conn)
				.await
				.map_err(|err| ExitError::Other(Cow::Owned(format!("{:?}", err))))?;
			event_state
				.create_event(&Event::new(
					transaction_hash_arr,
					event_data,
					block_number,
					EventType::EVM as i8,
					contract_evm_address.into(),
					Some(topics),
				))
				.await
				.map_err(|err| ExitError::Other(Cow::Owned(format!("{:?}", err))))
		}) {
			Ok(_) => Ok(()),
			Err(e) => Err(ExitError::Other(Cow::Owned(format!("{:?}", e)))),
		}
	}

	//Mark an address to be deleted, with funds transferred to target.
	fn mark_delete(
		&mut self,
		contract_evm_address: H160,
		contract_target_evm_address: H160,
	) -> std::result::Result<(), ExitError> {
		info!(
			"Handler: mark_delete: {:?} , target: {:?}",
			contract_evm_address, contract_target_evm_address
		);
		//Mark an address to be deleted, with funds transferred to target.
		Ok(())
	}

	//Invoke a create operation.
	fn create(
		&mut self,
		caller_evm: H160,
		scheme: CreateScheme,
		value: U256,
		init_code: Vec<u8>,
		_target_gas: Option<u64>,
	) -> Capture<(ExitReason, Option<H160>, Vec<u8>), <Self as Handler>::CreateInterrupt> {
		info!("Handler: create: {:?}", caller_evm);
		info!("Handler: create: init_code: {:?}", init_code);
		let handle = self.rt.handle();
		let account_address: Address = caller_evm.into();
		let cluster_address = self.cluster_address.clone();
		let nonce = self.nonce.clone();
		let _block_number = self.block_number;
		let _block_hash = self.block_hash;
		let _block_timestamp = self.block_timestamp;
		let contract_code: ContractCode = init_code;
		let db_tx_conn = self.db_tx_conn.clone();
		let db_pool_conn = self.db_pool_conn.clone();
		// Return the contract_instance_address as part of the capture result.
		match self.evm.execute_contract_deployment(
			&account_address,
			scheme,
			self.context.clone(),
			&cluster_address, //cluster address
			AccessType::PRIVATE as i8,
			&contract_code,
			nonce,
			&self.transaction_hash,
			self.block_number,
			&self.block_hash,
			self.block_timestamp,
			db_tx_conn,
			db_pool_conn,
			self.event_tx.clone(),
		) {
			Ok(contract_instance_address) => {
				info!("Handler: create: {:?}", contract_instance_address);
				let transfer = Transfer {
					source: H160(account_address),
					target: H160(contract_instance_address),
					value,
				};
				let db_tx_conn = self.db_tx_conn.clone();
				match handle.block_on(async move { transer_token(transfer, &db_tx_conn).await }) {
					Ok(()) => (),
					Err(e) => {
						info!("Handler: call: error: transer_token: {:?}", e);
						return Capture::Exit((
							ExitReason::Error(ExitError::Other(Cow::Owned(format!("{:?}", e)))),
							None,
							contract_code,
						))
					},
				}
				Capture::Exit((
					ExitReason::Succeed(ExitSucceed::Returned),
					Some(H160(contract_instance_address)),
					contract_code,
				))
			},
			Err(e) => {
				info!("Handler: create: error: {:?}", e);
				Capture::Exit((
					ExitReason::Error(ExitError::Other(Cow::Owned(format!("{:?}", e)))),
					None,
					contract_code,
				))
			},
		}
	}
	/*
	The default precompile addresses in Ethereum are as follows:
	0x0000000000000000000000000000000000000001: ECDSA recovery.
	0x0000000000000000000000000000000000000002: SHA-256 hash function.
	0x0000000000000000000000000000000000000003: RIPEMD-160 hash function.
	0x0000000000000000000000000000000000000004: Identity function (data copy).
	0x0000000000000000000000000000000000000005 to 0x0000000000000000000000000000000000000009: Precompiles for modular exponentiation, elliptic curve addition, scalar multiplication, and pairings on certain elliptic curve groups introduced in later Ethereum versions for various cryptographic operations.
	These precompiled contracts are designed to provide efficient computation of specific cryptographic algorithms and operations, which would be prohibitively expensive to compute in EVM bytecode.
	 */
	//Invoke a call operation.
	fn call(
		&mut self,
		contract_instance_address_evm: H160,
		transfer: Option<Transfer>,
		input: Vec<u8>,
		target_gas: Option<u64>,
		is_static: bool,
		context: Context,
	) -> Capture<(ExitReason, Vec<u8>), <Self as Handler>::CallInterrupt> {
		info!("Handler: call: {:?}", contract_instance_address_evm);
		info!("Handler: call: input: {:?}", input);
		let config_shanghai = Config::shanghai();
		let mut gasometer = match State::new_transact_call(
			RuntimeState {
				context: evm_interpreter::Context {
					address: H160::default(),
					caller: H160::default(),
					apparent_value: U256::default(),
				},
				transaction_context: TransactionContext {
					gas_price: U256::default(),
					origin: H160::default(),
				}
				.into(),
				retbuf: Vec::new(),
			},
			U256::from(1000000),
			&[],
			&[],
			&config_shanghai,
		) {
			Ok(res) => res,
			Err(e) => {
				info!("Handler: call: error: new_transact_call: {:?}", e);
				return Capture::Exit((
					ExitReason::Error(ExitError::Other(Cow::Owned(format!("{:?}", e)))),
					format!("{:?}", e).into(),
				))
			},
		};
		let result = if contract_instance_address_evm == address(1) {
			Some(ECRecover.execute(&input, &mut gasometer))
		} else if contract_instance_address_evm == address(2) {
			Some(Sha256.execute(&input, &mut gasometer))
		} else if contract_instance_address_evm == address(3) {
			Some(Ripemd160.execute(&input, &mut gasometer))
		} else if contract_instance_address_evm == address(4) {
			Some(Identity.execute(&input, &mut gasometer))
		} else if contract_instance_address_evm == address(5) {
			Some(Modexp.execute(&input, &mut gasometer))
		} else if contract_instance_address_evm == address(6) {
			Some(Bn128Add.execute(&input, &mut gasometer))
		} else if contract_instance_address_evm == address(7) {
			Some(Bn128Mul.execute(&input, &mut gasometer))
		} else if contract_instance_address_evm == address(8) {
			Some(Bn128Pairing.execute(&input, &mut gasometer))
		} else if contract_instance_address_evm == address(9) {
			Some(Blake2F.execute(&input, &mut gasometer))
		} else {
			None
		};

		if let Some(result) = result {
			let (result, data) = result;
			return match result {
				Ok(_res) => {
					info!(
						"Handler: call: precompile : {:?} : {:?}",
						contract_instance_address_evm,
						hex::encode(data.clone())
					);
					Capture::Exit((ExitReason::Succeed(ExitSucceed::Returned), data))
				},
				Err(e) => {
					info!("Handler: call: precompile : error: {:?}", e);
					Capture::Exit((
						ExitReason::Error(ExitError::Other(Cow::Owned(format!("{:?}", e)))),
						data,
					))
				},
			};
		}
		let contract_instance_address: Address = contract_instance_address_evm.into();

		let handle = self.rt.handle();
		if let Some(transfer) = transfer {
			let db_tx_conn = self.db_tx_conn.clone();
			match handle.block_on(async move { transer_token(transfer, &db_tx_conn).await }) {
				Ok(()) => (),
				Err(e) => {
					info!("Handler: call: error: transer_token 2: {:?}", e);
					return Capture::Exit((
						ExitReason::Error(ExitError::Other(Cow::Owned(format!("{:?}", e)))),
						e.to_string().into_bytes(),
					))
				},
			}
		}
		let db_tx_conn = self.db_tx_conn.clone();
		let (contract, contract_instance) = match handle.block_on(async move {
			let contract_instance = {
				let contract_instance_state = ContractInstanceState::new(&db_tx_conn).await?;
				let contract_instance = contract_instance_state
					.get_contract_instance(&contract_instance_address)
					.await?;
				contract_instance
			};
			let contract_state = ContractState::new(&db_tx_conn).await?;
			let contract = contract_state.get_contract(&contract_instance.contract_address).await?;
			Ok::<(Contract, ContractInstance), Error>((contract, contract_instance))
		}) {
			Ok((contract, contract_instance)) => (contract, contract_instance),
			Err(e) => {
				info!("Handler: call: error: get_contract {:?}", e);
				return Capture::Exit((
					ExitReason::Error(ExitError::Other(Cow::Owned(format!("{:?}", e)))),
					e.to_string().into_bytes(),
				))
			},
		};
		let contract_type = match contract.r#type.try_into() {
			Ok(ct) => ct,
			Err(e) => {
				info!("Handler: call: error: contract_type: {:?}", e);
				return Capture::Exit((
					ExitReason::Error(ExitError::Other(Cow::Owned(format!("{:?}", e)))),
					format!("{:?}", e).to_string().into_bytes(),
				))
			},
		};
		let db_tx_conn = self.db_tx_conn.clone();
		let db_pool_conn = self.db_pool_conn.clone();
		let (result, _gas) = match contract_type {
			ContractType::EVM => {
				info!("Handler: call: contract size: {:?}", contract.code.len());
				self.evm.execute_contract_function_call(
					&contract_instance_address,
					context,
					target_gas.unwrap_or_default(),
					&self.cluster_address, //cluster address
					&contract,
					&contract_instance,
					&vec![],
					&input,
					self.nonce,
					&self.transaction_hash,
					is_static,
					self.block_number,
					&self.block_hash,
					self.block_timestamp,
					db_tx_conn,
					db_pool_conn,
					self.event_tx.clone(),
				)
			},
		};
		match result {
			Capture::Exit(reason) => {
				let (reason, result) = reason;
				info!("Handler: call: reason: {:?}", reason);
				info!("Handler: call: result: {:?}", result);
				Capture::Exit((reason, (*result).clone()))
			},
			Capture::Trap(resolve) => Capture::Trap(resolve),
		}
	}

	//Pre-validation step for the runtime.
	fn pre_validate(
		&mut self,
		_context: &Context,
		opcode: Opcode,
		_stack: &Stack,
	) -> std::result::Result<(), ExitError> {
		//info!("Handler: pre_validate: {:?}", opcode);
		let valid_opcodes: Vec<u8> = vec![
			0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x10,
			0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x20,
			0x21, 0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x3a, 0x3b, 0x3c,
			0x3d, 0x3e, 0x3f, 0x40, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48, 0x50, 0x51,
			0x52, 0x53, 0x54, 0x55, 0x56, 0x57, 0x58, 0x59, 0x5a, 0x5b, 0x5c, 0x60, 0x61, 0x62,
			0x63, 0x64, 0x65, 0x66, 0x67, 0x68, 0x69, 0x6a, 0x6b, 0x6c, 0x6d, 0x6e, 0x6f, 0x70,
			0x71, 0x72, 0x73, 0x74, 0x75, 0x76, 0x77, 0x78, 0x79, 0x7a, 0x7b, 0x7c, 0x7d, 0x7e,
			0x7f, 0x80, 0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88, 0x89, 0x8a, 0x8b, 0x8c,
			0x8d, 0x8e, 0x8f, 0x90, 0x91, 0x92, 0x93, 0x94, 0x95, 0x96, 0x97, 0x98, 0x99, 0x9a,
			0x9b, 0x9c, 0x9d, 0x9e, 0x9f, 0xa0, 0xa1, 0xa2, 0xa3, 0xa4, 0xa5, 0xb0, 0xb1, 0xb2,
			0xb4, 0xb5, 0xb6, 0xb8, 0xb9, 0xba, 0xbb, 0xe1, 0xe2, 0xe3, 0xe4, 0xf0, 0xf1, 0xf2,
			0xf3, 0xf4, 0xf5, 0xf6, 0xfa, 0xfb, 0xfc, 0xfd, 0xfe, 0xff,
		];

		if !valid_opcodes.contains(&opcode.as_u8()) {
			info!("Handler: pre_validate error: InvalidCode: {:?}", opcode);
			return Err(ExitError::InvalidCode(opcode))
		}

		Ok(())
	}

	fn record_external_operation(
		&mut self,
		_: ExternalOperation,
	) -> std::result::Result<(), ExitError> {
		Ok(())
	}
}
