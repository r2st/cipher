use anyhow::{anyhow, Error};
use bigdecimal::BigDecimal;
use cipher_rpc::rpc_model::{
	self, transaction, NativeTokenTransfer, SmartContractDeployment, SmartContractFunctionCall,
	SmartContractInit, Stake, Transaction, UnStake,
};
use num_bigint::{BigInt, BigUint};
use num_traits::FromPrimitive;
use primitive_types::U256;
use primitives::*;
use system::{block::BlockType, errors::NodeError};

pub fn u256_to_balance(u256_value: &U256) -> Option<Balance> {
	// Convert the U256 to a u128 (Balance) manually.
	// Check if the U256 value can fit within a u128.
	if u256_value >= &U256::from(u128::MAX) {
		// Conversion not possible due to truncation/overflow.
		None
	} else {
		// Conversion is safe, perform the conversion.
		Some(u256_value.as_u128())
	}
}

pub fn get_U256_value(value: Balance) -> U256 {
	let value_biguint: BigUint = BigUint::from_u128(value).expect("Convert to BigUint failed");
	let value_u256: U256 = U256::from_big_endian(&value_biguint.to_bytes_be());
	value_u256
}

/// Try to convert a vector of bytes to an address
pub fn bytes_to_address(bytes: Vec<u8>) -> Result<Address, Error> {
	if bytes.len() != 20 {
		return Err(anyhow!("Invalid address length: {}", bytes.len()));
	}
	let mut address = Address::default();
	address.copy_from_slice(&bytes);
	Ok(address.into())
}

/// Hex string formatted address to bytes `Address`
pub fn address_from_str(hex_str: &str) -> Result<Address, anyhow::Error> {
	// Remove the '0x' prefix if present and decode the remaining hex string
	let bytes =
		hex::decode(hex_str.trim()).map_err(|e| anyhow!("Failed to decode hex string: {}", e))?;

	if bytes.len() != 20 {
		return Err(anyhow!("Address must be 20 bytes long"));
	}

	let mut address = [0u8; 20];
	address.copy_from_slice(&bytes);
	Ok(address)
}

/// Try to convert a vector of bytes to a block hash
pub fn bytes_to_hash(bytes: Vec<u8>) -> Result<BlockHash, Error> {
	if bytes.len() != 32 {
		return Err(anyhow!("Invalid address length: {}", bytes.len()));
	}
	let mut hash = BlockHash::default();
	hash.copy_from_slice(&bytes);
	Ok(hash.into())
}

/// Convert from a system transaction type to a proto transaction type
pub async fn to_proto_transaction(
	txn: system::transaction::Transaction,
) -> Result<Transaction, NodeError> {
	let nonce = txn.nonce.to_string();
	let fee_limit = txn.fee_limit.to_string();
	let signature = txn.signature;
	let verifying_key = txn.verifying_key;
	match txn.transaction_type {
		system::transaction::TransactionType::NativeTokenTransfer(address, balance) =>
			Ok(Transaction {
				tx_type: rpc_model::TransactionType::NativeTokenTransfer as i32,
				transaction: Some(transaction::Transaction::NativeTokenTransfer(
					NativeTokenTransfer { address: address.to_vec(), amount: balance.to_string() },
				)),
				nonce,
				fee_limit,
				signature,
				verifying_key,
			}),
		system::transaction::TransactionType::SmartContractDeployment {
			access_type,
			contract_type,
			contract_code,
			value,
			salt,
		} => Ok(Transaction {
			tx_type: rpc_model::TransactionType::SmartContractDeployment as i32,
			transaction: Some(transaction::Transaction::SmartContractDeployment(
				SmartContractDeployment {
					access_type: access_type as i32,
					contract_type: contract_type as i32,
					contract_code,
					value: value as u64,
					salt,
				},
			)),
			nonce,
			fee_limit,
			signature,
			verifying_key,
		}),
		system::transaction::TransactionType::SmartContractInit(address, arguments) =>
			Ok(Transaction {
				tx_type: rpc_model::TransactionType::SmartContractInstantiation as i32,
				transaction: Some(transaction::Transaction::SmartContractInit(SmartContractInit {
					address: address.to_vec(),
					arguments,
				})),
				nonce,
				fee_limit,
				signature,
				verifying_key,
			}),
		system::transaction::TransactionType::SmartContractFunctionCall {
			contract_instance_address,
			function,
			arguments,
		} => Ok(Transaction {
			tx_type: rpc_model::TransactionType::SmartContractFunctionCall as i32,
			transaction: Some(transaction::Transaction::SmartContractFunctionCall(
				SmartContractFunctionCall {
					contract_address: contract_instance_address.to_vec(),
					function_name: function,
					arguments,
				},
			)),
			nonce,
			fee_limit,
			signature,
			verifying_key,
		}),
		system::transaction::TransactionType::Stake { pool_address, amount } => Ok(Transaction {
			tx_type: rpc_model::TransactionType::Stake as i32,
			transaction: Some(transaction::Transaction::Stake(Stake {
				pool_address: pool_address.to_vec(),
				amount: amount.to_string(),
			})),
			nonce,
			fee_limit,
			signature,
			verifying_key,
		}),
		system::transaction::TransactionType::UnStake { pool_address, amount } => Ok(Transaction {
			tx_type: rpc_model::TransactionType::Unstake as i32,
			transaction: Some(transaction::Transaction::Unstake(UnStake {
				pool_address: pool_address.to_vec(),
				amount: amount.to_string(),
			})),
			nonce,
			fee_limit,
			signature,
			verifying_key,
		}),
		_ => Err(NodeError::InvalidTransactionType(format!(
			"Invalid transaction type: {:?}",
			txn.transaction_type
		))),
	}
}

/// Convert a rpc transaction type to a system transaction type
pub async fn from_rpc_transaction(
	txn: cipher_rpc::transaction::Transaction,
) -> Result<system::transaction::Transaction, Error> {
	let nonce = txn.nonce;
	let transaction_type: system::transaction::TransactionType = match txn.transaction_type {
		cipher_rpc::transaction::TransactionType::NativeTokenTransfer(address, balance) =>
			system::transaction::TransactionType::NativeTokenTransfer(address, balance),
		cipher_rpc::transaction::TransactionType::SmartContractDeployment {
			access_type,
			contract_type,
			contract_code,
			value,
			salt,
		} => system::transaction::TransactionType::SmartContractDeployment {
			access_type: (access_type as i8).try_into()?,
			contract_type: (contract_type as i8).try_into()?,
			contract_code,
			value,
			salt,
		},
		cipher_rpc::transaction::TransactionType::SmartContractInit(address, arguments) =>
			system::transaction::TransactionType::SmartContractInit(address, arguments),
		cipher_rpc::transaction::TransactionType::SmartContractFunctionCall {
			contract_instance_address,
			function,
			arguments,
		} => system::transaction::TransactionType::SmartContractFunctionCall {
			contract_instance_address,
			function,
			arguments,
		},
		cipher_rpc::transaction::TransactionType::CreateStakingPool {
			contract_instance_address,
			min_stake,
			max_stake,
			min_pool_balance,
			max_pool_balance,
			staking_period,
		} => system::transaction::TransactionType::CreateStakingPool {
			contract_instance_address,
			min_stake,
			max_stake,
			min_pool_balance,
			max_pool_balance,
			staking_period,
		},
		cipher_rpc::transaction::TransactionType::Stake { pool_address, amount } =>
			system::transaction::TransactionType::Stake { pool_address, amount },
		cipher_rpc::transaction::TransactionType::UnStake { pool_address, amount } =>
			system::transaction::TransactionType::UnStake { pool_address, amount },
		cipher_rpc::transaction::TransactionType::StakingPoolContract {
			pool_address,
			contract_instance_address,
		} => system::transaction::TransactionType::StakingPoolContract {
			pool_address,
			contract_instance_address,
		},
	};
	let fee_limit = txn.fee_limit;
	let signature = txn.signature;
	let verifying_key = txn.verifying_key;

	let sys_txn = system::transaction::Transaction {
		nonce,
		transaction_type,
		fee_limit,
		signature,
		verifying_key,
	};

	Ok(sys_txn)
}

/// Convert a proto transaction type to a system transaction type
pub async fn from_proto_transaction(
	txn: cipher_rpc::rpc_model::Transaction,
) -> Result<system::transaction::Transaction, Error> {
	let transaction_type =
		txn.transaction.ok_or(anyhow!("Missing transaction in proto transaction"))?;

	let transaction_type = match transaction_type {
		transaction::Transaction::NativeTokenTransfer(native_token_transfer) => {
			let address = bytes_to_address(native_token_transfer.address)?;
			let balance = native_token_transfer.amount.parse::<Balance>()?;
			system::transaction::TransactionType::NativeTokenTransfer(address, balance)
		},
		transaction::Transaction::SmartContractDeployment(smart_contract_deployment) => {
			let access_type = smart_contract_deployment.access_type;
			let contract_type = smart_contract_deployment.contract_type;
			let contract_code = smart_contract_deployment.contract_code;
			let value: Balance = smart_contract_deployment.value.into();
			let salt = smart_contract_deployment.salt;
			system::transaction::TransactionType::SmartContractDeployment {
				access_type: (access_type as i8).try_into()?,
				contract_type: (contract_type as i8).try_into()?,
				contract_code,
				value,
				salt,
			}
		},
		transaction::Transaction::SmartContractInit(smart_contract_init) => {
			let address = bytes_to_address(smart_contract_init.address)?;
			let arguments = smart_contract_init.arguments;
			system::transaction::TransactionType::SmartContractInit(address, arguments)
		},
		transaction::Transaction::SmartContractFunctionCall(smart_contract_function_call) => {
			let contract_instance_address =
				bytes_to_address(smart_contract_function_call.contract_address)?;
			let function = smart_contract_function_call.function_name;
			let arguments = smart_contract_function_call.arguments;
			system::transaction::TransactionType::SmartContractFunctionCall {
				contract_instance_address,
				function,
				arguments,
			}
		},
		transaction::Transaction::Stake(stake) => {
			let pool_address = bytes_to_address(stake.pool_address)?;
			let amount = stake.amount.parse::<Balance>()?;
			system::transaction::TransactionType::Stake { pool_address, amount }
		},
		transaction::Transaction::Unstake(unstake) => {
			let pool_address = bytes_to_address(unstake.pool_address)?;
			let amount = unstake.amount.parse::<Balance>()?;
			system::transaction::TransactionType::UnStake { pool_address, amount }
		},
	};

	let nonce = txn.nonce.parse::<u128>()?;
	let fee_limit = txn.fee_limit.parse::<Balance>()?;
	let signature = txn.signature;
	let verifying_key = txn.verifying_key;

	let sys_txn = system::transaction::Transaction {
		nonce,
		transaction_type,
		fee_limit,
		signature,
		verifying_key,
	};

	Ok(sys_txn)
}

/// Convert a proto block type to a system block type
pub async fn from_proto_block(
	block: cipher_rpc::rpc_model::Block,
) -> Result<system::block::Block, Error> {
	let block_number = block.number.parse::<u128>()?;
	let block_hash = bytes_to_hash(hex::decode(block.hash)?)?;
	let parent_hash = bytes_to_hash(hex::decode(block.parent_hash)?)?;
	let block_type = match block.block_type {
		0 => BlockType::CIPHERTokenBlock,
		1 => BlockType::CIPHERTokenBlock,
		2 => BlockType::CIPHERContractBlock,
		3 => BlockType::XTalkTokenBlock, // Possibly a place for state issues/differences
		_ => return Err(anyhow!("Invalid block type: {}", block.block_type)),
	};
	let cluster_address = bytes_to_address(hex::decode(block.cluster_address)?)?;
	let num_transactions = block.transactions.len() as i32;

	let sys_block_header = system::block_header::BlockHeader {
		block_number,
		block_hash,
		parent_hash,
		block_type,
		cluster_address,
		timestamp: block.timestamp,
		num_transactions,
	};

	let mut transactions: Vec<system::transaction::Transaction> = Vec::new();
	for txn in block.transactions {
		let nested_txn = txn.transaction.ok_or(anyhow!("Missing transaction in proto block"))?;

		// Convert to system type transaction
		let converted_txn = from_proto_transaction(nested_txn).await?;
		transactions.push(converted_txn);
	}

	// Form the system type block
	let sys_block = system::block::Block { block_header: sys_block_header, transactions };

	Ok(sys_block)
}
pub fn convert_to_big_decimal(bal: primitives::Balance) -> BigDecimal {
	let int_val = BigInt::from(bal);
	BigDecimal::new(int_val, 0)
}

pub fn convert_to_big_decimal_balance(bal: primitives::Balance) -> BigDecimal {
	let int_val = BigInt::from(bal);
	BigDecimal::new(int_val, 0)
}

pub fn convert_to_big_decimal_block_number(bal: primitives::BlockNumber) -> BigDecimal {
	let int_val = BigInt::from(bal);
	BigDecimal::new(int_val, 0)
}

pub fn convert_to_big_decimal_tx_sequence(bal: primitives::TransactionSequence) -> BigDecimal {
	let int_val = BigInt::from(bal);
	BigDecimal::new(int_val, 0)
}
