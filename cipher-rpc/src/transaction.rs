use crate::primitives::*;
use anyhow::{anyhow, Error};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[repr(i8)]
pub enum AccessType {
	PRIVATE = 0,
	PUBLIC = 1,
	// Will be used in future to restrict the contract to be initiated by only specified addresses
	RESTICTED = 2,
}

impl TryInto<AccessType> for i32 {
	type Error = Error;

	fn try_into(self) -> Result<AccessType, Self::Error> {
		match self {
			0 => Ok(AccessType::PRIVATE),
			1 => Ok(AccessType::PUBLIC),
			2 => Ok(AccessType::RESTICTED),
			_ => Err(anyhow!("Invalid access type {}", self)),
		}
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[repr(i8)]
pub enum ContractType {
	CIPHERVM = 0,
	EVM = 1,
	XTALK = 2,
}

impl TryInto<ContractType> for i32 {
	type Error = Error;

	fn try_into(self) -> Result<ContractType, Self::Error> {
		match self {
			0 => Ok(ContractType::CIPHERVM),
			1 => Ok(ContractType::EVM),
			2 => Ok(ContractType::XTALK),
			_ => Err(anyhow!("Invalid contract type {}", self)),
		}
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Transaction {
	pub nonce: Nonce,
	pub transaction_type: TransactionType,
	pub fee_limit: Balance,
	#[serde(with = "serde_bytes")]
	pub signature: SignatureBytes,
	#[serde(with = "serde_bytes")]
	pub verifying_key: VerifyingKeyBytes,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TransactionType {
	NativeTokenTransfer(Address, Balance),
	SmartContractDeployment {
		access_type: AccessType,
		contract_type: ContractType,
		contract_code: ContractCode,
		value: Balance,
		salt: Salt,
	},
	SmartContractInit(Address, ContractArgument),
	SmartContractFunctionCall {
		contract_instance_address: Address,
		function: ContractFunction,
		arguments: ContractArgument,
	},
	CreateStakingPool {
		contract_instance_address: Option<Address>,
		min_stake: Option<Balance>,
		max_stake: Option<Balance>,
		min_pool_balance: Option<Balance>,
		max_pool_balance: Option<Balance>,
		staking_period: Option<BlockNumber>,
	},
	Stake {
		pool_address: Address,
		amount: Balance,
	},
	UnStake {
		pool_address: Address,
		amount: Balance,
	},
	StakingPoolContract {
		pool_address: Address,
		contract_instance_address: Address,
	},
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TXSignPayload {
	pub nonce: Nonce,
	pub transaction_type: TransactionType,
	pub fee_limit: Balance,
}
