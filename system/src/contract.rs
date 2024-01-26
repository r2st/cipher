use anyhow::{anyhow, Error, Result};
use primitives::{AccessType, Address, ContractCode, ContractType as ContractTypei8};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Contract {
	pub address: Address,
	pub access: AccessType,
	pub r#type: ContractTypei8,
	pub code: ContractCode,
	pub owner_address: Address,
}

impl Contract {
	pub fn new(
		address: Address,
		access: AccessType,
		r#type: ContractTypei8,
		code: ContractCode,
		owner_address: Address,
	) -> Contract {
		Contract { address, access, r#type, code, owner_address }
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[repr(i8)]
pub enum ContractType {
	EVM = 0,
}

impl TryInto<ContractType> for i8 {
	type Error = Error;

	fn try_into(self) -> Result<ContractType, Self::Error> {
		match self {
			0 => Ok(ContractType::EVM),
			_ => Err(anyhow!("Invalid contract type {}", self)),
		}
	}
}
