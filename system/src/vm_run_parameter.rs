use primitives::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VMRunParameter {
	pub contract_code: ContractCode,
	pub function: ContractFunction,
	pub arguments: ContractArgument,
	pub contract_instance_address: Address,
	pub caller_address: Address,
	pub owner_address: Address,
	pub is_read_only: bool,
	pub block_number: BlockNumber,
}

impl VMRunParameter {
	pub fn new(
		contract_code: ContractCode,
		function: ContractFunction,
		arguments: ContractArgument,
		contract_instance_address: Address,
		caller_address: Address,
		owner_address: Address,
		is_read_only: bool,
		block_number: BlockNumber,
	) -> VMRunParameter {
		VMRunParameter {
			contract_code,
			function,
			arguments,
			contract_instance_address,
			caller_address,
			owner_address,
			is_read_only,
			block_number,
		}
	}
}
