use std::fmt;

use primitives::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Validator {
	pub address: Address,
	pub cluster_address: Address,
	pub block_number: BlockNumber,
	pub stake: Balance,
}

impl Validator {
	pub fn new(
		address: Address,
		cluster_address: Address,
		block_number: BlockNumber,
		stake: Balance,
	) -> Validator {
		Validator { address, cluster_address, block_number, stake }
	}
}

impl fmt::Display for Validator {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(
			f,
			"Validator {{ address: 0x{}, cluster_address: 0x{}, block #{}, stake: {} CIPHER tokens }}",
			hex::encode(self.address),
			hex::encode(self.cluster_address),
			self.block_number,
			self.stake
		)
	}
}
