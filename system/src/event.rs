use anyhow::{anyhow, Error, Result};
use primitive_types::H256;
use primitives::{EventType as EventTypei8, *};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Event {
	pub transaction_hash: TransactionHash,
	pub event_data: EventData,
	pub block_number: BlockNumber,
	pub event_type: EventTypei8,
	pub contract_address: Address,
	pub topics: Option<Vec<H256>>,
}

impl Event {
	pub fn new(
		transaction_hash: TransactionHash,
		event_data: EventData,
		block_number: BlockNumber,
		event_type: EventTypei8,
		contract_address: Address,
		topics: Option<Vec<H256>>,
	) -> Event {
		Event { transaction_hash, event_data, block_number, event_type, contract_address, topics }
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum EventType {
	CIPHERVM,
	EVM,
}

impl TryInto<EventType> for i8 {
	type Error = Error;

	fn try_into(self) -> Result<EventType, Self::Error> {
		match self {
			0 => Ok(EventType::CIPHERVM),
			1 => Ok(EventType::EVM),
			_ => Err(anyhow!("Invalid contract type {}", self)),
		}
	}
}
