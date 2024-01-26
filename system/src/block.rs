use crate::{
	block_header::BlockHeader, transaction::Transaction,
	transaction_receipt::TransactionReceiptResponse,
};
use anyhow::{anyhow, Error as AError};
use async_trait::async_trait;

use libp2p_gossipsub::MessageId;
use primitives::*;
use serde::{Deserialize, Serialize};
use std::{error::Error, fmt};
use vrf_helper::{
	common::{get_signature_from_bytes, SecpVRF},
	secp_vrf::KeySpace,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BlockPayload {
	pub block: Block,
	pub signature: SignatureBytes,
	pub verifying_key: VerifyingKeyBytes,
}

impl BlockPayload {
	pub async fn verify_signature(&self) -> Result<(), AError> {
		let signature_bytes: [u8; 64] = match self.signature.clone().try_into() {
			Ok(s) => s,
			Err(_) => return Err(anyhow!("Unable to get signature_bytes")),
		};
		let verifying_bytes: [u8; 33] = match self.verifying_key.clone().try_into() {
			Ok(v) => v,
			Err(_) => return Err(anyhow!("Unable to get verifying_bytes")),
		};

		let signature = get_signature_from_bytes(&signature_bytes)?;

		let public_key = KeySpace::public_key_from_bytes(&verifying_bytes)?;
		self.block
			.verify_with_ecdsa(&public_key, signature)
			.map_err(|e| anyhow!("BlockPayload: {}", e))
	}

	pub fn as_bytes(&self) -> Result<Vec<u8>, Box<dyn Error + Send>> {
		match serde_json::to_vec(self) {
			Ok(bytes) => Ok(bytes),
			Err(e) => Err(Box::new(e)),
		}
	}
}

impl fmt::Display for BlockPayload {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(
			f,
			"BlockPayload {{ \n\tblock: {}, \n\tsignature: 0x{}, \n\tverifying_key 0x{} }}",
			self.block,
			hex::encode(&self.signature),
			hex::encode(&self.verifying_key)
		)
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BlockSignPayload {
	pub block_number: BlockNumber,
	pub parent_hash: BlockHash,
	pub block_type: BlockType,
	pub cluster_address: Address,
	pub transactions: Vec<Transaction>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Block {
	pub block_header: BlockHeader,
	pub transactions: Vec<Transaction>,
}

impl Block {
	pub fn new(block_header: BlockHeader, transactions: Vec<Transaction>) -> Block {
		Block { block_header, transactions }
	}

	pub fn as_bytes(&self) -> Result<Vec<u8>, Box<dyn Error + Send>> {
		match serde_json::to_vec(self) {
			Ok(bytes) => Ok(bytes),
			Err(e) => Err(Box::new(e)),
		}
	}
}

impl fmt::Display for Block {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(
			f,
			"Block {{ block_header: {}, transactions: {:?} }}",
			self.block_header, self.transactions
		)
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum BlockType {
	CIPHERTokenBlock,
	CIPHERContractBlock,
	XTalkTokenBlock,
	XTalkContractBlock,
	SuperBlock,
}

#[async_trait]
pub trait BlockBroadcast {
	async fn block_broadcast(
		&self,
		block_payload: BlockPayload,
	) -> Result<MessageId, Box<dyn Error + Send>>;
	async fn block_validate_broadcast(
		&self,
		block_payload: BlockPayload,
	) -> Result<MessageId, Box<dyn Error + Send>>;
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BlockResponse {
	pub block_header: BlockHeader,
	pub transactions: Vec<TransactionReceiptResponse>,
}
