use anyhow::{anyhow, Error as AError};

use async_trait::async_trait;
use libp2p_gossipsub::MessageId;
use primitives::{Address, IpAddress, Metadata, SignatureBytes, VerifyingKeyBytes};
use serde::{Deserialize, Serialize};
use std::error::Error;
use vrf_helper::{
	common::{get_signature_from_bytes, SecpVRF},
	secp_vrf::KeySpace,
};

#[derive(Clone, Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize, Debug, Default)]
pub struct NodeInfo {
	pub data: NodeInfoSignPayload,
	pub address: Address,
	pub signature: SignatureBytes,
	pub verifying_key: VerifyingKeyBytes,
}

#[derive(Clone, Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize, Debug, Default)]
pub struct NodeInfoSignPayload {
	pub ip_address: IpAddress,
	pub metadata: Metadata,
	pub cluster_address: Address,
}

impl NodeInfo {
	pub fn new(
		address: Address,
		ip_address: IpAddress,
		metadata: Metadata,
		cluster_address: Address,
		signature: SignatureBytes,
		verifying_key: VerifyingKeyBytes,
	) -> Self {
		NodeInfo {
			data: NodeInfoSignPayload { ip_address, metadata, cluster_address },
			address,
			signature,
			verifying_key,
		}
	}
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
		self.data
			.verify_with_ecdsa(&public_key, signature)
			.map_err(|e| anyhow!("NodeInfo: {}", e))
	}
	pub fn as_bytes(&self) -> Result<Vec<u8>, Box<dyn Error + Send>> {
		match serde_json::to_vec(self) {
			Ok(bytes) => Ok(bytes),
			Err(e) => Err(Box::new(e)),
		}
	}
}

#[async_trait]
pub trait NodeInfoBroadcast {
	async fn node_info_broadcast(
		&self,
		node_info: NodeInfo,
	) -> Result<MessageId, Box<dyn Error + Send>>;
}
