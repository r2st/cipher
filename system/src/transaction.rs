use crate::{access::AccessType, contract::ContractType};
use anyhow::{anyhow, Error, Result};
use async_trait::async_trait;
use jsonrpsee::tracing::warn;
use libp2p_gossipsub::MessageId;
use log::debug;
use vrf_helper::{
	common::{get_signature_from_bytes, SecpVRF},
	secp_vrf::KeySpace,
};

use primitives::*;
use secp256k1::{ecdsa::Signature, PublicKey};
use serde::{Deserialize, Serialize};
use sha3::{Digest, Keccak256};

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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TransactionTypeNativeTX {
	NativeTokenTransfer(Address, String),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TXSignPayload {
	pub nonce: Nonce,
	pub transaction_type: TransactionType,
	pub fee_limit: Balance,
}

impl Transaction {
	/// This is used to create a new Transaction
	/// # Arguments
	/// * `nonce` - Nonce
	/// * `transaction_type` - TransactionType
	/// * `fee_limit` - Balance
	/// * `signature` - Signature
	/// * `verifying_key` - PublicKey
	/// # Returns
	/// * `Transaction`
	/// # Example
	/// ```
	/// use primitives::*;
	/// use secp256k1::{PublicKey, SecretKey};
	/// use vrf_helper::common::SecpVRF;
	/// use vrf_helper::secp_vrf::KeySpace;
	/// use system::transaction::{Transaction, TransactionType};
	/// let nonce = 1;
	/// let transaction_type = TransactionType::NativeTokenTransfer(Address::from([0; 20]), 100);
	/// ```
	pub fn new(
		nonce: Nonce,
		transaction_type: TransactionType,
		fee_limit: Balance,
		signature: Signature,
		verifying_key: PublicKey,
	) -> Self {
		Transaction {
			nonce,
			transaction_type,
			fee_limit,
			signature: signature.serialize_compact().to_vec(),
			verifying_key: verifying_key.serialize().to_vec(),
		}
	}

	/// This is used to create a new Transaction from bytes
	/// # Arguments
	/// * `bytes` - Vec<u8>
	/// # Returns
	/// * `Result<Transaction, Error>`
	pub fn verify_signature_native_token_tx(&self) -> Result<bool, Error> {
		let cloned_transaction = self.clone();
		let tx_type = cloned_transaction.transaction_type.clone();
		let a: TransactionTypeNativeTX = match tx_type {
			TransactionType::NativeTokenTransfer(address, amount) =>
				TransactionTypeNativeTX::NativeTokenTransfer(address, amount.to_string()),
			_ => {
				let msg = format!("Transaction type is not native token transfer");
				log::error!("{}", msg);
				return Err(anyhow!("{}", msg))
			},
		};

		#[derive(Debug, Serialize, Deserialize)]
		pub struct TXNT {
			pub nonce: Nonce,
			pub transaction_type: TransactionTypeNativeTX,
			pub fee_limit: Balance,
		}

		let signed_payload = TXNT {
			nonce: cloned_transaction.nonce,
			transaction_type: a,
			fee_limit: cloned_transaction.fee_limit,
		};

		let signature = match get_signature_from_bytes(&cloned_transaction.signature.to_vec()) {
			Ok(sig) => {
				debug!("SERVER => Received Signature: {:?}", (&sig.serialize_compact()));
				sig
			},
			Err(err) => {
				let msg = format!("Failed to get signature from bytes {:?}", err);
				log::error!("{}", msg);
				return Err(anyhow!("{}", msg))
			},
		};

		let public_key = KeySpace::public_key_from_bytes(&self.verifying_key)?;

		debug!("SERVER => Received Signature: {:?}", hex::encode(&signature.serialize_compact()));
		debug!("SERVER => Received Public Key: {:?}", hex::encode(&public_key.serialize()));

		debug!("Server Signed Payload: {:?}", signed_payload);

		let verified = signed_payload.verify_with_ecdsa(&public_key, signature).is_ok();
		if !verified {
			warn!("Signature on native token transaction is not valid");
		}
		Ok(verified)
	}

	pub fn verify_signature(&self) -> Result<bool, Error> {
		let cloned_transaction = self.clone();

		let signed_payload = TXSignPayload {
			nonce: cloned_transaction.nonce,
			transaction_type: cloned_transaction.transaction_type,
			fee_limit: cloned_transaction.fee_limit,
		};

		let signature = match get_signature_from_bytes(&cloned_transaction.signature.to_vec()) {
			Ok(sig) => {
				debug!("SERVER => Received Signature: {:?}", (&sig.serialize_compact()));
				sig
			},
			Err(err) => {
				let msg = format!("Failed to get signature from bytes {:?}", err);
				log::error!("{}", msg);
				return Err(anyhow!("{}", msg))
			},
		};

		let public_key = KeySpace::public_key_from_bytes(&self.verifying_key)?;

		debug!("SERVER => Received Signature: {:?}", hex::encode(&signature.serialize_compact()));
		debug!("SERVER => Received Public Key: {:?}", hex::encode(&public_key.serialize()));

		debug!("Server Signed Payload: {:?}", signed_payload);

		let verified = signed_payload.verify_with_ecdsa(&public_key, signature).is_ok();
		if !verified {
			warn!("Signature on transaction is not valid");
		}
		Ok(verified)
	}

	pub fn as_bytes(&self) -> Result<Vec<u8>> {
		match serde_json::to_vec(self) {
			Ok(bytes) => Ok(bytes),
			Err(e) => Err(anyhow!("Error: {:?}", e)),
		}
	}

	pub fn transaction_hash(&self) -> Result<TransactionHash, Error> {
		let tx_bytes = self.clone().as_bytes()?;

		// Create a Keccak-256 hasher
		let mut hasher = Keccak256::new();

		// Update the hasher with the transaction bytes
		hasher.update(&tx_bytes);

		// Obtain the hash result as a fixed-size array
		let result: TransactionHash = hasher.finalize().into();

		Ok(result)
	}
}

#[async_trait]
pub trait TransactionBroadcast {
	async fn transaction_broadcast(&self, transaction: Transaction) -> Result<MessageId>;
}
