/// Concepts and implementations inspired by frontier project
/// https://github.com/polkadot-evm/frontier/blob/master/client/rpc-core/src/types/transaction.rs
use super::bytes::Bytes;
use ethereum::{AccessListItem, LegacyTransactionMessage, TransactionV2 as EthereumTransaction};
use ethereum_types::{H160, H256, U256, U64};
use jsonrpsee_types::error::ErrorObjectOwned;
use secp256k1::{Message, PublicKey, Secp256k1, SecretKey};
use serde::{Deserialize, Serialize};
use sha3::{Digest, Keccak256};
use std::{env, fmt::Debug};

#[macro_export]
macro_rules! internal_err {
	($message:expr, $data:expr) => {
		ErrorObjectOwned::owned(400, $message, $data)
	};
	($message:expr) => {
		ErrorObjectOwned::owned(400, $message, None::<()>)
	};
}

pub enum TransactionMessage {
	Legacy(LegacyTransactionMessage),
	// EIP2930(ethereum::EIP2930TransactionMessage),
	// EIP1559(ethereum::EIP1559TransactionMessage),
}

/// Transaction request coming from RPC
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct TransactionRequest {
	/// Sender
	pub from: Option<H160>,
	/// Recipient
	pub to: Option<H160>,
	/// Gas Price, legacy.
	#[serde(default)]
	pub gas_price: Option<U256>,
	/// Max BaseFeePerGas the user is willing to pay.
	#[serde(default)]
	pub max_fee_per_gas: Option<U256>,
	/// The miner's tip.
	#[serde(default)]
	pub max_priority_fee_per_gas: Option<U256>,
	/// Gas
	pub gas: Option<U256>,
	/// Value of transaction in wei
	pub value: Option<U256>,
	/// The compiled code of a contract OR the first 4 bytes of the hash of the
	/// invoked method signature and encoded parameters. For details see Ethereum Contract ABI
	#[serde(skip_serializing_if = "Option::is_none")]
	pub data: Option<Bytes>,
	/// Transaction's nonce
	pub nonce: Option<U256>,
	/// Pre-pay to warm storage access.
	#[serde(default)]
	pub access_list: Option<Vec<AccessListItem>>,
	/// EIP-2718 type
	#[serde(rename = "type")]
	pub transaction_type: Option<U256>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EvmSendTxnRequest {
	pub from: H160,
	pub to: Option<H160>,
	pub gas: Option<U256>,
	pub gas_price: Option<U256>,
	pub value: Option<U256>,
	pub data: Option<Bytes>,
	pub nonce: Option<U64>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EvmEthCallRequest {
	/// From
	pub from: Option<H160>,
	/// To
	pub to: Option<H160>,
	/// Gas Price
	pub gas_price: Option<U256>,
	/// Gas
	pub gas: Option<U256>,
	/// Value
	pub value: Option<U256>,
	/// Data
	pub data: Option<Bytes>,
	/// Nonce
	pub nonce: Option<U256>,
}
impl From<EvmEthCallRequest> for TransactionMessage {
	fn from(req: EvmEthCallRequest) -> Self {
		TransactionMessage::Legacy(LegacyTransactionMessage {
			nonce: U256::zero(),
			gas_price: req.gas_price.unwrap_or_default(),
			gas_limit: req.gas.unwrap_or_default(),
			value: req.value.unwrap_or_default(),
			input: req.data.map(|s| s.into_vec()).unwrap_or_default(),
			action: match req.to {
				Some(to) => ethereum::TransactionAction::Call(to),
				None => ethereum::TransactionAction::Create,
			},
			chain_id: None,
		})
	}
}
impl From<TransactionRequest> for TransactionMessage {
	fn from(req: TransactionRequest) -> Self {
		TransactionMessage::Legacy(LegacyTransactionMessage {
			nonce: U256::zero(),
			gas_price: req.gas_price.unwrap_or_default(),
			gas_limit: req.gas.unwrap_or_default(),
			value: req.value.unwrap_or_default(),
			input: req.data.map(|s| s.into_vec()).unwrap_or_default(),
			action: match req.to {
				Some(to) => ethereum::TransactionAction::Call(to),
				None => ethereum::TransactionAction::Create,
			},
			chain_id: None,
		})
	}
}
// fn public_key_address(public: &libsecp256k1::PublicKey) -> H160 {
// 	let mut res = [0u8; 64];
// 	res.copy_from_slice(&public.serialize()[1..65]);
// 	H160::from(H256::from_slice(Keccak256::digest(&res).as_slice()))
// }

// fn secret_key_address(secret: &libsecp256k1::SecretKey) -> H160 {
// 	let public = libsecp256k1::PublicKey::from_secret_key(&secret);
// 	public_key_address(&public)
// }

// fn keys(sec_keys: Vec<&str>) -> Result<Vec<libsecp256k1::SecretKey>, ErrorObjectOwned> {
// 	let mut keys = Vec::new();
// 	for key in sec_keys {
// 		// Convert the private key string to bytes
// 		let private_key_bytes = match hex::decode(key).map_err(|_| "Invalid private key") {
// 			Ok(result) => result,
// 			Err(e) => {
// 				log::error!("Failed to create bytes from private key {}", e);
// 				return Err(internal_err!("Failed to convert bytes to private key"))
// 			},
// 		};
// 		let secret_key = match libsecp256k1::SecretKey::parse_slice(&private_key_bytes)
// 			.map_err(|_| "Invalid private key length")
// 		{
// 			Ok(sk) => sk,
// 			Err(e) => {
// 				log::error!("Failed to create secret key: {}", e);
// 				return Err(internal_err!("Failed to create secret key"))
// 			},
// 		};

// 		keys.push(secret_key);
// 	}

// 	Ok(keys)
// }

// pub fn sign(
// 	message: &TransactionMessage,
// 	address: &H160,
// ) -> Result<EthereumTransaction, ErrorObjectOwned> {
// 	let mut transaction = None;
// 	let secret_keys = vec![
// 		"f6b82b53ecbe1978b8651f740739b1d181f0285381e65e5e3491d8e821ab9bd0",
// 		"bf7b645cad4c527189fe9bf59a8db74f28dd6f927637b19e4fe0fe60b1afc72f",
// 		"d545e67bfab13d1ae4e2e8db9b65f7288dd57802d1c1377f2d4dc3959f63a72b",
// 	];
// 	let keys = keys(secret_keys)?;
// 	for secret in &keys {
// 		let key_address = secret_key_address(secret);
// 		if &key_address == address {
// 			match message {
// 				TransactionMessage::Legacy(m) => {
// 					let signing_message = libsecp256k1::Message::parse_slice(&m.hash()[..])
// 						.map_err(|_| internal_err!("invalid signing message"))?;
// 					let (signature, recid) = libsecp256k1::sign(&signing_message, secret);
// 					let v = match m.chain_id {
// 						None => 27 + recid.serialize() as u64,
// 						Some(chain_id) => 2 * chain_id + 35 + recid.serialize() as u64,
// 					};
// 					let rs = signature.serialize();
// 					let r = H256::from_slice(&rs[0..32]);
// 					let s = H256::from_slice(&rs[32..64]);
// 					transaction = Some(EthereumTransaction::Legacy(ethereum::LegacyTransaction {
// 						nonce: m.nonce,
// 						gas_price: m.gas_price,
// 						gas_limit: m.gas_limit,
// 						action: m.action,
// 						value: m.value,
// 						input: m.input.to_owned(),
// 						signature: ethereum::TransactionSignature::new(v, r, s)
// 							.ok_or_else(|| internal_err!("signer generated invalid signature"))?,
// 					}));
// 				},
// 			}
// 			break;
// 		}
// 	}
// 	transaction.ok_or_else(|| internal_err!("signer not available"))
// }

pub trait EthSigner: Send + Sync {
	/// Available accounts from this signer.
	fn accounts(&self) -> Vec<H160>;
	/// Sign a transaction message using the given account in message.
	fn sign(
		&self,
		message: TransactionMessage,
		address: &H160,
	) -> Result<EthereumTransaction, ErrorObjectOwned>;

	// Method to clone the trait object
	fn clone_box(&self) -> Box<dyn EthSigner>;

	// Method to create a debug string
	fn debug_str(&self) -> String;

	// Method to get the secret key for a given address
	fn secret_key(&self, address: &H160) -> Option<&SecretKey>;
}

// Implementing Clone for a Box of a trait object
impl Clone for Box<dyn EthSigner> {
	fn clone(&self) -> Box<dyn EthSigner> {
		self.clone_box()
	}
}

#[derive(Clone, Debug)]
pub struct EthDevSigner {
	keys: Vec<SecretKey>,
}

impl EthDevSigner {
	pub fn new() -> Self {
		Self {
			keys: vec![SecretKey::from_slice(&[
				0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11,
				0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11,
				0x11, 0x11, 0x11, 0x11,
			])
			.expect("Test key is valid; qed")],
		}
	}

	// Method to safely retrieve a reference to a secret key
	pub fn secret_key_ref(&self, key: &SecretKey) -> Option<&SecretKey> {
		self.keys.iter().find(|&k| k == key)
	}
}

fn secret_key_address(secret: &SecretKey) -> H160 {
	let secp = Secp256k1::new();
	let public_key = PublicKey::from_secret_key(&secp, secret);
	public_key_address(&public_key)
}

fn public_key_address(public_key: &PublicKey) -> H160 {
	let public_key_bytes = public_key.serialize_uncompressed();
	let hash = Keccak256::digest(&public_key_bytes[1..]);
	H160::from_slice(&hash[12..])
}

pub fn initialize_eth_signer() -> Option<Vec<Box<dyn EthSigner>>> {
	dotenvy::from_filename("signers.env").ok(); // Load environment variables from signers.env file

	let keys_str = match env::var("SIGNER_KEYS") {
		Ok(val) => val,
		Err(_) => {
			log::error!("SIGNER_KEYS not found in .env file");
			return None;
		},
	};

	let signers: Vec<Box<dyn EthSigner>> = keys_str
		.split(',')
		.map(|s| s.trim())
		.filter_map(|key_str| {
			hex::decode(key_str) // Decode the hex string to bytes
				.ok()
				.and_then(|bytes| SecretKey::from_slice(&bytes).ok())
				.map(|key| {
					let eth_dev_signer = EthDevSigner { keys: vec![key] }; // Initialize EthDevSigner with a single key
					Box::new(eth_dev_signer) as Box<dyn EthSigner>
				})
		})
		.collect();

	if signers.is_empty() {
		log::warn!("No valid keys found in SIGNER_KEYS");
		None
	} else {
		Some(signers)
	}
}

impl EthSigner for EthDevSigner {
	fn accounts(&self) -> Vec<H160> {
		self.keys.iter().map(secret_key_address).collect()
	}

	fn sign(
		&self,
		message: TransactionMessage,
		address: &H160,
	) -> Result<EthereumTransaction, ErrorObjectOwned> {
		let mut transaction = None;
		let secp = Secp256k1::new();

		for secret in &self.keys {
			let key_address = secret_key_address(secret);

			if &key_address == address {
				let TransactionMessage::Legacy(ref m) = message;
				let signing_message = Message::from_slice(&m.hash()[..])
					.map_err(|_| internal_err!("invalid signing message"))?;
				let recoverable_sig = secp.sign_ecdsa_recoverable(&signing_message, secret);
				let (rec_id, sig) = recoverable_sig.serialize_compact();

				let r = H256::from_slice(&sig[0..32]);
				let s = H256::from_slice(&sig[32..64]);

				// Compute v
				let v = {
					let recovery_id = rec_id.to_i32();
					let standard_v = if recovery_id % 2 == 0 { 27 } else { 28 };
					m.chain_id.map_or(standard_v, |chain_id| standard_v + 2 * chain_id + 8)
				};

				transaction = Some(EthereumTransaction::Legacy(ethereum::LegacyTransaction {
					nonce: m.nonce,
					gas_price: m.gas_price,
					gas_limit: m.gas_limit,
					action: m.action,
					value: m.value,
					input: m.clone().input,
					signature: ethereum::TransactionSignature::new(v, r, s)
						.ok_or_else(|| internal_err!("signer generated invalid signature"))?,
				}));
			}
		}

		transaction.ok_or_else(|| internal_err!("signer not available"))
	}

	fn clone_box(&self) -> Box<dyn EthSigner> {
		Box::new(self.clone())
	}

	fn debug_str(&self) -> String {
		format!("{:?}", self)
	}

	fn secret_key(&self, address: &H160) -> Option<&SecretKey> {
		self.keys.iter().find(|key| {
			let key_address = secret_key_address(key);
			&key_address == address
		})
	}
}
