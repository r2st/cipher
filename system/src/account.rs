use anyhow::{anyhow, Error as AError};
use ethers::utils::keccak256;
use k256::{elliptic_curve::sec1::ToEncodedPoint, PublicKey as K256PublicKey};
use primitives::*;
use serde::{Deserialize, Serialize};
use sha3::{Digest, Keccak256};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Account {
	pub address: Address,
	pub balance: Balance,
	pub nonce: Nonce,
	pub account_type: AccountType,
}

unsafe impl Send for Account {}
unsafe impl Sync for Account {}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AccountType {
	System,
	User,
}

impl AccountType {
	pub fn as_str(&self) -> &'static str {
		match *self {
			AccountType::System => "System",
			AccountType::User => "User",
		}
	}
}

// impl FromSql<Accounttype, Pg> for AccountType {
// 	fn from_sql(bytes: PgValue) -> deserialize::Result<Self> {
// 		match bytes.as_bytes() {
// 			b"System" => Ok(AccountType::System),
// 			b"User" => Ok(AccountType::User),
// 			_ => Err("Unrecognized enum variant".into()),
// 		}
// 	}
// }

impl Account {
	pub fn new(address: Address) -> Account {
		Account { address, balance: 0, nonce: 0, account_type: AccountType::User }
	}

	pub fn new_system(address: Address) -> Account {
		Account { address, balance: 0, nonce: 0, account_type: AccountType::System }
	}

	pub fn address(verifying_key_bytes: &VerifyingKeyBytes) -> Result<Address, AError> {
		let public_key = match secp256k1::PublicKey::from_slice(verifying_key_bytes.as_slice()) {
			Ok(public_key) => public_key,
			Err(err) => return Err(anyhow!("Unable to construct public key {:?}", err)),
		};

		let k_pub_bytes =
			K256PublicKey::from_sec1_bytes(&public_key.serialize_uncompressed()).unwrap();

		let k_pub_bytes = k_pub_bytes.to_encoded_point(false);
		let k_pub_bytes = k_pub_bytes.as_bytes();

		let hash = keccak256(&k_pub_bytes[1..]);
		let mut bytes = [0u8; 20];
		bytes.copy_from_slice(&hash[12..]);
		Ok(bytes)
	}

	pub fn contract_address(
		account_address: &Address,
		cluster_address: &Address,
		nonce: Nonce,
	) -> Address {
		let mut input: Vec<u8> = Vec::new();
		//input.extend_from_slice(contract_code);
		input.extend_from_slice(account_address);
		input.extend_from_slice(cluster_address);
		input.extend_from_slice(&nonce.to_be_bytes());

		let hash = Keccak256::digest(&input);
		let mut address = [0u8; 20];
		address.copy_from_slice(&hash[12..]);
		address
	}

	pub fn contract_instance_address(
		account_address: &Address,
		contract_address: &Address,
		cluster_address: &Address,
		nonce: Nonce,
	) -> Address {
		let mut input: Vec<u8> = Vec::new();
		input.extend_from_slice(account_address);
		input.extend_from_slice(contract_address);
		input.extend_from_slice(cluster_address);
		input.extend_from_slice(&nonce.to_be_bytes());

		let hash = Keccak256::digest(&input);
		let mut address = [0u8; 20];
		address.copy_from_slice(&hash[12..]);
		address
	}

	pub fn pool_address(
		account_address: &Address,
		cluster_address: &Address,
		nonce: Nonce,
	) -> Address {
		let mut input: Vec<u8> = Vec::new();
		input.extend_from_slice(account_address);
		input.extend_from_slice(cluster_address);
		input.extend_from_slice(&nonce.to_be_bytes());

		let hash = Keccak256::digest(&input);
		let mut address = [0u8; 20];
		address.copy_from_slice(&hash[12..]);
		address
	}
}
