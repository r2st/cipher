pub mod json;
pub mod primitives;
pub mod transaction;
use crate::{primitives::*, transaction::TransactionType};
use anyhow::{anyhow, Result};
use k256::{elliptic_curve::sec1::ToEncodedPoint, PublicKey as K256PublicKey};
use secp256k1::{hashes::sha256, Message, Secp256k1, SecretKey};
use serde::{Deserialize, Serialize};
use sha3::{Digest, Keccak256};

pub mod rpc_model {
	tonic::include_proto!("rpc_model");
	use anyhow::anyhow;
	use std::str::FromStr;

	impl TryFrom<submit_transaction_request::TransactionType> for super::transaction::TransactionType {
		type Error = anyhow::Error;

		fn try_from(
			value: submit_transaction_request::TransactionType,
		) -> Result<Self, Self::Error> {
			let result_txn_type = match value {
				submit_transaction_request::TransactionType::NativeTokenTransfer(
					NativeTokenTransfer { address, amount },
				) => super::transaction::TransactionType::NativeTokenTransfer(
					address.try_into().map_err(|_| anyhow!("Failed to convert address bytes"))?,
					u128::from_str(&amount)
						.map_err(|_| anyhow!("Failed to convert string to u128"))?,
				),
				submit_transaction_request::TransactionType::SmartContractDeployment(
					SmartContractDeployment {
						access_type,
						contract_type,
						contract_code,
						value,
						salt,
					},
				) => super::transaction::TransactionType::SmartContractDeployment {
					access_type: access_type.try_into()?,
					contract_type: contract_type.try_into()?,
					contract_code,
					value: value.into(),
					salt,
				},
				submit_transaction_request::TransactionType::SmartContractInit(
					SmartContractInit { address, arguments },
				) => super::transaction::TransactionType::SmartContractInit(
					address.try_into().map_err(|_| anyhow!("Failed to convert address bytes"))?,
					arguments,
				),
				submit_transaction_request::TransactionType::SmartContractFunctionCall(
					SmartContractFunctionCall { contract_address, function_name, arguments },
				) => super::transaction::TransactionType::SmartContractFunctionCall {
					contract_instance_address: contract_address
						.try_into()
						.map_err(|_| anyhow!("Failed to convert contract_address bytes"))?,
					function: function_name,
					arguments,
				},
				submit_transaction_request::TransactionType::CreateStakingPool(
					CreateStakingPool {
						contract_instance_address,
						min_stake,
						max_stake,
						min_pool_balance,
						max_pool_balance,
						staking_period,
					},
				) => super::transaction::TransactionType::CreateStakingPool {
					contract_instance_address: match contract_instance_address {
						Some(x) => Some(x.try_into().map_err(|_| {
							anyhow!("Failed to convert contract_instance_address bytes")
						})?),
						None => None,
					},

					min_stake: min_stake.and_then(|s| s.parse().ok()),
					max_stake: max_stake.and_then(|s| s.parse().ok()),
					min_pool_balance: min_pool_balance.and_then(|s| s.parse().ok()),
					max_pool_balance: max_pool_balance.and_then(|s| s.parse().ok()),
					staking_period: staking_period.and_then(|s| s.parse().ok()),
				},
				submit_transaction_request::TransactionType::Stake(Stake {
					pool_address,
					amount,
				}) => super::transaction::TransactionType::Stake {
					pool_address: pool_address
						.try_into()
						.map_err(|_| anyhow!("Failed to convert pool_address bytes"))?,
					amount: u128::from_str(&amount)
						.map_err(|_| anyhow!("Failed to convert string to u128"))?,
				},
				submit_transaction_request::TransactionType::Unstake(UnStake {
					pool_address,
					amount,
				}) => super::transaction::TransactionType::UnStake {
					pool_address: pool_address
						.try_into()
						.map_err(|_| anyhow!("Failed to convert pool_address bytes"))?,
					amount: u128::from_str(&amount)
						.map_err(|_| anyhow!("Failed to convert string to u128"))?,
				},
			};
			Ok(result_txn_type)
		}
	}

	impl TryFrom<transaction::Transaction> for super::transaction::TransactionType {
		type Error = anyhow::Error;

		fn try_from(value: transaction::Transaction) -> Result<Self, Self::Error> {
			let result_txn_type = match value {
				transaction::Transaction::NativeTokenTransfer(NativeTokenTransfer {
					address,
					amount,
				}) => super::transaction::TransactionType::NativeTokenTransfer(
					address.try_into().map_err(|_| anyhow!("Failed to convert address bytes"))?,
					u128::from_str(&amount)
						.map_err(|_| anyhow!("Failed to convert string to u128"))?,
				),
				transaction::Transaction::SmartContractDeployment(SmartContractDeployment {
					access_type,
					contract_type,
					contract_code,
					value,
					salt,
				}) => super::transaction::TransactionType::SmartContractDeployment {
					access_type: access_type.try_into()?,
					contract_type: contract_type.try_into()?,
					contract_code,
					value: value.into(),
					salt,
				},
				transaction::Transaction::SmartContractInit(SmartContractInit {
					address,
					arguments,
				}) => super::transaction::TransactionType::SmartContractInit(
					address.try_into().map_err(|_| anyhow!("Failed to convert address bytes"))?,
					arguments,
				),
				transaction::Transaction::SmartContractFunctionCall(
					SmartContractFunctionCall { contract_address, function_name, arguments },
				) => super::transaction::TransactionType::SmartContractFunctionCall {
					contract_instance_address: contract_address
						.try_into()
						.map_err(|_| anyhow!("Failed to convert contract_address bytes"))?,
					function: function_name,
					arguments,
				},
				/* TODO: fix this
				transaction::Transaction::CreateStakingPool(CreateStakingPool {
					contract_instance_address,
					min_stake,
					max_stake,
					min_pool_balance,
					max_pool_balance,
					staking_period,
				}) => super::transaction::TransactionType::CreateStakingPool {
					contract_instance_address: match contract_instance_address {
						Some(x) => Some(x.try_into().map_err(|_| {
							anyhow!("Failed to convert contract_instance_address bytes")
						})?),
						None => None,
					},
					min_stake: min_stake.map(|x| x.into()),
					max_stake: max_stake.map(|x| x.into()),
					min_pool_balance: min_pool_balance.map(|x| x.into()),
					max_pool_balance: max_pool_balance.map(|x| x.into()),
					staking_period: staking_period.map(|x| x.into()),
				},
				*/
				transaction::Transaction::Stake(Stake { pool_address, amount }) =>
					super::transaction::TransactionType::Stake {
						pool_address: pool_address
							.try_into()
							.map_err(|_| anyhow!("Failed to convert pool_address bytes"))?,
						amount: u128::from_str(&amount)
							.map_err(|_| anyhow!("Failed to convert string to u128"))?,
					},
				transaction::Transaction::Unstake(UnStake { pool_address, amount }) =>
					super::transaction::TransactionType::UnStake {
						pool_address: pool_address
							.try_into()
							.map_err(|_| anyhow!("Failed to convert pool_address bytes"))?,
						amount: u128::from_str(&amount)
							.map_err(|_| anyhow!("Failed to convert string to u128"))?,
					},
			};
			Ok(result_txn_type)
		}
	}
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TXSignPayload {
	pub nonce: Nonce,
	pub transaction_type: TransactionType,
	pub fee_limit: Balance,
}

pub fn sign(
	secret_key: SecretKey,
	transaction_type: rpc_model::submit_transaction_request::TransactionType,
	fee_limit: Balance,
	nonce: Nonce,
) -> Result<Vec<u8>> {
	let transaction_type: TransactionType = transaction_type.try_into()?;
	let sign_payload = TXSignPayload { nonce, transaction_type, fee_limit };
	let json_str = serde_json::to_string(&sign_payload)?;
	let message = Message::from_hashed_data::<sha256::Hash>(json_str.as_bytes());
	let sig = secret_key.sign_ecdsa(message);
	Ok(sig.serialize_compact().to_vec())
}

pub fn get_address_from_privkey_str(private_key: &str) -> Result<String> {
	let private_key = SecretKey::from_slice(&hex::decode(private_key)?)?;
	get_address_from_private_key(&private_key)
}

pub fn get_address_from_private_key(private_key: &SecretKey) -> Result<String> {
	let verifying_key_bytes = private_key.public_key(&Secp256k1::new()).serialize().to_vec();

	let public_key = match secp256k1::PublicKey::from_slice(verifying_key_bytes.as_slice()) {
		Ok(public_key) => public_key,
		Err(err) => return Err(anyhow!("Unable to construct public key {:?}", err)),
	};

	let k_pub_bytes = K256PublicKey::from_sec1_bytes(&public_key.serialize_uncompressed()).unwrap();

	let k_pub_bytes = k_pub_bytes.to_encoded_point(false);
	let k_pub_bytes = k_pub_bytes.as_bytes();

	let hash = Keccak256::digest(&k_pub_bytes[1..]);
	let mut bytes = [0u8; 20];
	bytes.copy_from_slice(&hash[12..]);

	Ok(hex::encode(bytes))
}
