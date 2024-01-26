#[cfg(test)]
mod tests {
	use crate::{
		account::Account,
		node_info::*,
		transaction::{TXSignPayload, Transaction, TransactionType},
	};
	use primitives::*;
	use secp256k1::ecdsa::Signature;
	use serde::{Deserialize, Serialize};
	use vrf_helper::{
		common::{get_signature_from_bytes, ByteOps, SecpVRF},
		secp_vrf::KeySpace,
	};

	#[derive(Serialize, Deserialize)]
	struct SigningPayload {
		nonce: Nonce,
		fee_limit: Balance,
		transaction_type: TransactionType,
	}

	#[derive(Clone, Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize, Debug, Default)]
	struct SignNodeInfoPayload {
		pub address: Address,
		#[serde(with = "serde_bytes")]
		pub ip_address: IpAddress,
		#[serde(with = "serde_bytes")]
		pub metadata: Metadata,
	}

	#[test]
	fn test_transaction_verify_signature_valid() {
		// Create a valid transaction
		let nonce = 1;
		let recipient: Address = [2u8; 20].into();
		let amount = 100;
		let fee_limit = 10;

		let key_space = KeySpace::new();

		let payload = TXSignPayload {
			nonce,
			transaction_type: TransactionType::NativeTokenTransfer(recipient, amount),
			fee_limit,
		};

		let signature = payload.sign_with_ecdsa(key_space.secret_key).unwrap();
		let signature_bytes = signature.serialize_compact().to_vec();

		let verifying_key = key_space.public_key;

		let constructed_signature = get_signature_from_bytes(&signature_bytes).unwrap();
		assert_eq!(signature, constructed_signature);

		let transaction = Transaction::new(
			nonce,
			TransactionType::NativeTokenTransfer(recipient, amount),
			fee_limit,
			signature,
			verifying_key,
		);

		let result = transaction.verify_signature();

		assert_eq!(result.unwrap(), true);
	}

	#[test]
	fn test_transaction_verify_signature_invalid() {
		let nonce = 1;
		let recipient: Address = [2u8; 20].into();
		let amount = 100;
		let fee_limit = 10;

		let key_space = KeySpace::new();

		let payload = SigningPayload {
			nonce,
			fee_limit,
			transaction_type: TransactionType::NativeTokenTransfer(recipient, amount),
		};

		let signature = payload.sign_with_ecdsa(key_space.secret_key).unwrap();
		let verifying_key = key_space.public_key;

		let transaction = Transaction::new(
			nonce,
			// Modify the recipient address to make the signature invalid
			TransactionType::NativeTokenTransfer([3u8; 20], amount),
			fee_limit,
			signature,
			verifying_key,
		);

		// Verify the signature
		let result = transaction.verify_signature();

		// Assert that the signature verification fails
		assert_eq!(result.unwrap(), false);
	}

	#[tokio::test]
	async fn test_node_info_verify_signature_valid() {
		// Create a valid transaction
		let ip_address = "127.0.0.1".to_string();
		let account: Address = [1u8; 20].into();
		let metadata = "metadata".to_string();

		let key_space = KeySpace::new();

		let payload = NodeInfoSignPayload {
			ip_address: IpAddress::from("127.0.0.1".to_string()),
			metadata: "metadata".to_string().into_bytes(),
			cluster_address: [1u8; 20],
		};

		let signature: Signature = payload.sign_with_ecdsa(key_space.secret_key).unwrap();

		// creating a byte array out of signature
		let serialized_signature_byte = signature.serialize_compact().to_vec();
		let reconstructed_signature =
			get_signature_from_bytes(&serialized_signature_byte.clone()).unwrap();

		assert_eq!(signature, reconstructed_signature);

		let public_key = key_space.public_key;
		let timestamp = 0;
		let ip_address = "127.0.0.1".as_bytes().to_vec();
		let metadata = "metadata".as_bytes().to_vec();
		let cluster_address = [1u8; 20];
		let address = Account::address(&public_key.serialize().to_vec().clone()).unwrap();
		let node_info = NodeInfo::new(
			address,
			ip_address,
			metadata,
			cluster_address,
			signature.serialize_compact().to_vec(),
			public_key.serialize().to_vec(),
		);

		// Verify the signature
		let result = node_info.verify_signature().await;

		// Assert that the signature verification is successful
		assert_eq!(result.is_ok(), true);
	}

	#[tokio::test]
	async fn test_node_info_verify_signature_invalid() {
		// Create an invalid transaction
		let ip_address = "127.0.0.1".to_string();
		let address: Address = [1u8; 20].into();
		let metadata = "metadata".to_string();

		let key_space = KeySpace::new();
		let payload = NodeInfoSignPayload {
			ip_address: IpAddress::from(ip_address.clone()),
			metadata: metadata.clone().into_bytes(),
			cluster_address: [1u8; 20],
		};

		let signature = payload.sign_with_ecdsa(key_space.secret_key).unwrap();
		let public_key = key_space.public_key;
		let ip_address = "127.0.0.1".as_bytes().to_vec();
		let metadata = "metadata".as_bytes().to_vec();
		let cluster_address = [1u8; 20];
		let address = Account::address(&public_key.serialize().to_vec().clone()).unwrap();
		let mut node_info = NodeInfo::new(
			address,
			ip_address,
			metadata,
			cluster_address,
			signature.serialize_compact().to_vec(),
			public_key.serialize().to_vec(),
		);

		// Modify the recipient address to make the signature invalid
		node_info.data.cluster_address = [2u8; 20].into();
		let result = node_info.verify_signature().await;
		// Assert that the signature verification fails
		assert_eq!(result.is_ok(), false);
	}

	#[test]
	fn test_address() {
		let verifying_key_bytes: VerifyingKeyBytes = vec![
			2, 21, 237, 183, 233, 166, 79, 153, 112, 198, 13, 148, 184, 102, 183, 54, 134, 152, 13,
			115, 72, 116, 56, 42, 209, 0, 39, 0, 229, 216, 112, 217, 69,
		];
		let expected_address: Address = [
			117, 16, 73, 56, 186, 164, 124, 84, 168, 96, 4, 239, 153, 140, 199, 108, 46, 97, 98,
			137,
		];

		let result = Account::address(&verifying_key_bytes).unwrap();
		println!("{:?}", result);
		assert_eq!(result, expected_address);
	}
}
