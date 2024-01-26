use account::{account_manager::AccountManager, account_state::AccountState};
use anyhow::{anyhow, Error};
use db::db::DbTxConn;
use log::{error, info};
use primitives::*;
use secp256k1::{Secp256k1, SecretKey};
use std::{env::var, sync::Arc};
use system::account::Account;

pub struct ExecuteToken {}

impl ExecuteToken {
	pub async fn execute_native_token_transfer<'a>(
		from_address: &Address,
		to_address: &Address,
		amount: Balance,
		db_tx_conn: Arc<&'a DbTxConn<'a>>,
		_db_pool_conn: Arc<&'a DbTxConn<'a>>,
	) -> Result<(), Error> {
		info!("Executing NativeTokenTransfer");
		// let config = system::config::Config::default();
		// let is_local = config.is_local;

		// if !is_local {
		// 	// let system_account_address = [
		// 	// 	81, 171, 156, 72, 5, 255, 96, 4, 142, 5, 183, 83, 91, 224, 8, 27, 39, 16, 104, 253,
		// 	// ];

		// 	let node_private_key = var("NODE_PRIVKEY").unwrap_or(
		// 		"6913aeae91daf21a8381b1af75272fe6fae8ec4a21110674815c8f0691e32758".to_string(),
		// 	);
		// 	let secret_key = SecretKey::from_slice(
		// 		&hex::decode(node_private_key).expect("Error decoding node_private_key"),
		// 	)
		// 	.expect("Failed to parse provided private_key");

		// 	let secp = Secp256k1::new();
		// 	let verifying_key = secret_key.public_key(&secp);
		// 	let verifying_key_bytes = verifying_key.serialize().to_vec();
		// 	let whitelisted_system_account_address =
		// 		Account::address(&verifying_key_bytes).expect("Failed to get node address");

		// 	println!(
		// 		"System Account Whitelisted for Sending Native Coin: {:?}",
		// 		whitelisted_system_account_address
		// 	);
		// 	// println!("System Account Private Key Whitelisted for Sending Native Coin:
		// 	// {:?}",node_private_key.clone());

		// 	let system_account_address = whitelisted_system_account_address;
		// 	if from_address != &system_account_address {
		// 		let message = format!(
		// 			r#"
		// 			❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌
		// 			👮👮Insufficient privileges 👮👮
		// 			❌❌❌❌❌❌❌❌❌❌❌❌❌❌❌
		// 			"#
		// 		);
		// 		error!("{}", message);
		// 		return Err(anyhow!(message))
		// 	}
		// }

		let account_state = AccountState::new(&db_tx_conn).await?;
		let account = account_state.get_account(from_address).await?;
		let mut account_manager = AccountManager { account };
		account_manager.transfer(&to_address, &amount, &account_state).await?;
		account_state.increment_nonce(&from_address).await?;
		Ok(())
	}
}
