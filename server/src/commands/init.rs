use account::{account_manager::AccountManager, account_state::AccountState};
use db::db::Database;
use directories::UserDirs;
use genesis::genesis::{Genesis, GenesisAccount};
use vrf_helper::common::SecpVRF;

use node_info::{node_info_manager::NodeInfoManager, node_info_state::NodeInfoState};
use primitives::*;
use secp256k1::SecretKey;
use serde_json::to_string_pretty;
use staking::{staking_manager::StakingManager, staking_state::StakingState};
use std::{
	fs::{create_dir_all, File},
	io::Write,
	path::{Path, PathBuf},
};
use structopt::StructOpt;
use system::{
	account::Account,
	config::Config,
	node_info::{NodeInfo, NodeInfoSignPayload},
	validator::Validator,
};
use toml;
use validator::validator_state::ValidatorState;
#[derive(Debug, StructOpt)]
#[structopt(name = "init")]
pub struct InitCmd {
	#[structopt(long = "path", short = "w")]
	working_dir: Option<PathBuf>,
}

impl InitCmd {
	pub async fn execute(&self) {
		// Determine the working directory
		let working_dir = match &self.working_dir {
			Some(dir) => dir.clone(),
			None => {
				let user_dirs = UserDirs::new().expect("Couldn't fetch home directory");
				user_dirs.home_dir().to_path_buf()
			},
		};
		println!("Working directory: {:?}", working_dir);

		// Check if directory already exists
		if Path::new(&working_dir).exists() {
			println!("Directory already exists");
		} else {
			create_dir_all(&working_dir).expect("Couldn't create my_folder directory");
			println!("Created my_folder directory");
		}

		// Create config.json in the 'cipher' directory
		let config_path = working_dir.join("config.toml");
		let genesis_path = working_dir.join("genesis.json");

		// Create a Config struct with default values
		let mut config_data = Config::default();
		// Create default genesis data
		let genesis_data = Genesis::new(config_data.dev_mode);

		Database::re_initialize(&config_data).await;

		let genesis_node_address: Address =
			hex::decode("78e044394595d4984f66c1b19059bc14ecc24063".to_string())
				.expect("unable to decode node address")
				.try_into()
				.expect("Wrong length of Vec");
		let cluster_address: Address = hex::decode(config_data.cluster_address.clone())
			.expect("unable to decode cluster address")
			.try_into()
			.expect("Wrong length of Vec");
		let node_address: Address = hex::decode(config_data.node_address.clone())
			.expect("unable to decode node address")
			.try_into()
			.expect("Wrong length of Vec");
		let validator_pool_address =
			self.create_validators_pool(&genesis_node_address, &cluster_address).await;
		config_data.validator_pool_address = hex::encode(validator_pool_address);

		let secret_key = SecretKey::from_slice(
			&hex::decode(config_data.node_private_key.clone())
				.expect("Error decoding node_private_key"),
		)
		.expect("Error parsing into SecretKey");

		let verifying_key = hex::decode(config_data.node_verifying_key.clone())
			.expect("Error decoding node_verifying_key");

		let db_pool_conn = Database::get_pool_connection().await.expect("Unable to get db conn");
		{
			let state =
				NodeInfoState::new(&db_pool_conn).await.expect("Error creating NodeInfoState");
			state.raw_query("TRUNCATE node_info").await.unwrap_or_default();
			NodeInfoManager::store_node_info(
				node_address.clone(),
				config_data.node_ip_address.as_bytes().to_vec(),
				config_data.node_metadata.as_bytes().to_vec(),
				cluster_address.clone(),
				secret_key.clone(),
				verifying_key.clone(),
				&db_pool_conn,
			)
			.await
			.expect("Unable to store_node_info");
		}

		// Create and fund genesis accounts
		Self::create_fund_genesis_accounts(&genesis_data.data.accounts).await;

		// Create and prefund testing accounts
		if config_data.dev_mode {
			println!("\n================\nDEV MODE ENABLED\n================\n");
		} else {
			println!("\n===============\nPRODUCTION MODE\n===============\n");
		}

		// For official accounts like CIPHER Foundation, Employee Allocation, Ecosystem
		// Initiatives, etc.
		// Currently creates 0 genesis accounts. This is because the accounts already exist and will
		// come over with the mainnet beta migration Self::seed_genesis_accounts(
		// 	&genesis_data,
		// 	cluster_address.clone(),
		// 	node_address.clone(),
		// )
		// .await;

		// Set the initial block proposer
		// Currently hardcoded to 0x78e044394595d4984f66c1b19059bc14ecc24063
		genesis_data
			.set_block_proposer(cluster_address.clone(), 1, node_address, &db_pool_conn)
			.await;

		// Set the genesis node info
		Self::set_genesis_node_info(
			genesis_node_address.clone(),
			cluster_address.clone(),
			secret_key,
			verifying_key,
		)
		.await;

		// Set genesis validators
		Self::set_genesis_validators(
			&genesis_data.data.validators,
			&genesis_data.data.staking_pool_address,
		)
		.await;

		// Serialize the struct to a TOML string
		let config_str = toml::to_string(&config_data).expect("Failed to serialize config");

		// Serialize GenesisData to a pretty JSON string
		let genesis_str = to_string_pretty(&genesis_data).expect("Failed to serialize GenesisData");
		// Create and write to the config file
		let mut file = File::create(&config_path).expect("Couldn't create config file");
		file.write_all(config_str.as_bytes()).expect("Couldn't write to config file");
		println!("TOML config file has been created at {:?}", config_path);

		// Create and write to the genesis file
		let mut file = File::create(&genesis_path).expect("Couldn't create genesis file");
		file.write_all(genesis_str.as_bytes()).expect("Couldn't write to genesis file");
		println!("Genesis file has been created at {:?}", genesis_path);
	}

	async fn create_validators_pool(
		&self,
		account_address: &Address,
		cluster_address: &Address,
	) -> Address {
		let db_pool_conn = Database::get_pool_connection().await.expect("db tx conn error");
		let pool_address = Account::pool_address(account_address, cluster_address, 0);
		{
			let account_state =
				AccountState::new(&db_pool_conn).await.expect("Error creating account state");
			account_state.raw_query("TRUNCATE account").await.unwrap_or_default();
		}
		{
			let staking_state =
				StakingState::new(&db_pool_conn).await.expect("Error creating staking state");
			staking_state.raw_query("TRUNCATE staking_pool").await.unwrap_or_default();
		}
		let manager = StakingManager {};
		manager
			.create_pool(
				&pool_address,
				&account_address,
				None,
				cluster_address,
				0,
				None,
				None,
				None,
				None,
				None,
				&db_pool_conn,
			)
			.await
			.expect("Error creating validator's staking pool");
		pool_address
	}

	// async fn dev_mode_initialize(
	// 	genesis: &Genesis,
	// 	cluster_address: Address,
	// 	node_address: Address,
	// ) {
	// 	// let cipher_alice_genesis_account =
	// 	// 	[81, 171, 156, 72, 5, 255, 96, 4, 142, 5, 183, 83, 91, 224, 8, 27, 39, 16, 104, 253];

	// 	// let cipher_bob_genesis_account =
	// 	// 	[66, 38, 248, 244, 201, 157, 18, 245, 19, 255, 59, 64, 19, 82, 126, 182, 88, 67, 38, 2];

	// 	// let cipher_charlie_genesis_account = [
	// 	// 	32, 158, 175, 144, 24, 145, 169, 130, 76, 8, 152, 220, 137, 175, 27, 83, 142, 166, 201,
	// 	// 	226,
	// 	// ];

	// 	// let cipher_dave_genesis_account = [
	// 	// 	233, 32, 101, 39, 27, 47, 232, 92, 79, 99, 247, 65, 197, 140, 9, 183, 79, 119, 109, 171,
	// 	// ];

	// 	// let amount = genesis.data.genesis_amount.clone();
	// 	let db_pool_conn = Database::get_pool_connection().await.expect("db tx conn error");
	// 	//get the accounts from genesis
	// 	{
	// 		genesis
	// 			.set_block_proposer(cluster_address.clone(), 1, node_address.clone(), &db_pool_conn)
	// 			.await;
	// 	}

	// 	let account_state = AccountState::new(&db_pool_conn).await.unwrap();
	// 	for acc in genesis.accounts().into_iter() {
	// 		// println!("Address: {}", acc.address); // Note that you can only print the address here,
	// since accounts() returns 		println!("Address 0x{} funded with {} CIPHER tokens", acc.address,
	// acc.balance); 								  // Vec<String>
	// 		let address_bytes_vec = hex::decode(acc.address.clone()).expect("Decoding failed");
	// 		// Convert the byte vector to a byte array
	// 		let mut address_bytes = [0u8; 20];
	// 		address_bytes.copy_from_slice(&address_bytes_vec);
	// 		// First Address always will be cipher_dev_account:
	// 		// 75104938baa47c54a86004ef998cc76c2e616289
	// 		let mut cipher_genesis_account = AccountManager { account: Account::new(address_bytes) };
	// 		cipher_genesis_account.account.balance = acc.balance;
	// 		account_state.update_balance(&cipher_genesis_account.account).await.unwrap();
	// 	}
	// // Alice Address :- 4226f8f4c99d12f513ff3b4013527eb658432602
	// let mut cipher_alice_genesis =
	// 	AccountManager { account: Account::new(cipher_alice_genesis_account) };

	// cipher_alice_genesis.account.balance = amount;
	// account_state.update_balance(&cipher_alice_genesis.account).await.unwrap();
	// /* info!(
	// 	"CIPHER Genesis Alice_System  {:?} funded with {:?}",
	// 	hex::encode(cipher_alice_genesis_account),
	// 	amount
	// ); */
	// // Bob Address :- 209eaf901891a9824c0898dc89af1b538ea6c9e2
	// let mut cipher_bob_genesis = AccountManager { account:
	// Account::new(cipher_bob_genesis_account) };

	// cipher_bob_genesis.account.balance = amount;
	// account_state.update_balance(&cipher_bob_genesis.account).await.unwrap();
	// /* info!(
	// 	"CIPHER Genesis Bob_System {:?} funded with {:?}",
	// 	hex::encode(cipher_bob_genesis_account),
	// 	amount
	// ); */
	// let mut cipher_charlie_genesis =
	// 	AccountManager { account: Account::new(cipher_charlie_genesis_account) };
	// // Charlie Address :- 12cdb017bf6d97f276d1a9067f67dd89153feca7
	// cipher_charlie_genesis.account.balance = amount;
	// account_state.update_balance(&cipher_charlie_genesis.account).await.unwrap();
	// /* info!(
	// 	"CIPHER Genesis Charlie_System {:?} funded with {:?}",
	// 	hex::encode(cipher_charlie_genesis_account),
	// 	amount
	// ); */
	// // Dave Address :- e92065271b2fe85c4f63f741c58c09b74f776dab
	// let mut cipher_dave_genesis =
	// 	AccountManager { account: Account::new(cipher_charlie_genesis_account) };
	// cipher_dave_genesis.account.balance = amount;
	// account_state.update_balance(&cipher_dave_genesis.account).await.unwrap();
	// /* info!(
	// 	"CIPHER Genesis Dave_System  {:?} funded with {:?}",
	// 	hex::encode(cipher_dave_genesis_account),
	// 	amount
	// ); */
	// // Legacy Accounts these accounts dont have private keys
	// // TODO: remove these accounts
	// let mut alice = AccountManager { account: Account::new([0u8; 20]) };
	// alice.account.balance = 1_000;
	// account_state.update_balance(&alice.account).await.unwrap();
	// // info!("Alice account created/funded");

	// // Create and fund Alice account
	// let mut bob = AccountManager { account: Account::new([1u8; 20]) };
	// bob.account.balance = 1_234;
	// account_state.update_balance(&bob.account).await.unwrap();
	// // info!("Bob account created");

	// // Create and fund Alice account
	// let mut charlie = AccountManager { account: Account::new([2u8; 20]) };
	// charlie.account.balance = 10_025;
	// account_state.update_balance(&charlie.account).await.unwrap();
	// // info!("Charlie account created/funded");
	// }

	/// Seed genesis accounts with CIPHER tokens
	async fn seed_genesis_accounts(
		genesis: &Genesis,
		cluster_address: Address,
		node_address: Address,
	) {
		let db_pool_conn = Database::get_pool_connection().await.expect("db tx conn error");
		{
			genesis
				.set_block_proposer(cluster_address.clone(), 1, node_address.clone(), &db_pool_conn)
				.await;
		}

		// Examples
		const CIPHER_FOUNDATION_ADDRESS: [u8; 20] = [4u8; 20];
		const EMPLOYEE_ALLOCATION_ADDRESS: [u8; 20] = [5u8; 20];
		const ECOSYSTEM_INITIATIVES_ADDRESS: [u8; 20] = [6u8; 20];

		// Maybe change to come from env variable or config file in the future
		// let _seeds: Vec<(&str, Address, Balance)> = vec![
		// 	("CIPHER_FOUNDATION_ACCOUNT", CIPHER_FOUNDATION_ADDRESS, 200_000_000),
		// 	("EMPLOYEE_ALLOCATION_ACCOUNT", EMPLOYEE_ALLOCATION_ADDRESS, 100_000_000),
		// 	("ECOSYSTEM_INITIATIVES_ACCOUNT", ECOSYSTEM_INITIATIVES_ADDRESS, 150_000_000),
		// ];
		// let _account_state = AccountState::new(&db_pool_conn).await.unwrap();

		// !!!IMPORTANT!!!: Don't create genesis accounts for now. There are already 6 CIPHER
		// accounts that exist and will come over with the mainnet beta migration

		// for seed in seeds {
		// 	let account_name = seed.0;
		// 	let account_address = seed.1;
		// 	let account_balance = seed.2;

		// 	let mut cipher_genesis_account = AccountManager { account: Account::new(account_address)
		// };

		// 	cipher_genesis_account.account.balance = account_balance;
		// 	account_state.update_balance(&cipher_genesis_account.account).await.unwrap();
		// 	info!(
		//         "Genesis account created and funded | Name: {}, address: 0x{}, starting balance:
		// {} CIPHER",         account_name,
		//         hex::encode(account_address),
		//         account_balance
		//     );
		// }
	}

	/// Using to create accounts for multi-node testing
	async fn create_fund_genesis_accounts(accounts: &Vec<GenesisAccount>) {
		let db_pool_conn = Database::get_pool_connection().await.expect("db tx conn error");
		let account_state = AccountState::new(&db_pool_conn).await.unwrap();

		println!("");
		for acc in accounts.into_iter() {
			println!("Account funded -> {}", acc);

			let address_bytes_vec = hex::decode(acc.address.clone()).expect("Decoding failed");
			// Convert the byte vector to a byte array
			let mut address_bytes = [0u8; 20];
			address_bytes.copy_from_slice(&address_bytes_vec);
			// First Address always will be cipher_dev_account:
			// 75104938baa47c54a86004ef998cc76c2e616289
			let mut cipher_genesis_account =
				AccountManager { account: Account::new(address_bytes) };
			cipher_genesis_account.account.balance = acc.balance;
			account_state.update_balance(&cipher_genesis_account.account).await.unwrap();
		}
	}

	/// Set the genesis validators in the DB
	async fn set_genesis_validators(genesis_validators: &Vec<Validator>, pool_address: &Address) {
		let staking_manager = StakingManager {};
		let db_pool_conn = Database::get_pool_connection().await.expect("error db tx conn");

		for validator in genesis_validators {
			{
				staking_manager
					.stake(&validator.address, pool_address, 0, validator.stake, &db_pool_conn)
					.await
					.expect("Error staking genesis validator");
			}
			{
				let validator_state = ValidatorState::new(&db_pool_conn)
					.await
					.expect("Unable to get validator_state");
				validator_state
					.store_validator(validator)
					.await
					.expect("Error storing genesis validator");
			}
			println!(
				"Genesis validator 0x{} staked {} to pool 0x{}",
				hex::encode(validator.address),
				validator.stake,
				hex::encode(pool_address)
			);
		}
	}

	/// Store the NodeInfo for the genesis node in the DB. Nodes receive NodeInfo from other nodes
	/// by p2p broadcast, but the genesis node has no other nodes listening to it, so other nodes
	/// won't have it unless it is there from genesis.
	async fn set_genesis_node_info(
		genesis_node_address: Address,
		cluster_address: Address,
		secret_key: SecretKey,
		verifying_key: Vec<u8>,
	) {
		let ip_address = "127.0.0.1".as_bytes().to_vec();
		let metadata = "metadata".as_bytes().to_vec();

		let node_info_payload = NodeInfoSignPayload {
			ip_address: ip_address.clone(),
			metadata: metadata.clone(),
			cluster_address: cluster_address.clone(),
		};

		let signature = node_info_payload
			.sign_with_ecdsa(secret_key)
			.expect("Unable to sign node_info_payload");

		let node_info = NodeInfo::new(
			genesis_node_address,
			ip_address,
			metadata,
			cluster_address,
			signature.serialize_compact().to_vec(),
			verifying_key,
		);
		let db_pool_conn = Database::get_pool_connection().await.expect("error db tx conn");
		let node_info_state =
			NodeInfoState::new(&db_pool_conn).await.expect("Unable to get NodeInfoState");
		node_info_state
			.store_node_info(&node_info)
			.await
			.expect("Unable to store_node_info");

		println!("Genesis node info set: 0x{}", hex::encode(node_info.address));
	}
}
