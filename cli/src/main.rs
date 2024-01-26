use anyhow::{anyhow, Context};
use cipher_rpc::rpc_model::{
	node_client::NodeClient, GetAccountStateRequest, GetAccountStateResponse,
	GetBlockByNumberRequest, GetBlockByNumberResponse, GetChainStateRequest,
	GetCurrentNonceRequest, GetCurrentNonceResponse, GetEventsRequest, GetEventsResponse,
	GetStakeRequest, GetStakeResponse, GetTransactionReceiptRequest, GetTransactionReceiptResponse,
	GetTransactionsByAccountRequest, GetTransactionsByAccountResponse,
	SmartContractReadOnlyCallResponse, SubmitTransactionResponse,
};
use cli::{types::SmartContractReadOnlyFunctionCall, *};
use reqwest::Client;
use secp256k1::{Secp256k1, SecretKey};
use std::error::Error;
mod examples;
mod json;
use clap::{builder::TypedValueParser as _, Parser, Subcommand};
use json::*;
use log::{debug, info};
use pretty_env_logger;

use serde_json::json;
use std::{fmt::Display, fs::File, io::Read, process::Command as Commandline};
use system::account::Account;
use tokio_stream::StreamExt;

const DEFAULT_GRPC_HOST: &str = "http://127.0.0.1:50052";
const DEFAULT_JSON_RPC_HOST: &str = "http://127.0.0.1:50051";

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum RpcType {
	Json,
	Proto,
}

impl Display for RpcType {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			RpcType::Json => write!(f, "json"),
			RpcType::Proto => write!(f, "proto"),
		}
	}
}

/// CIPHER cli
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
	#[arg(long)]
	pub endpoint: Option<String>,
	#[arg(long)]
	pub private_key: Option<String>,
	#[arg(long, default_value_t = 100)]
	pub fee_limit: u128,
	#[arg(long, default_value_t = 1)]
	pub req_id: u64,
	#[arg(long, default_value_t = RpcType::Json,
        value_parser = clap::builder::PossibleValuesParser::new(["json", "proto"]).map(|s| if s == "json" { RpcType::Json } else { RpcType::Proto } ))]
	pub rpc_type: RpcType,
	#[command(subcommand)]
	pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
	Secp256k1Creds {
		#[arg(short, long)]
		privkey: Option<String>,
	},
	PrintAddress {
		#[arg(short, long)]
		privkey: String,
	},
	PayloadExamples,
	AccountState {
		#[arg(short, long)]
		address: Option<String>,
	},
	TransactionReceipt {
		#[arg(
			long,
			default_value = "0000000000000000000000000000000000000000000000000000000000000000"
		)]
		hash: String,
	},
	TransactionsByAccount {
		#[arg(short, long)]
		address: Option<String>,
		#[arg(short, long)]
		number_of_transactions: u32,
		#[arg(short, long)]
		starting_from: u32,
	},
	ChainState,
	CurrentNonce {
		#[arg(short, long)]
		address: Option<String>,
	},
	BlockByNumber {
		#[arg(short, long)]
		block_number: String,
	},
	GetStake {
		#[arg(short, long)]
		pool_address: String,
		#[arg(short, long)]
		account_address: String,
	},
	SubmitTxn {
		#[arg(short, long)]
		payload_file_path: String,
	},
	SubmitSol {
		#[arg(short, long)]
		payload_file_path: String,
	},
	ReadOnlyFuncCall {
		#[arg(short, long)]
		payload_file_path: String,
	},
	GetEvents {
		#[arg(
			long,
			default_value = "00000000000000000000000000000000000000000000000000000000000000000"
		)]
		tx_hash: String,
		#[arg(short, long, default_value = "0")]
		timestamp_seconds: String,
	},
	StartHardhat,
	OpenHardhatConsole,
	HardhatCompile,
	HardhatFlatten,
	DeployContractHardhat {
		#[arg(short, long)]
		network: String,
	},
	ContractHardhatTest {
		#[arg(short, long)]
		file: Option<String>,
	},
	VerifyContractHardhat {
		#[arg(short, long)]
		network: String,
		#[arg(short, long)]
		address: String,
		#[arg(short, long)]
		constructor_args: Option<String>,
	},
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
	let args = Args::parse();
	pretty_env_logger::init();

	let endpoint = match args.endpoint {
		Some(e) => e,
		None => match args.rpc_type {
			RpcType::Json => DEFAULT_JSON_RPC_HOST.to_owned(),
			RpcType::Proto => DEFAULT_GRPC_HOST.to_owned(),
		},
	};

	let json_client = Client::new().post(&endpoint);
	let grpc_client = NodeClient::connect(endpoint).await;

	match (args.rpc_type, args.command) {
		(_, Command::PayloadExamples) => println!(
			"{}",
			examples::get_payload_examples().expect("ERROR: invalid examples constructed!")
		),
		(_, Command::Secp256k1Creds { privkey }) => {
			let (privkey, pubkey, peer_id) = secp256k1_creds(privkey)?;
			println!("privkey: {privkey}\npubkey:  {pubkey}\npeer_id: {peer_id}")
		},
		(_, Command::PrintAddress { privkey }) => {
			let address = cipher_rpc::get_address_from_privkey_str(&privkey)?;
			println!("{address}");
		},
		(_, Command::StartHardhat) => {
			let current_dir = std::env::current_dir()?;
			let path = std::fs::canonicalize("cli/evm-contract")?;
			std::env::set_current_dir(&path)?;
			Commandline::new("npm").arg("install").status()?;
			Commandline::new("npx").arg("hardhat").arg("node").status()?;
			std::env::set_current_dir(&current_dir)?;
		},
		(_, Command::HardhatCompile) => {
			let current_dir = std::env::current_dir()?;
			let path = std::fs::canonicalize("cli/evm-contract")?;
			std::env::set_current_dir(&path)?;
			Commandline::new("npx").arg("hardhat").arg("compile").status()?;
			std::env::set_current_dir(&current_dir)?;
		},
		(_, Command::HardhatFlatten) => {
			let current_dir = std::env::current_dir()?;
			let path = std::fs::canonicalize("cli/evm-contract")?;
			std::env::set_current_dir(&path)?;
			Commandline::new("npx").arg("hardhat").arg("flatten").status()?;
			std::env::set_current_dir(&current_dir)?;
		},
		(_, Command::OpenHardhatConsole) => {
			let current_dir = std::env::current_dir()?;
			let path = std::fs::canonicalize("cli/evm-contract")?;
			std::env::set_current_dir(&path)?;
			Commandline::new("npx").arg("hardhat").arg("console").status()?;
			std::env::set_current_dir(&current_dir)?;
		},
		(_, Command::DeployContractHardhat { network }) => {
			let current_dir = std::env::current_dir()?;
			let path = std::fs::canonicalize("cli/evm-contract")?;
			println!("path: {:?}", path);
			std::env::set_current_dir(&path)?;
			Commandline::new("npx")
				.args(["hardhat", "run", "scripts/deploy.ts", "--network", &network])
				.status()?;
			std::env::set_current_dir(&current_dir)?;
		},
		(_, Command::ContractHardhatTest { file }) => {
			let current_dir = std::env::current_dir()?;
			let path = std::fs::canonicalize("cli/evm-contract")?;
			std::env::set_current_dir(&path)?;
			match file {
				Some(arg) => Commandline::new("npx").args(["hardhat", "test", &arg]).status()?,
				None => Commandline::new("npx").args(["hardhat", "test"]).status()?,
			};
			std::env::set_current_dir(&current_dir)?;
		},
		(_, Command::VerifyContractHardhat { network, address, constructor_args }) => {
			let current_dir = std::env::current_dir()?;
			let path = std::fs::canonicalize("cli/evm-contract")?;
			std::env::set_current_dir(&path)?;
			match constructor_args {
				Some(arg) => Commandline::new("npx")
					.args(["hardhat", "verify", "--network", &network, &address, &arg])
					.status()?,
				None => Commandline::new("npx")
					.args(["hardhat", "verify", "--network", &network])
					.status()?,
			};
			std::env::set_current_dir(&current_dir)?;
		},
		// grpc commands
		(RpcType::Proto, Command::AccountState { address }) => {
			let address = if let Some(address) = address {
				address
			} else {
				cipher_rpc::get_address_from_privkey_str(
					&args.private_key.expect("--private-key not supplied"),
				)
				.expect("Failed to create account address from privkey")
			};
			let response =
				grpc_client?.get_account_state(GetAccountStateRequest { address }).await?;
			let result = response.into_inner().account_state;
			println!("AccountState = \n{result:?}");
		},
		(RpcType::Proto, Command::TransactionReceipt { hash }) => {
			let response = grpc_client?
				.get_transaction_receipt(GetTransactionReceiptRequest { hash })
				.await?;
			let result = response.into_inner();
			println!("GetTransactionReceipt = \n{result:?}");
		},
		(
			RpcType::Proto,
			Command::TransactionsByAccount { address, number_of_transactions, starting_from },
		) => {
			let address = if let Some(address) = address {
				address
			} else {
				cipher_rpc::get_address_from_privkey_str(
					&args.private_key.expect("--private-key not supplied"),
				)
				.expect("Failed to create account address from privkey")
			};
			let response = grpc_client?
				.get_transactions_by_account(GetTransactionsByAccountRequest {
					address,
					number_of_transactions,
					starting_from,
				})
				.await?;
			let result = response.into_inner();
			println!("GetTransactionsByAccount = \n{result:?}");
		},
		(RpcType::Proto, Command::ChainState) => {
			let response = grpc_client?.get_chain_state(GetChainStateRequest {}).await?;
			let result = response.into_inner();
			println!("GetChainState = \n{result:?}",);
		},
		(RpcType::Proto, Command::BlockByNumber { block_number }) => {
			let response = grpc_client?
				.get_block_by_number(GetBlockByNumberRequest { block_number })
				.await?;
			let result = response.into_inner();
			println!("GetBlockByNumber = \n{result:?}");
		},
		(RpcType::Proto, Command::GetStake { pool_address, account_address }) => {
			let response = grpc_client?
				.get_stake(GetStakeRequest { pool_address, account_address })
				.await?;
			let result = response.into_inner();
			println!("GetStake = \n{:?}", result);
		},
		(RpcType::Proto, Command::SubmitTxn { payload_file_path }) => {
			let private_key = args.private_key.expect("--private-key not supplied");
			debug!("SubmitTxn = \n{:?}", payload_file_path);
			let account_address = cipher_rpc::get_address_from_privkey_str(&private_key).unwrap();
			let mut grpc_client = grpc_client?;
			let nonce_response = grpc_client
				.get_account_state(GetAccountStateRequest { address: account_address })
				.await?;
			let nonce: u128 = nonce_response
				.into_inner()
				.account_state
				.ok_or(anyhow!("Failed to fetch nonce"))?
				.nonce
				.parse()?;

			let response = grpc_client
				.submit_transaction(load_submit_txn_req(
					&payload_file_path,
					&private_key,
					args.fee_limit,
					nonce + 1,
				)?)
				.await?;
			let result = response.into_inner();
			println!("SubmitTxn = \n{:?}", result);
		},
		(RpcType::Proto, Command::ReadOnlyFuncCall { payload_file_path }) => {
			let mut file =
				File::open(payload_file_path).with_context(|| "Failed to open payload file")?;
			let mut file_content = String::new();
			file.read_to_string(&mut file_content)
				.with_context(|| "Failed to read the file")?;
			let read_only_request: SmartContractReadOnlyFunctionCall =
				serde_json::from_str(&file_content)
					.with_context(|| "Failed to deserialize read only payload")?;

			let read_only_request: cipher_rpc::rpc_model::SmartContractReadOnlyCallRequest =
				read_only_request.try_into()?;
			let response = grpc_client?.smart_contract_read_only_call(read_only_request).await?;
			let result = response.into_inner();
			println!("ReadOnlyFuncCall = \n{result:?}",);
		},
		(RpcType::Proto, Command::GetEvents { tx_hash, timestamp_seconds }) => {
			let request = GetEventsRequest {
				tx_hash: tx_hash.clone(),
				timestamp: timestamp_seconds.parse()?,
			};

			// Make the gRPC call
			let mut response_stream = grpc_client?.get_events(request).await?.into_inner();

			// Process the response stream
			tokio::spawn(async move {
				while let Some(result) = response_stream.next().await {
					match result {
						Ok(response) => {
							let events = response.events_data;
							for event in events {
								println!("Event: {:?}", hex::encode(event));
							}
						},
						Err(e) => eprintln!("Error processing response: {:?}", e),
					}
				}
			});
		},
		(RpcType::Proto, Command::CurrentNonce { address }) => {
			let address = if let Some(address) = address {
				address
			} else {
				cipher_rpc::get_address_from_privkey_str(
					&args.private_key.expect("--private-key not supplied"),
				)
				.expect("Failed to create account address from privkey")
			};
			let request = GetCurrentNonceRequest { address };

			let response = grpc_client?.get_current_nonce(request).await?;
			let nonce = response.into_inner().nonce;
			println!("Nonce: {nonce}");
		},
		(RpcType::Json, Command::AccountState { address }) => {
			let private_key = args.private_key.expect("--private-key not supplied");
			let address = match address {
				Some(address) => address,
				None => hex::encode(
					Account::address(
						&SecretKey::from_slice(&hex::decode(&private_key)?)?
							.public_key(&Secp256k1::new())
							.serialize()
							.to_vec(),
					)
					.unwrap(),
				),
			};
			let result = post_json_rpc(
				json_client,
				"cipher_getAccountState",
				json!({"request": GetAccountStateRequest { address } }),
			)
			.await?;
			let result = parse_response::<GetAccountStateResponse>(result)?;
			println!("AccountState = \n{result:?}");
		},
		(RpcType::Json, Command::TransactionReceipt { hash }) => {
			let response = post_json_rpc(
				json_client,
				"cipher_getTransactionReceipt",
				json!({"request": GetTransactionReceiptRequest{hash}}),
			)
			.await?;

			// println!("TransactionReceipt = \n{:?}", result);

			let result = parse_response::<GetTransactionReceiptResponse>(response.clone())
				.map_err(|e| {
					anyhow!("Failed to parse response: {:?}, response: {:?}", e, response)
				})?;
			println!("GetTransactionReceiptResponse = \n{result:?}");
		},
		(
			RpcType::Json,
			Command::TransactionsByAccount { address, number_of_transactions, starting_from },
		) => {
			let address = if let Some(address) = address {
				address
			} else {
				cipher_rpc::get_address_from_privkey_str(
					&args.private_key.expect("--private-key not supplied"),
				)
				.expect("Failed to create account address from privkey")
			};
			let response = post_json_rpc(
				json_client,
				"cipher_getTransactionsByAccount",
				json!({"request": GetTransactionsByAccountRequest{address, number_of_transactions, starting_from}}),
			)
			.await?;

			let result = parse_response::<GetTransactionsByAccountResponse>(response.clone())
				.map_err(|e| {
					anyhow!("Failed to parse response: {:?}, response: {:?}", e, response)
				})?;
			println!("GetTransactionReceiptResponse = \n{result:?}");
		},
		(RpcType::Json, Command::SubmitTxn { payload_file_path }) => {
			let private_key = args.private_key.expect("--private-key not supplied");
			let secret_key = SecretKey::from_slice(&hex::decode(&private_key)?)
				.with_context(|| "Failed to parse provided private_key")?;
			let nonce = json::get_nonce(
				json_client.try_clone().expect("Not able to clone json client"),
				&secret_key,
			)
			.await?;

			let secp = Secp256k1::new();
			let verifying_key = secret_key.public_key(&secp);
			let verifying_key_bytes = verifying_key.serialize().to_vec();
			let node_address =
				Account::address(&verifying_key_bytes).expect("Failed to get node address");

			debug!("Verifying key: {:?}", hex::encode(&verifying_key_bytes));
			debug!("NODE ADDRESS: 0x{}", hex::encode(node_address));

			let request =
				load_submit_txn_req(&payload_file_path, &private_key, args.fee_limit, nonce + 1)?;

			let request_json = serde_json::to_value(&request)
				.with_context(|| "Can serialize transaction to JSON")?;

			let response = post_json_rpc(
				json_client,
				"cipher_submitTransaction",
				json!({ "request": request_json }),
			)
			.await?;
			let result =
				parse_response::<SubmitTransactionResponse>(response.clone()).map_err(|e| {
					anyhow!("Failed to parse response: {:?}, response: {:?}", e, response)
				})?;

			println!("SubmitTxn = \n{result:?}");
		},
		(RpcType::Json, Command::SubmitSol { payload_file_path }) => {
			let private_key = args.private_key.expect("--private-key not supplied");
			let mut file = File::open(payload_file_path).context("Failed to open payload file")?;

			let mut file_content = String::new();
			file.read_to_string(&mut file_content)
				.with_context(|| "Failed to read the file")?;

			let deployment_info: EVMSmartContractDeployment =
				serde_json::from_str(&file_content).expect("Failed to parse JSON");

			let sol_file = deployment_info.smart_contract_deployment[2]["file"].as_str().unwrap();
			let value = deployment_info.smart_contract_deployment[3].as_u64().unwrap();
			let salt = &deployment_info.smart_contract_deployment[4]["text"];
			info!("deployment_info 3: {:?}", deployment_info.smart_contract_deployment[3]);
			info!("deployment_info 4: {:?}", deployment_info.smart_contract_deployment[4]["text"]);

			let mut file = File::open(sol_file).unwrap();
			let mut hex_code = String::new();
			file.read_to_string(&mut hex_code).unwrap();

			let clean_hex_string =
				if hex_code.starts_with("0x") { &hex_code[2..] } else { &hex_code };

			let txn = types::Transaction::SmartContractDeployment(
				types::AccessType::PUBLIC,
				types::ContractType::EVM,
				types::U8s::Hex(clean_hex_string.parse()?),
				value.into(),
				types::U8s::Text(salt.to_string()),
			);

			let secret_key = SecretKey::from_slice(&hex::decode(&private_key)?)
				.with_context(|| "Failed to parse provided private_key")?;
			let nonce = json::get_nonce(
				json_client.try_clone().expect("Not able to clone json client"),
				&secret_key,
			)
			.await?;

			let request = get_submit_txn_req(txn, &private_key, args.fee_limit, nonce + 1)?;

			let request_json = serde_json::to_value(&request)
				.with_context(|| "Can serialize transaction to JSON")?;

			let result = post_json_rpc(
				json_client,
				"cipher_submitTransaction",
				json!({ "request": request_json }),
			)
			.await?;
			let result = parse_response::<SubmitTransactionResponse>(result)?;

			println!("SubmitSol = \n{result:?}");
		},
		(RpcType::Json, Command::ChainState) => {
			let result = post_json_rpc(
				json_client,
				"cipher_getChainState",
				json!({"request": GetChainStateRequest{}}),
			)
			.await?
			.result
			.unwrap();

			println!("GetChainState = \n{result:?}",);
		},
		(RpcType::Json, Command::BlockByNumber { block_number }) => {
			let response = post_json_rpc(
				json_client,
				"cipher_getBlockByNumber",
				json!({"request": GetBlockByNumberRequest{ block_number}}),
			)
			.await?;

			// println!("BlockByNumber = \n{result:?}");

			let result =
				parse_response::<GetBlockByNumberResponse>(response.clone()).map_err(|e| {
					anyhow!("Failed to parse response: {:?}, response: {:?}", e, response)
				})?;
			println!("BlockByNumber = \n{result:?}",);
		},
		(RpcType::Json, Command::ReadOnlyFuncCall { payload_file_path }) => {
			let file_content = read_file(payload_file_path);

			println!("FILE CONTENT PAYLOAD: {:?}", file_content);
			let read_only_request: SmartContractReadOnlyFunctionCall =
				serde_json::from_str(&file_content)
					.with_context(|| "Failed to deserialize read only payload")?;

			let read_only_request: cipher_rpc::rpc_model::SmartContractReadOnlyCallRequest =
				read_only_request.try_into()?;

			let result = post_json_rpc(
				json_client,
				"cipher_smartContractReadOnlyCall",
				json!({ "request": read_only_request }),
			)
			.await?;

			if let Some(inner_result) = result.result {
				let data: SmartContractReadOnlyCallResponse = serde_json::from_value(inner_result)?;

				// If success
				if data.status == 0 {
					let result = String::from_utf8(data.result)?;
					println!("ReadOnlyFuncCall =\n{}", result);
				}
			} else {
				info!("ReadOnlyFuncCall =\n{:?}", result);
			}
		},
		(RpcType::Json, Command::GetStake { pool_address, account_address }) => {
			let response = post_json_rpc(
				json_client,
				"cipher_getStake",
				json!({"request": GetStakeRequest{pool_address, account_address}}),
			)
			.await?;

			let result = parse_response::<GetStakeResponse>(response.clone()).map_err(|e| {
				anyhow!("Failed to parse response: {:?}, response: {:?}", e, response)
			})?;

			println!("GetStakeResponse = {result:?}",);
		},
		(RpcType::Json, Command::GetEvents { tx_hash, timestamp_seconds }) => {
			let _timestamp: u64 = timestamp_seconds
				.parse()
				.with_context(|| "Failed to parse timestamp into u64")?;
			let timestamp = 0; //FIX ME
			let result = post_json_rpc(
				json_client,
				"cipher_getEvents",
				json!({"request": GetEventsRequest{tx_hash, timestamp}}),
			)
			.await?;

			//println!("result: {:?}", result);
			let result = parse_response::<GetEventsResponse>(result)?;
			let events: Vec<Vec<u8>> = result.events_data;

			for event in events {
				// Attempt to convert the event to a String
				if let Ok(event_str) = String::from_utf8(event.clone()) {
					println!("Event data as String:\n{}", event_str);
				} else {
					// Attempt to deserialize the event as a JSON value
					if let Ok(event_json) = serde_json::from_slice::<serde_json::Value>(&event) {
						println!("Event data as JSON:\n{:#?}", event_json);
					} else {
						// If neither String nor JSON, print as raw bytes
						println!("Event data as Raw Bytes:\n{:?}", hex::encode(event));
					}
				}
			}
		},
		(RpcType::Json, Command::CurrentNonce { address }) => {
			let address = if let Some(address) = address {
				address
			} else {
				cipher_rpc::get_address_from_privkey_str(
					&args.private_key.expect("--private-key not supplied"),
				)
				.expect("Failed to create account address from privkey")
			};

			let result = post_json_rpc(
				json_client,
				"cipher_getCurrentNonce",
				json!({"request": GetCurrentNonceRequest{address}}),
			)
			.await?;

			let result = parse_response::<GetCurrentNonceResponse>(result)?;
			println!("GetCurrentNonceResponse = \n{result:?}",);
		},
		(RpcType::Proto, command @ Command::SubmitSol { .. }) => {
			unimplemented!("Json RPC Command not implemented: {command:?}")
		},
	}

	Ok(())
}
