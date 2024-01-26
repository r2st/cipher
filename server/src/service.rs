use account::account_state::AccountState as SysAccountState;
use block::block_state::BlockState;
use cipher_rpc::rpc_model::{
	self, submit_transaction_request::TransactionType as gRPCTransactionType, AccountState, Block,
	GetAccountStateRequest, GetAccountStateResponse, GetBlockByNumberRequest,
	GetBlockByNumberResponse, GetChainStateRequest, GetChainStateResponse, GetCurrentNonceRequest,
	GetCurrentNonceResponse, GetEventsRequest, GetEventsResponse, GetLatestTransactionsRequest,
	GetStakeRequest, GetStakeResponse, GetTransactionReceiptRequest, GetTransactionReceiptResponse,
	GetTransactionsByAccountRequest, GetTransactionsByAccountResponse,
	SmartContractReadOnlyCallRequest, SmartContractReadOnlyCallResponse, SubmitTransactionRequest,
	SubmitTransactionResponse, TransactionStatus,
};
use contract::contract_state::ContractState;
use contract_instance::contract_instance_state::ContractInstanceState;
use db::db::Database;
use ethers::types::{Log, H256};
use event::event_state::EventState;
use evm_runtime::Capture;
use execute::{execute_common::ContractExecutor, execute_contract_evm};
use itertools::Itertools;
use log::{debug, error};
use node_crate::node::FullNode;
use primitives::*;
use secp256k1::PublicKey;
use staking::staking_state::StakingState;
use std::{
	str::FromStr,
	sync::Arc,
	time::{SystemTime, UNIX_EPOCH},
	vec,
};
use system::{
	access::AccessType,
	account::{Account, AccountType},
	contract::ContractType,
	errors::NodeError,
	mempool::ProcessMempool,
	transaction::{Transaction as SystemTransaction, TransactionType as SystemTransactionType},
	transaction_response::{TransactionResponse, TransactionResponseType},
};
use tokio::sync::broadcast;
use tonic::Status;
use types::eth::signers::EthSigner;
use util::{convert::to_proto_transaction, generic::whitelist_check};
use vrf_helper::common::get_signature_from_bytes;

pub type SubmitTransactionStream =
	tokio_stream::wrappers::ReceiverStream<Result<SubmitTransactionResponse, Status>>;
pub type GetEventsStream =
	tokio_stream::wrappers::ReceiverStream<Result<GetEventsResponse, Status>>;

#[derive(Clone)]
pub struct FullNodeService {
	pub whitelist_check: bool,
	pub node: FullNode,
	pub signers: Option<Vec<Box<dyn EthSigner>>>,
}

impl FullNodeService {
	// Method to set an EthSigner
	pub fn set_eth_signer(&mut self, signer: Vec<Box<dyn EthSigner>>) {
		self.signers = Some(signer);
	}

	pub async fn get_account_state(
		&self,
		req: GetAccountStateRequest,
	) -> Result<GetAccountStateResponse, NodeError> {
		let db_pool_conn = Database::get_pool_connection().await.map_err(|e| {
			NodeError::EventFetchError(format!("Failed to get db_pool_conn: {}", e))
		})?;
		let account_state = SysAccountState::new(&db_pool_conn)
			.await
			.map_err(|e| NodeError::DBError(format!("Failed to access database: {}", e)))?;
		let address = address_string_to_bytes(&req.address).await?;

		let account = account_state
			.get_account(&address)
			.await
			.map_err(|_| NodeError::AccountFetchError(format!("{address:?}")))?;

		let account_type = match account.account_type {
			AccountType::System => 0,
			AccountType::User => 1,
		};

		let acc_state = AccountState {
			balance: account.balance.to_string(),
			nonce: account.nonce.to_string(),
			account_type,
		};
		let response = GetAccountStateResponse { account_state: Some(acc_state) };

		Ok(response)
	}

	/// Submit a transaction to the chain
	pub async fn submit_transaction(
		&self,
		req: SubmitTransactionRequest,
	) -> Result<SubmitTransactionStream, NodeError> {
		let (tx_channel, rx) = tokio::sync::mpsc::channel(4);
		let service = self.clone();
		tokio::spawn(async move {
			let db_pool_conn = Database::get_pool_connection().await.map_err(|e| {
				NodeError::EventFetchError(format!("Failed to get db_pool_conn: {}", e))
			})?;
			debug!("SIGNATURE: {:?}", req.signature);
			let signature = get_signature_from_bytes(&req.signature)
				.map_err(|e| NodeError::DBError(format!("Failed to parse signature: {}", e)))?;

			let verifying_key = match PublicKey::from_slice(&req.verifying_key) {
				Ok(key) => key,

				Err(e) =>
					return Err(NodeError::ParseError(format!(
						"Failed to parse verifying_key: {}",
						e
					))),
			};
			let from_address: Address = Account::address(&verifying_key.serialize().to_vec())
				.map_err(|_| {
					NodeError::InvalidAddress("Failed to convert bytes to Address".to_string())
				})?;

			let mut contract_code = vec![];
			let nonce: Nonce = req
				.nonce
				.parse()
				.map_err(|_| NodeError::ParseError("Failed to parse nonce".to_string()))?;
			let fee_limit: Balance = req
				.fee_limit
				.parse()
				.map_err(|_| NodeError::ParseError("Failed to parse fee_limit".to_string()))?;

			let transaction_type = req
				.transaction_type
				.ok_or(NodeError::InvalidTransactionType(format!("Must have a TransactionType")))?;

			let tx_type: SystemTransactionType = match transaction_type {
				gRPCTransactionType::NativeTokenTransfer(tx) => {
					let to_address: Address = tx.address.clone().try_into().map_err(|_| {
						NodeError::InvalidAddress("Failed to convert bytes to Address".to_string())
					})?;

					let whitelist_transfer_check = service.whitelist_check;

					// // Only whitelist check in production
					if whitelist_transfer_check {
						if let Err(e) = whitelist_check(&from_address, &to_address).await {
							let _ = tx_channel
								.send(Err(Status::from(NodeError::InvalidAddress(format!(
									"Account failed whitelist check: {}",
									e
								)))))
								.await;
							return Ok(());
						}
					}

					let account_state = SysAccountState::new(&db_pool_conn).await.map_err(|e| {
						NodeError::DBError(format!("Failed to access database: {}", e))
					})?;
					let balance = account_state.get_balance(&from_address).await.map_err(|e| {
						NodeError::AccountFetchError(format!(
							"Failed to fetch balance for account 0x{}: {}",
							hex::encode(from_address),
							e
						))
					})?;

					let tx_amount = u128::from_str(&tx.amount)
						.map_err(|_| NodeError::ParseError("Failed to parse amount".to_string()))?;
					if balance < tx_amount {
						return Err(NodeError::InsufficientBalance(format!(
							"Insufficient funds for account 0x{}, balance: {}",
							hex::encode(from_address),
							balance
						)))
					}

					let address: Address = tx.address.try_into().map_err(|_| {
						NodeError::InvalidAddress("Failed to convert bytes to Address".to_string())
					})?;
					let amount = u128::from_str(&tx.amount)
						.map_err(|_| NodeError::ParseError("Failed to parse amount".to_string()))?;
					print!("====================");
					println!("amount: {}", amount);
					print!("====================");
					SystemTransactionType::NativeTokenTransfer(address, amount as u128)
				},
				gRPCTransactionType::SmartContractDeployment(payload) => {
					let access_type = match payload.access_type {
						x if x == AccessType::PRIVATE as i32 => AccessType::PRIVATE,
						x if x == AccessType::PUBLIC as i32 => AccessType::PUBLIC,
						x if x == AccessType::RESTICTED as i32 => AccessType::RESTICTED,
						x =>
							return Err(NodeError::InvalidAccessType(format!(
								"failed to convert AccessType {}",
								x
							))),
					};
					let contract_type = match payload.contract_type {
						x if x == ContractType::EVM as i32 => ContractType::EVM,
						x =>
							return Err(NodeError::InvalidContractType(format!(
								"failed to convert ContractType {}",
								x
							))),
					};
					contract_code = payload.contract_code.clone();
					SystemTransactionType::SmartContractDeployment {
						access_type,
						contract_type,
						contract_code: payload.contract_code,
						value: payload.value as u128,
						salt: payload.salt,
					}
				},
				gRPCTransactionType::SmartContractInit(payload) => {
					let address = payload.address.try_into().map_err(|_| {
						NodeError::InvalidAddress("Failed to convert bytes to Address".to_string())
					})?;
					SystemTransactionType::SmartContractInit(address, payload.arguments)
				},
				gRPCTransactionType::SmartContractFunctionCall(payload) => {
					let contract_instance_address =
						payload.contract_address.try_into().map_err(|_| {
							NodeError::InvalidAddress(
								"Failed to convert bytes to Address".to_string(),
							)
						})?;
					let function: Vec<u8> = payload.function_name;
					let arguments: Vec<u8> = payload.arguments;
					SystemTransactionType::SmartContractFunctionCall {
						contract_instance_address,
						function,
						arguments,
					}
				},
				gRPCTransactionType::CreateStakingPool(payload) => {
					let contract_instance_address: Option<Address> = payload
						.contract_instance_address
						.map(|address_bytes| {
							address_bytes.try_into().map_err(|_| {
								NodeError::InvalidAddress(
									"Failed to convert bytes to Address".to_string(),
								)
							})
						})
						.transpose()?;

					// TODO: handle unwraps
					SystemTransactionType::CreateStakingPool {
						contract_instance_address,
						min_stake: payload
							.min_stake
							.as_ref()
							.map(|s| u128::from_str(s))
							.transpose() // Convert Option<Result<_, _>> to Result<Option<_>, _>
							.map_err(|_| {
								NodeError::ParseError("Failed to parse min_stake".to_string())
							})?,
						max_stake: payload
							.max_stake
							.as_ref()
							.map(|s| u128::from_str(s))
							.transpose() // Convert Option<Result<_, _>> to Result<Option<_>, _>
							.map_err(|_| {
								NodeError::ParseError("Failed to parse max_stake".to_string())
							})?,
						min_pool_balance: payload
							.min_pool_balance
							.as_ref()
							.map(|s| u128::from_str(s))
							.transpose() // Convert Option<Result<_, _>> to Result<Option<_>, _>
							.map_err(|_| {
								NodeError::ParseError(
									"Failed to parse min_pool_balance".to_string(),
								)
							})?,
						max_pool_balance: payload
							.max_pool_balance
							.as_ref()
							.map(|s| u128::from_str(s))
							.transpose() // Convert Option<Result<_, _>> to Result<Option<_>, _>
							.map_err(|_| {
								NodeError::ParseError(
									"Failed to parse max_pool_balance".to_string(),
								)
							})?,
						staking_period: payload
							.staking_period
							.as_ref()
							.map(|s| u128::from_str(s))
							.transpose() // Convert Option<Result<_, _>> to Result<Option<_>, _>
							.map_err(|_| {
								NodeError::ParseError("Failed to parse staking_period".to_string())
							})?,
					}
				},
				gRPCTransactionType::Stake(payload) => {
					// let pool_address = payload.pool_address.try_into().map_err(|_| {
					// 	NodeError::InvalidAddress("Failed to convert bytes to Address".to_string())
					// })?;
					//
					// SystemTransactionType::Stake { pool_address, amount: payload.amount as u128 }
					let pool_address = payload.pool_address.try_into().map_err(|_| {
						NodeError::InvalidAddress("Failed to convert bytes to Address".to_string())
					})?;
					let amount = u128::from_str(&payload.amount).map_err(|_| {
						NodeError::ParseError("Failed to parse stake amount".to_string())
					})?;

					SystemTransactionType::Stake { pool_address, amount }
				},
				gRPCTransactionType::Unstake(payload) => {
					let pool_address = payload.pool_address.try_into().map_err(|_| {
						NodeError::InvalidAddress("Failed to convert bytes to Address".to_string())
					})?;
					let amount = u128::from_str(&payload.amount).map_err(|_| {
						NodeError::ParseError("Failed to parse unstake amount".to_string())
					})?;

					SystemTransactionType::UnStake { pool_address, amount }
					// let pool_address = payload.pool_address.try_into().map_err(|_| {
					// 	NodeError::InvalidAddress("Failed to convert bytes to Address".to_string())
					// })?;
					//
					// SystemTransactionType::UnStake { pool_address, amount: payload.amount as u128
					// }
				},
			};

			// Form transaction into expected format
			let transaction =
				SystemTransaction::new(nonce, tx_type, fee_limit, signature, verifying_key);
			// Submit transaction to the mempool
			match service
				.node
				.mempool_tx
				.send(ProcessMempool::AddTransaction(transaction.clone()))
				.await
			{
				Ok(_) => { /* If the send is successful, do nothing and proceed */ },
				Err(e) => {
					log::warn!("Unable to write transaction to mempool channel: {:?}", e);
					return Err(NodeError::UnexpectedError(format!(
						"Something went wrong: {:?}",
						e
					)));
				},
			}
			match TransactionResponse::new(
				transaction.clone(),
				&service.node.cluster_address,
				&contract_code,
				transaction.nonce,
				from_address,
			) {
				Ok(tx_response) => {
					let hash = hex::encode(tx_response.tx_hash);
					let contract_address = match tx_response.data {
						Some(tx_type) => match tx_type {
							TransactionResponseType::SmartContractDeployment(addr) =>
								Some(hex::encode(addr)),
							TransactionResponseType::SmartContractInit(addr) =>
								Some(hex::encode(addr)),
							TransactionResponseType::XtalkSmartContractDeployment(addr) =>
								Some(hex::encode(addr)),
							TransactionResponseType::XtalkSmartContractInit(addr) =>
								Some(hex::encode(addr)),
							TransactionResponseType::CreateStakingPool(addr) =>
								Some(hex::encode(addr)),
						},
						None => None,
					};
					let response = SubmitTransactionResponse { hash, contract_address };
					if tx_channel.send(Ok(response)).await.is_err() {
						return Err(NodeError::UnexpectedError(format!(
							"Event receiver has dropped"
						)));
					} else {
						Ok(())
					}
				},
				Err(e) => {
					return Err(NodeError::UnexpectedError(format!(
						"Something went wrong: {:?}",
						e
					)));
				},
			}
		});
		Ok(tokio_stream::wrappers::ReceiverStream::new(rx))
	}

	pub async fn get_transaction_receipt(
		&self,
		req: GetTransactionReceiptRequest,
	) -> Result<rpc_model::GetTransactionReceiptResponse, NodeError> {
		let db_pool_conn = Database::get_pool_connection().await.map_err(|e| {
			NodeError::EventFetchError(format!("Failed to get db_pool_conn: {}", e))
		})?;
		let block_state = BlockState::new(&db_pool_conn)
			.await
			.map_err(|e| NodeError::DBError(format!("Failed to access database: {}", e)))?;

		let tx_resp = block_state
			.load_transaction_receipt(parse_hash(&req.hash).await?)
			.await
			.map_err(|e| {
				NodeError::TransactionReceiptFetchError(format!(
					"Failed to load transaction receipt by hash {}: {}",
					req.hash, e
				))
			})?
			.ok_or(NodeError::TransactionReceiptFetchError(format!(
				"No transaction receipt for hash {}:",
				req.hash
			)))?;

		let tx = to_proto_transaction(tx_resp.transaction).await?;
		let from = tx_resp.from.to_vec();
		let transaction_hash = tx_resp.tx_hash.to_vec();
		let block_hash = tx_resp.block_hash.to_vec();
		let block_number = i64::try_from(tx_resp.block_number).unwrap_or(i64::MAX);
		let fee_used = tx_resp.fee_used.to_string();
		let timestamp = u64::try_from(tx_resp.timestamp).unwrap_or(u64::MIN);

		let tx: rpc_model::TransactionResponse = rpc_model::TransactionResponse {
			transaction: Some(tx),
			from,
			transaction_hash,
			block_hash,
			block_number,
			fee_used,
			timestamp,
		};

		let response = GetTransactionReceiptResponse {
			transaction: Some(tx),
			status: if tx_resp.status {
				TransactionStatus::Succeed.into()
			} else {
				TransactionStatus::Failed.into()
			},
		};

		Ok(response)
	}

	pub async fn get_transactions_by_account(
		&self,
		req: GetTransactionsByAccountRequest,
	) -> Result<GetTransactionsByAccountResponse, NodeError> {
		let db_pool_conn = Database::get_pool_connection().await.map_err(|e| {
			NodeError::EventFetchError(format!("Failed to get db_pool_conn: {}", e))
		})?;
		let block_state = BlockState::new(&db_pool_conn).await.map_err(|e| {
			NodeError::DBError(format!("Failed to access BlockState database: {}", e))
		})?;

		let address = address_string_to_bytes(&req.address).await?;
		let min_block = req.starting_from;
		let num_transactions: usize = req.number_of_transactions as usize;

		let tx_vec: Vec<system::transaction_receipt::TransactionReceiptResponse> = block_state
			.load_transaction_receipt_by_address(address)
			.await
			.map_err(|e| NodeError::AccountFetchError(format!("Account may not exist: {}", e)))?;

		// Get only sorted transactions >= to the min requested block # and take the latest
		// num_transactions by block number
		let tx_vec_filtered: Vec<system::transaction_receipt::TransactionReceiptResponse> = tx_vec
			.into_iter()
			.filter(|tx| tx.block_number >= min_block as u128)
			.sorted_by_key(|tx| tx.block_number)
			.rev()
			.take(num_transactions)
			.collect();

		let mut proto_transactions: Vec<GetTransactionReceiptResponse> = vec![];
		for tx_receipt in tx_vec_filtered {
			let tx = to_proto_transaction(tx_receipt.transaction).await?;
			let from = tx_receipt.from.to_vec();
			let transaction_hash = tx_receipt.tx_hash.to_vec();
			let block_hash = tx_receipt.block_hash.to_vec();
			let block_number = i64::try_from(tx_receipt.block_number).unwrap_or(i64::MAX);
			let fee_used = tx_receipt.fee_used.to_string();
			let timestamp = u64::try_from(tx_receipt.timestamp).unwrap_or(u64::MIN);

			let tx: rpc_model::TransactionResponse = rpc_model::TransactionResponse {
				transaction: Some(tx),
				from,
				transaction_hash,
				block_hash,
				block_number,
				fee_used,
				timestamp,
			};

			// let tx = to_proto_transaction(tx_receipt.transaction).await?;
			let receipt_response = GetTransactionReceiptResponse {
				transaction: Some(tx),
				status: if tx_receipt.status {
					TransactionStatus::Succeed.into()
				} else {
					TransactionStatus::Failed.into()
				},
			};
			proto_transactions.push(receipt_response);
		}

		let response = GetTransactionsByAccountResponse { transactions: proto_transactions };

		Ok(response)
	}

	pub async fn smart_contract_read_only_call(
		&self,
		req: SmartContractReadOnlyCallRequest,
	) -> Result<SmartContractReadOnlyCallResponse, NodeError> {
		let db_pool_conn = Database::get_pool_connection().await.map_err(|e| {
			NodeError::EventFetchError(format!("Failed to get db_pool_conn: {}", e))
		})?;
		let call = match req.call {
			Some(c) => c,
			None =>
				return Err(NodeError::InvalidReadOnlyFunctionCall(format!(
					"Must contain a SmartContractFunctionCall"
				))),
		};
		let block_state = BlockState::new(&db_pool_conn).await.map_err(|e| {
			NodeError::DBError(format!("Failed to access BlockState database: {}", e))
		})?;
		let contract_instance_address = call.contract_address.try_into().map_err(|_| {
			NodeError::InvalidAddress("Failed to convert bytes to Address".to_string())
		})?;
		let function_name = call.function_name;
		let args = call.arguments.to_vec();
		let contract_instance_state =
			ContractInstanceState::new(&db_pool_conn).await.map_err(|e| {
				NodeError::DBError(format!(
					"Failed to access ContractInstanceState database: {}",
					e
				))
			})?;
		let contract_state = ContractState::new(&db_pool_conn).await.map_err(|e| {
			NodeError::DBError(format!("Failed to access ContractState database: {}", e))
		})?;

		let account_state = SysAccountState::new(&db_pool_conn).await.map_err(|e| {
			NodeError::DBError(format!("Failed to access AccountState database: {}", e))
		})?;

		let contract_instance = contract_instance_state
			.get_contract_instance(&contract_instance_address)
			.await
			.map_err(|e| NodeError::ContractInstanceFetchError(format!("Failed due to: {}", e)))?;
		let account = account_state
			.get_account(&contract_instance.owner_address)
			.await
			.map_err(|e| NodeError::AccountFetchError(format!("Account does not exist: {}", e)))?;
		let nonce = account.nonce + 1;
		let chain_state =
			block_state.load_chain_state(self.node.cluster_address).await.map_err(|e| {
				NodeError::ChainStateFetchError(format!("Failed to load chain state: {}", e))
			})?;
		let timestamp = SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.expect("Unable to get timestamp")
			.as_secs();

		EventState::new(&db_pool_conn)
			.await
			.map_err(|e| NodeError::DBError(format!("Failed to access EventState database: {}", e)))
			.and_then(|_event_state| Ok(()))?;

		let contract = contract_state
			.get_contract(&contract_instance.contract_address)
			.await
			.map_err(|e| NodeError::ContractFetchError(format!("Failed due to: {}", e)))?;
		let contract_type = contract.r#type.try_into().map_err(|_| {
			NodeError::InvalidContractType(format!("Invalid ContractType: {}", contract.r#type))
		})?;
		let executor: &(dyn ContractExecutor + Sync) = match contract_type {
			ContractType::EVM => &execute_contract_evm::ExecuteContract,
		};

		// TODO: Future intern refactor - read only calls don't return events, but low level api's
		// require this event channel sender
		let (event_tx, _) = broadcast::channel(1000);

		let (result, _gas) = executor
			.execute_contract_function_call_read_only(
				&contract,
				&contract_instance,
				&function_name,
				&args,
				&self.node.cluster_address,
				nonce,
				chain_state.block_number,
				&chain_state.block_hash,
				timestamp,
				Arc::new(&db_pool_conn),
				Arc::new(&db_pool_conn),
				event_tx,
			)
			.await;

		let result = match result {
			Capture::Exit(reason) => {
				let (reason, result) = reason;
				(*result).clone()
			},
			Capture::Trap(resolve) => {
				vec![]
			},
		};
		/*.map_err(|e| {
			NodeError::SmartContractCallFailed(format!("Read only call failed: {}", e))
		})?;*/

		let response =
			SmartContractReadOnlyCallResponse { status: TransactionStatus::Succeed.into(), result };

		Ok(response)
	}

	pub async fn get_chain_state(
		&self,
		_req: GetChainStateRequest,
	) -> Result<GetChainStateResponse, NodeError> {
		let db_pool_conn = Database::get_pool_connection().await.map_err(|e| {
			NodeError::EventFetchError(format!("Failed to get db_pool_conn: {}", e))
		})?;
		let block_state = BlockState::new(&db_pool_conn).await.map_err(|e| {
			NodeError::DBError(format!("Failed to access BlockState database: {}", e))
		})?;

		let cluster_address = self.node.cluster_address;

		let chain_state = block_state.load_chain_state(cluster_address).await.map_err(|e| {
			NodeError::ChainStateFetchError(format!("Failed to load chain state: {}", e))
		})?;

		let response = GetChainStateResponse {
			cluster_address: hex::encode(chain_state.cluster_address),
			head_block_number: chain_state.block_number.to_string(),
			head_block_hash: hex::encode(chain_state.block_hash),
		};

		Ok(response)
	}

	pub async fn get_block_by_number(
		&self,
		req: GetBlockByNumberRequest,
	) -> Result<GetBlockByNumberResponse, NodeError> {
		let db_pool_conn = Database::get_pool_connection().await.map_err(|e| {
			NodeError::EventFetchError(format!("Failed to get db_pool_conn: {}", e))
		})?;
		let block_state = BlockState::new(&db_pool_conn).await.map_err(|e| {
			NodeError::DBError(format!("Failed to access BlockState database: {}", e))
		})?;
		let block_number = parse_block_number(req.block_number).await?;
		let cluster_address = self.node.cluster_address;

		let persisted_block = block_state
			.load_block_response(block_number, &cluster_address)
			.await
			.map_err(|e| NodeError::BlockFetchError(format!("Failed to get block: {}", e)))?;

		let mut transactions: Vec<rpc_model::TransactionResponse> = vec![];
		for txn in persisted_block.transactions.into_iter() {
			let txn = rpc_model::TransactionResponse {
				transaction: Some(to_proto_transaction(txn.transaction).await?),
				from: txn.from.to_vec(),
				transaction_hash: txn.tx_hash.to_vec(),
				block_hash: txn.block_hash.to_vec(),
				block_number: i64::try_from(txn.block_number).unwrap_or(i64::MAX),
				fee_used: txn.fee_used.to_string(),
				timestamp: txn.timestamp as u64,
			};

			transactions.push(txn);
		}
		let timestamp = SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.expect("Unable to get timestamp")
			.as_micros();
		let block = Block {
			number: block_number.to_string(),
			hash: hex::encode(persisted_block.block_header.block_hash),
			parent_hash: hex::encode(persisted_block.block_header.parent_hash),
			timestamp: timestamp as u64,
			transactions,
			block_type: persisted_block.block_header.block_type as i32,
			cluster_address: hex::encode(cluster_address),
		};

		Ok(GetBlockByNumberResponse { block: Some(block) })
	}

	/// Get the latest n number of block headers
	pub async fn get_latest_block_headers(
		&self,
		num_headers: u32,
	) -> Result<Vec<rpc_model::BlockHeader>, NodeError> {
		let db_pool_conn = Database::get_pool_connection().await.map_err(|e| {
			NodeError::EventFetchError(format!("Failed to get db_pool_conn: {}", e))
		})?;
		let block_state = BlockState::new(&db_pool_conn).await.map_err(|e| {
			NodeError::DBError(format!("Failed to access BlockState database: {}", e))
		})?;

		let block_headers = block_state
			.load_latest_block_headers(num_headers, self.node.cluster_address)
			.await
			.map_err(|e| {
				NodeError::BlockFetchError(format!("Failed to get latest block headers: {}", e))
			})?;

		Ok(block_headers)
	}

	pub async fn get_latest_transactions(
		&self,
		req: GetLatestTransactionsRequest,
	) -> Result<(), NodeError> {
		let db_pool_conn = Database::get_pool_connection().await.map_err(|e| {
			NodeError::EventFetchError(format!("Failed to get db_pool_conn: {}", e))
		})?;
		let block_state = BlockState::new(&db_pool_conn).await.map_err(|e| {
			NodeError::DBError(format!("Failed to access BlockState database: {}", e))
		})?;

		let num_transactions = req.number_of_transactions;

		// let tx_per_page: usize = req.transactions_per_page.try_into().unwrap_or(usize::MAX);

		block_state.load_latest_transactions(num_transactions).await.map_err(|e| {
			NodeError::BlockFetchError(format!("Failed to get latest transactions: {}", e))
		})?;

		Ok(())
	}

	pub async fn get_stake(&self, req: GetStakeRequest) -> Result<GetStakeResponse, NodeError> {
		let db_pool_conn = Database::get_pool_connection().await.map_err(|e| {
			NodeError::EventFetchError(format!("Failed to get db_pool_conn: {}", e))
		})?;
		let staking_state = StakingState::new(&db_pool_conn)
			.await
			.map_err(|e| NodeError::DBError(format!("Failed to access database: {}", e)))?;

		let address = address_string_to_bytes(&req.account_address).await?;
		let pool_address = address_string_to_bytes(&req.pool_address).await?;

		let staking_account =
			staking_state.get_staking_account(&address, &pool_address).await.map_err(|e| {
				NodeError::StakeFetchError(format!("Failed to get staking account: {}. This likely means that the account has no stake in this pool i.e. a staking balance of 0.", e))
			})?;

		let response = GetStakeResponse { amount: staking_account.balance.to_string() };

		Ok(response)
	}

	/// Get the current nonce value for the provided account address
	pub async fn get_current_nonce(
		&self,
		req: GetCurrentNonceRequest,
	) -> Result<GetCurrentNonceResponse, NodeError> {
		let db_pool_conn = Database::get_pool_connection().await.map_err(|e| {
			NodeError::EventFetchError(format!("Failed to get db_pool_conn: {}", e))
		})?;
		let account_state = SysAccountState::new(&db_pool_conn)
			.await
			.map_err(|e| NodeError::DBError(format!("Failed to access database: {}", e)))?;
		let address = address_string_to_bytes(&req.address).await?;

		let account = account_state
			.get_account(&address)
			.await
			.map_err(|e| NodeError::AccountFetchError(format!("Account does not exist: {}", e)))?;

		let response = GetCurrentNonceResponse { nonce: u128::to_string(&account.nonce) };

		Ok(response)
	}

	/// Get event(s) emitted from a transaction, by hash
	pub async fn get_events(&self, req: GetEventsRequest) -> Result<GetEventsStream, NodeError> {
		let (tx, rx) = tokio::sync::mpsc::channel(4);

		tokio::spawn(async move {
			let db_pool_conn = Database::get_pool_connection().await.map_err(|e| {
				NodeError::EventFetchError(format!("Failed to get db_pool_conn: {}", e))
			})?;
			let event_state = EventState::new(&db_pool_conn).await.map_err(|e| {
				NodeError::DBError(format!("Failed to access events database: {}", e))
			})?;

			let tx_hash = parse_hash(&req.tx_hash).await?;
			let events = event_state
				.get_all_events(&tx_hash)
				.await
				.map_err(|e| NodeError::EventFetchError(format!("Failed to find events: {}", e)))?;

			let response = GetEventsResponse { events_data: events };
			if tx.send(Ok(response)).await.is_err() {
				return Err(NodeError::UnexpectedError(format!("Event receiver has dropped")));
			} else {
				Ok(())
			}
		});

		Ok(tokio_stream::wrappers::ReceiverStream::new(rx))
	}

	// Check if the transaction has been included in a block
	pub async fn monitor_transaction_inclusion(&self, hash: H256) -> Result<bool, NodeError> {
		let hash_string = format!("{:x}", hash);
		let prost_string: ::prost::alloc::string::String = hash_string.into();
		let request = GetEventsRequest { tx_hash: prost_string, timestamp: 0u64 };

		let max_attempts = 200;
		let delay_duration = tokio::time::Duration::from_millis(1000);
		let mut attempts = 0;

		loop {
			if attempts >= max_attempts {
				break; // Exit the loop after max_attempts
			} else {
				match self.get_events(request.clone()).await {
					Ok(event_response) => {
						let response =
							match event_response.into_inner().recv().await.ok_or_else(|| {
								NodeError::ParseError("No events found".to_string())
							})? {
								Ok(response) => response,
								Err(e) => {
									error!("Failed to get events: {}", e);
									continue;
								},
							};

						for event in response.events_data {
							if let Ok(output) = serde_json::from_slice::<serde_json::Value>(&event)
							{
								if let Ok(event_log) = serde_json::from_value::<Log>(output) {
									if event_log.transaction_hash == Some(hash) {
										return Ok(true);
									}
								}
							}
						}
					},
					Err(e) => {
						error!("Failed to get events: {}", e);
					},
				}

				attempts += 1; // Increment the attempt counter
				tokio::time::sleep(delay_duration).await;
			}
		}

		Ok(false) // Return false if the transaction is not found after all attempts
	}
}

async fn address_string_to_bytes(address_str: &str) -> Result<[u8; 20], NodeError> {
	// Check if the string is exactly 40 characters long (20 bytes in hexadecimal representation)
	if address_str.len() / 2 != 20 {
		return Err(NodeError::InvalidAddress(format!(
			"Invalid address length {}",
			address_str.len() / 2
		)))
	}

	// Convert the hexadecimal string into bytes
	let bytes: Result<Vec<u8>, _> = (0..20)
		.map(|i| {
			u8::from_str_radix(&address_str[i * 2..(i * 2) + 2], 16).map_err(|e| e.to_string())
		})
		.collect();

	// Convert the Vec<u8> into [u8; 20] array
	let res = bytes
		.map(|vec| {
			let mut result = [0u8; 20];
			result.copy_from_slice(&vec);
			result
		})
		.map_err(|e| {
			NodeError::InvalidAddress(format!("Failed to convert address string to bytes: {e}",))
		})?;
	Ok(res)
}

async fn parse_hash(hash: &str) -> Result<[u8; 32], NodeError> {
	if hash.len() != 64 {
		Err(NodeError::ParseError(format!("Invalid hash str length: {}", hash.len())))
	} else {
		let mut bytes = [0u8; 32];

		for (i, hex_pair) in hash.as_bytes().chunks(2).enumerate() {
			let hash_str = std::str::from_utf8(hex_pair)
				.map_err(|_| NodeError::ParseError(format!("Invalid hex format {}", hash)))?;

			bytes[i] = u8::from_str_radix(hash_str, 16)
				.map_err(|_| NodeError::ParseError(format!("Invalid hex format {}", hash)))?;
		}

		Ok(bytes)
	}
}
#[allow(dead_code)]
async fn parse_amount(amount: String) -> Result<Balance, NodeError> {
	amount
		.parse::<Balance>()
		.map_err(|_| NodeError::ParseError("Failed to parse amount".to_string()))
}

async fn parse_block_number<T: AsRef<str>>(block_number: T) -> Result<BlockNumber, NodeError> {
	block_number
		.as_ref()
		.parse::<BlockNumber>()
		.map_err(|_| NodeError::ParseError("Failed to parse block a mount".to_string()))
}

#[allow(dead_code)]
async fn parse_u128(value: &str) -> Option<u128> {
	match u128::from_str(value) {
		Ok(parsed) => Some(parsed),
		Err(err) => {
			error!("Failed to parse u128: {}", err);
			None
		},
	}
}
