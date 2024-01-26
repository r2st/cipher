use crate::service::FullNodeService;
use account::account_state::AccountState;
use anyhow::{self};
use async_scoped::TokioScope;
use block::block_state::BlockState;
use contract::contract_state::ContractState;
use contract_instance::contract_instance_state::ContractInstanceState;
use ethereum_types::{H160, H256, U256, U64};
use types::eth::{
	block::{Block, BlockNumber, Header},
	bytes::Bytes,
	filter::{Filter, Kind, Params},
	log::Log,
	signers::{EvmEthCallRequest, TransactionRequest},
	sync::{SyncInfo, SyncStatus},
	transaction::{Transaction, TransactionReceipt},
};

use event::event_state::EventState;

use jsonrpsee::{
	core::{async_trait, SubscriptionResult},
	proc_macros::rpc,
	//server::{Methods, ServerHandle, ws, stop_channel},
	types::ErrorObjectOwned,
	PendingSubscriptionSink,
	SubscriptionMessage,
};
//use hyper::service::{service_fn};
//use hyper::server::conn::AddrStream;
use jsonrpsee_server::{stop_channel, ws, Methods, Server, ServerHandle};
//use jsonrpsee_server::Server;
//use tower::Service;
use cipher_rpc::rpc_model::{
	GetAccountStateRequest, GetAccountStateResponse, GetBlockByNumberRequest,
	GetBlockByNumberResponse, GetChainStateRequest, GetChainStateResponse, GetCurrentNonceRequest,
	GetCurrentNonceResponse, GetEventsRequest, GetEventsResponse, GetLatestBlockHeadersRequest,
	GetLatestBlockHeadersResponse, GetLatestTransactionsRequest, GetLatestTransactionsResponse,
	GetStakeRequest, GetStakeResponse, GetTransactionReceiptRequest, GetTransactionReceiptResponse,
	GetTransactionsByAccountRequest, GetTransactionsByAccountResponse, SmartContractFunctionCall,
	SmartContractReadOnlyCallRequest, SmartContractReadOnlyCallResponse, SubmitTransactionRequest,
	SubmitTransactionResponse,
};
use lazy_static::lazy_static;
use log::{debug, error, info};
use prost::alloc::string::String;
use secp256k1::{
	ecdsa::{RecoveryId, Signature},
	Secp256k1, SECP256K1,
};
use serde_json::{json, Value};
use std::{error::Error as StdError, net::SocketAddr, str::FromStr};
use system::{errors::NodeError, network::EventBroadcast};
use tokio::{
	sync::{broadcast, broadcast::error::RecvError, mpsc, oneshot},
	task, time,
};
use tower_http::cors::{Any, CorsLayer};
use util::convert::u256_to_balance;

const MAX_CONNECTIONS: u32 = 10_000u32;

use std::time::{SystemTime, UNIX_EPOCH};
#[macro_export]
macro_rules! error_object {
	($message:expr, $data:expr) => {
		ErrorObjectOwned::owned(400, $message, $data)
	};
	($message:expr) => {
		ErrorObjectOwned::owned(400, $message, None::<()>)
	};
}

pub enum TypedTransaction {
	Legacy(TransactionRequest),
	// Eip2930(Eip2930TransactionRequest),
	// Eip1559(Eip1559TransactionRequest),
	// DepositTransaction(DepositTransaction),
}

#[cfg(test)]
mod mockable {

	use jsonrpsee::{DisconnectError, SubscriptionMessage};
	use mockall::automock;

	pub struct SubscriptionSink {}
	#[automock]
	impl SubscriptionSink {
		pub async fn send(&self, msg: SubscriptionMessage) -> Result<(), DisconnectError> {
			Ok(())
		}
	}
}

use db::db::Database;
#[cfg(not(test))]
use jsonrpsee::SubscriptionSink;
use tonic::codegen::http::Method;

lazy_static! {
	static ref DEFAULT_GAS_AMOUNT: U256 = U256::from(10000);
	static ref DEFAULT_GAS_PRICE: U256 = U256::from(10000);
	static ref CHAIN_ID: U64 = 1066.into(); // ciphervm in roman numerals (cipher = 61, already taken by ethereum classic)
}

// const RETRY_LIMIT: usize = 10; // maximum number of retries
// const TIMEOUT_DURATION: tokio::time::Duration = tokio::time::Duration::from_secs(5); // 10
// seconds timeout

async fn vec_to_bytes(data: Vec<u8>) -> Result<Bytes, ErrorObjectOwned> {
	// Convert Vec<u8> to Bytes
	Ok(Bytes::from(data))
}

async fn convert_ethereum_log(value: Value) -> Result<Log, serde_json::Error> {
	// Assuming `value` is a serde_json::Value representing a Log
	serde_json::from_value(value)
}

// async fn confirm_transaction(
//     transaction_hash: [u8; 32],
//     cluster_address: [u8; 20],
//     required_confirmations: u32,
// ) -> Result<u32, ErrorObjectOwned> {
//     let block_state = match BlockState::new().await {
//         Ok(state) => state,
//         Err(e) => {
//             log::error!("Failed to initialize block state because {:?}", e);
//             return Err(error_object!("Failed to initialize block state"));
//         },
//     };

//     let transaction = match block_state.load_transaction_receipt(transaction_hash).await {
//         Ok(Some(transaction)) => transaction,
//         Ok(None) => return Err(error_object!("Transaction not found")),
//         Err(e) => {
//             log::error!("Failed to load transaction receipt because {:?}", e);
//             return Err(error_object!("Failed to load transaction receipt"));
//         },
//     };

//     let transaction_block_num = transaction.block_number as u32;
//     let start_time = tokio::time::Instant::now();  // Start time for timeout tracking

//     loop {
//         let current_block = match block_state.load_chain_state(cluster_address).await {
//             Ok(state) => state.block_number as u32,
//             Err(e) => {
//                 log::error!("Failed to load chain state because {:?}", e);
//                 return Err(error_object!("Failed to load chain state"));
//             },
//         };

//         let confirmations = current_block.saturating_sub(transaction_block_num);

//         if confirmations >= required_confirmations {
//             return Ok(confirmations);
//         }

//         if tokio::time::Instant::now().duration_since(start_time) > TIMEOUT_DURATION {
//             // Timeout reached
//             return Err(error_object!("Confirmation timeout reached"));
//         }

//         // Wait for some time before checking again
//         tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
//     }
// }

#[cfg(test)]
use mockable::MockSubscriptionSink as SubscriptionSink;
use system::mempool::ResponseMempool;

#[rpc(server, namespace = "eth")]
pub trait EvmCompatibility {
	#[method(name = "syncing")]
	async fn syncing(&self) -> Result<SyncStatus, ErrorObjectOwned>;

	#[method(name = "coinbase")]
	async fn coinbase(&self) -> Result<H160, ErrorObjectOwned>;

	#[method(name = "getStorageAt")]
	async fn get_storage_at(
		&self,
		address: H160,
		index: H256,
		block: BlockNumber,
	) -> Result<H256, ErrorObjectOwned>;

	#[method(name = "getBalance")]
	async fn balance(
		&self,
		address: H160,
		block_number: Option<BlockNumber>,
	) -> Result<U256, ErrorObjectOwned>;

	#[method(name = "maxPriorityFeePerGas")]
	async fn max_priority_fee_per_gas(&self) -> Result<U256, ErrorObjectOwned>;

	#[method(name = "getLogs")]
	async fn get_logs(&self, filter: serde_json::Value) -> Result<Vec<Log>, ErrorObjectOwned>;

	#[method(name = "getCode")]
	async fn get_code(
		&self,
		address: H160,
		block: Option<BlockNumber>,
	) -> Result<Bytes, ErrorObjectOwned>;

	#[method(name = "call")]
	async fn call(
		&self,
		request: EvmEthCallRequest,
		block: BlockNumber,
	) -> Result<Bytes, ErrorObjectOwned>;

	#[method(name = "accounts")]
	async fn accounts(&self) -> Result<Vec<H160>, ErrorObjectOwned>;

	/// Note:
	/// need name = "subscription" as the method of the returned events needs to be
	/// "eth_subscription" however, need to use "eth_subscribe" as the method name in the request
	/// but for name = "subscription", we need unsubscribe specified as well, not sure yet if it
	/// should be "unsubscribe" or "eth_unsubscribe"
	#[subscription(name = "subscription", aliases = ["eth_subscribe"], item = String, unsubscribe = "unsubscribe")]
	async fn subscribe(&self, kind: Kind, params: Option<Params>) -> SubscriptionResult;

	#[method(name = "blockNumber")]
	async fn block_number(&self) -> Result<String, ErrorObjectOwned>;

	#[method(name = "chainId")]
	async fn chain_id(&self) -> Result<Option<U64>, ErrorObjectOwned>;

	#[method(name = "feeHistory")]
	async fn fee_history(
		&self,
		block_count: U256,
		newest_block: BlockNumber,
		reward_percentiles: Vec<f64>,
	) -> Result<ethers::types::FeeHistory, ErrorObjectOwned>;

	#[method(name = "estimateGas")]
	async fn estimate_gas(
		&self,
		eth_call: TransactionRequest,
		block_number: Option<BlockNumber>,
	) -> Result<U256, ErrorObjectOwned>;

	#[method(name = "gasPrice")]
	async fn gas_price(&self) -> Result<U256, ErrorObjectOwned>;

	#[method(name = "getBlockByNumber")]
	async fn get_block_by_number(
		&self,
		block_number: BlockNumber,
		include_tx: bool,
	) -> Result<Option<Block>, ErrorObjectOwned>;

	#[method(name = "getTransactionCount")]
	async fn get_transaction_count(
		&self,
		address: H160,
		number: Option<BlockNumber>,
	) -> Result<U256, ErrorObjectOwned>;

	#[method(name = "getBlockTransactionCountByNumber")]
	async fn get_block_transaction_count_by_number(
		&self,
		block: BlockNumber,
	) -> Result<U256, ErrorObjectOwned>;

	#[method(name = "getTransactionByHash")]
	async fn transaction_by_hash(
		&self,
		hash: H256,
	) -> Result<Option<Transaction>, ErrorObjectOwned>;

	#[method(name = "getTransactionReceipt")]
	async fn transaction_receipt(
		&self,
		hash: H256,
	) -> Result<Option<TransactionReceipt>, ErrorObjectOwned>;

	#[method(name = "sendTransaction")]
	async fn send_transaction(&self, request: TransactionRequest)
		-> Result<H256, ErrorObjectOwned>;

	#[method(name = "sendRawTransaction")]
	async fn send_raw_transaction(&self, bytes: Bytes) -> Result<H256, ErrorObjectOwned>;
}

#[rpc(server, namespace = "cipher")]
pub trait FullNodeJson {
	#[method(name = "getAccountState")]
	async fn get_account_state(
		&self,
		request: GetAccountStateRequest,
	) -> Result<GetAccountStateResponse, ErrorObjectOwned>;

	#[method(name = "submitTransaction")]
	async fn submit_transaction(
		&self,
		request: SubmitTransactionRequest,
	) -> Result<SubmitTransactionResponse, ErrorObjectOwned>;

	#[method(name = "getTransactionReceipt")]
	async fn get_transaction_receipt(
		&self,
		request: GetTransactionReceiptRequest,
	) -> Result<GetTransactionReceiptResponse, ErrorObjectOwned>;

	#[method(name = "getTransactionsByAccount")]
	async fn get_transactions_by_account(
		&self,
		request: GetTransactionsByAccountRequest,
	) -> Result<GetTransactionsByAccountResponse, ErrorObjectOwned>;

	#[method(name = "smartContractReadOnlyCall")]
	async fn smart_contract_read_only_call(
		&self,
		request: SmartContractReadOnlyCallRequest,
	) -> Result<SmartContractReadOnlyCallResponse, ErrorObjectOwned>;

	#[method(name = "getChainState")]
	async fn get_chain_state(
		&self,
		request: GetChainStateRequest,
	) -> Result<GetChainStateResponse, ErrorObjectOwned>;

	#[method(name = "getBlockByNumber")]
	async fn get_block_by_number(
		&self,
		request: GetBlockByNumberRequest,
	) -> Result<GetBlockByNumberResponse, ErrorObjectOwned>;

	#[subscription(name = "getLatestBlockHeaders", item = String, unsubscribe = "unsubscribeLatestBlockHeaders")]
	async fn get_latest_block_headers(
		&self,
		request: GetLatestBlockHeadersRequest,
	) -> SubscriptionResult;

	#[subscription(name = "getLatestTransactions", item = String, unsubscribe = "unsubscribeLatestTransactions")]
	async fn get_latest_transactions(
		&self,
		request: GetLatestTransactionsRequest,
	) -> SubscriptionResult;

	#[method(name = "getStake")]
	async fn get_stake(
		&self,
		request: GetStakeRequest,
	) -> Result<GetStakeResponse, ErrorObjectOwned>;

	#[method(name = "getCurrentNonce")]
	async fn get_current_nonce(
		&self,
		request: GetCurrentNonceRequest,
	) -> Result<GetCurrentNonceResponse, ErrorObjectOwned>;

	#[method(name = "getEvents")]
	async fn get_events(
		&self,
		request: GetEventsRequest,
	) -> Result<GetEventsResponse, ErrorObjectOwned>;

	#[subscription(name = "subscribeEvents", item = String)]
	async fn subscribe_events(&self, kind: Kind, params: Option<Params>) -> SubscriptionResult;
}

// impl From<NodeError> for ErrorObject<'_> {
//     fn from(err: NodeError) -> Self {
//         ErrorObjectOwned::owned(400, err.to_string(), None::<()>)
//     }
// }

pub struct FullNodeJsonImpl {
	service: FullNodeService,
	mempool_json_rx: mpsc::Receiver<ResponseMempool>,
}

#[async_trait]
impl EvmCompatibilityServer for FullNodeJsonImpl {
	async fn syncing(&self) -> Result<SyncStatus, ErrorObjectOwned> {
		let db_pool_conn = Database::get_pool_connection()
			.await
			.map_err(|e| error_object!(&format!("Failed to get db_pool_conn: {}", e)))?;
		let block_state = match BlockState::new(&db_pool_conn).await {
			Ok(state) => state,
			Err(e) => {
				log::error!("Failed to initialize block state because {:?}", e);
				return Err(error_object!("Failed to initialize block state"));
			},
		};
		let current_block =
			match block_state.load_chain_state(self.service.node.cluster_address).await {
				Ok(state) => state.block_number,
				Err(e) => {
					log::error!("Failed to load chain state because {:?}", e);
					return Err(error_object!("Failed to load chain state"));
				},
			};
		Ok(SyncStatus::Info(SyncInfo {
			starting_block: U256::zero(),
			current_block: U256::from(current_block),
			highest_block: U256::from(current_block),
			warp_chunks_amount: None,
			warp_chunks_processed: None,
		}))
	}

	async fn coinbase(&self) -> Result<H160, ErrorObjectOwned> {
		Ok(H160::from([0u8; 20]))
	}

	async fn get_storage_at(
		&self,
		address: H160,
		index: H256,
		_block: BlockNumber,
	) -> Result<H256, ErrorObjectOwned> {
		let db_pool_conn = Database::get_pool_connection().await.map_err(|e| {
			NodeError::EventFetchError(format!("Failed to get db_pool_conn: {}", e))
		})?;
		let contract_instance_state = match ContractInstanceState::new(&db_pool_conn).await {
			Ok(result) => result,
			Err(e) => {
				return Err(error_object!(&format!(
					"Failed to start contract instance state because of {}",
					e
				)));
			},
		};
		let key: Vec<u8> = index.as_bytes().to_vec();
		let result = match contract_instance_state.get_state_key_value(&address.0, &key).await {
			Ok(Some(result)) => result,
			Ok(None) => {
				return Err(error_object!(&format!(
					"Failed to get state key value because of {}",
					"Key not found"
				)));
			},
			Err(e) => {
				// Handle the error case
				return Err(error_object!(&format!(
					"Failed to get state key value because of {}",
					e
				)));
			},
		};

		Ok(H256::from_slice(&result))
	}

	async fn balance(
		&self,
		address: H160,
		_block_number: Option<BlockNumber>,
	) -> Result<U256, ErrorObjectOwned> {
		let db_pool_conn = Database::get_pool_connection().await.map_err(|e| {
			NodeError::EventFetchError(format!("Failed to get db_pool_conn: {}", e))
		})?;
		let account_state = match AccountState::new(&db_pool_conn).await {
			Ok(result) => result,
			Err(e) => {
				return Err(error_object!(&format!(
					"Failed to start account state because of {}",
					e
				)));
			},
		};
		let balance = match account_state.get_balance(&address.0).await {
			Ok(result) => result,
			Err(e) => {
				return Err(error_object!(&format!(
					"Failed to get account balance because of {}",
					e
				)));
			},
		};
		Ok(U256::from(balance))
	}

	async fn max_priority_fee_per_gas(&self) -> Result<U256, ErrorObjectOwned> {
		// let block_state = match BlockState::new().await {
		// 	Ok(result) => result,
		// 	Err(e) => {
		// 		return Err(error_object!(&format!(
		// 			"Failed to start block state because of {}",
		// 			e
		// 		)));
		// 	},
		// };
		// // https://github.com/ethereum/go-ethereum/blob/master/eth/ethconfig/config.go#L44-L51
		// let at_percentile = 60;
		// let block_count = 20;
		// let index = (at_percentile * 2) as usize;
		// let highest = match
		// block_state.load_chain_state(self.service.node.cluster_address).await{ 	Ok(state) =>
		// state.block_number as u64, 	Err(e) => {
		// 		return Err(error_object!(&format!(
		// 			"Failed to load chain state because of {}",
		// 			e
		// 		)));
		// 	},
		// };
		// let lowest = highest.saturating_sub(block_count - 1);
		// // https://github.com/ethereum/go-ethereum/blob/master/eth/gasprice/gasprice.go#L149
		// let mut rewards = Vec::new();
		// if let Ok(fee_history_cache) = &self.fee_history_cache.lock() {
		// 	for n in lowest..highest + 1 {
		// 		if let Some(block) = fee_history_cache.get(&n) {
		// 			let reward = if let Some(r) = block.rewards.get(index) {
		// 				U256::from(*r)
		// 			} else {
		// 				U256::zero()
		// 			};
		// 			rewards.push(reward);
		// 		}
		// 	}
		// } else {
		// 	return Err(internal_err("Failed to read fee oracle cache."));
		// }
		// Ok(*rewards.iter().min().unwrap_or(&U256::zero()))
		Ok(U256::zero())
	}

	//Get code of address.
	async fn get_code(
		&self,
		address: H160,
		_block: Option<BlockNumber>,
	) -> Result<Bytes, ErrorObjectOwned> {
		let (tx, rx) = oneshot::channel();
		let db_pool_conn = Database::get_pool_connection()
			.await
			.map_err(|e| error_object!(&format!("Failed to get db_pool_conn: {}", e)))?;
		let contract_state = ContractState::new(&db_pool_conn).await.map_err(|e| {
			error_object!(&format!("Failed to start contract state because of: {}", e))
		})?;
		TokioScope::scope_and_block(|scope| {
			scope.spawn(async move {
				let max_attempts = 200;
				let delay_duration = time::Duration::from_millis(300);
				let mut attempts = 0;

				while attempts < max_attempts {
					match contract_state.get_contract(&address.0).await {
						Ok(result) => {
							let _ = tx.send(Ok(Bytes::from(result.code)));
							return;
						},
						Err(_) => {
							attempts += 1;
							time::sleep(delay_duration).await;
						},
					};
				}

				let _ = tx.send(Err(error_object!(&format!(
					"Max attempts reached without retrieving the contract"
				))));
			});
		});

		match rx.await {
			Ok(result) => result,
			Err(_) => Err(error_object!("Channel closed before receiving a bytes response")),
		}
	}

	async fn accounts(&self) -> Result<Vec<H160>, ErrorObjectOwned> {
		let mut accounts = Vec::new();
		let signers = match &self.service.signers {
			Some(signer) => signer,
			None => return Err(error_object!("Could not return contract")),
		};
		for signer in signers {
			accounts.append(&mut signer.accounts());
		}
		Ok(accounts)
	}

	// Assume necessary imports and context are present
	async fn call(
		&self,
		request: EvmEthCallRequest,
		_block: BlockNumber,
	) -> Result<Bytes, ErrorObjectOwned> {
		Database::get_pool_connection()
			.await
			.map_err(|e| error_object!(&format!("Failed to get db_pool_conn: {}", e)))
			.and_then(|_db_conn| Ok(()))?;

		let to = request
			.to
			.ok_or_else(|| error_object!("No 'to' address found in the request"))?;
		let data = request.data.ok_or_else(|| error_object!("No data found in the request"))?;

		let data = data.into_vec();

		if data.len() < 4 {
			return Err(error_object!("Data too short for a function selector"));
		}
		let request = SmartContractReadOnlyCallRequest {
			call: Some(SmartContractFunctionCall {
				contract_address: to.0.into(),
				function_name: "".as_bytes().to_vec(),
				arguments: data,
			}),
		};
		let response = match self.service.smart_contract_read_only_call(request).await {
			Ok(res) => res,
			Err(e) => return Err(e.into()),
		};
		Ok(response.result.into())
	}

	async fn fee_history(
		&self,
		block_count: U256,
		newest_block: BlockNumber,
		reward_percentiles: Vec<f64>,
	) -> Result<ethers::types::FeeHistory, ErrorObjectOwned> {
		let db_pool_conn = Database::get_pool_connection()
			.await
			.map_err(|e| error_object!(&format!("Failed to get db_pool_conn: {}", e)))?;
		let block_state = BlockState::new(&db_pool_conn)
			.await
			.map_err(|e| error_object!(&format!("Failed to start BlockState due to {e}")))?;
		let range_limit = U256::from(1024);
		let block_count =
			if block_count > range_limit { range_limit.as_usize() } else { block_count.as_usize() };
		let gas_used_ratio: Vec<f64> = std::iter::repeat_with(|| 0.1).take(block_count).collect();
		let reward_percent = match reward_percentiles {
			percentiles => {
				let rewards: Vec<Vec<U256>> = (0..block_count)
					.map(|_| percentiles.iter().map(|_| U256::from(1u8)).collect())
					.collect();
				Some(rewards)
			},
		};
		// Determine the range of blocks to analyze
		let end_block = match newest_block {
			BlockNumber::Earliest => 0,
			BlockNumber::Latest => {
				let chain_state = block_state
					.load_chain_state(self.service.node.cluster_address)
					.await
					.map_err(|e| error_object!(&format!("Failed to load block due to {e}")))?;
				chain_state.block_number as u64
			},
			BlockNumber::Num(x) => x,
			_ => return Err(error_object!("Unsupported block number")),
		};

		let start_block = end_block.saturating_sub(block_count as u64);
		let mut base_fee_per_gas: Vec<U256> = Vec::new();
		base_fee_per_gas.push(*DEFAULT_GAS_AMOUNT);
		let reward_view: Option<Vec<Vec<U256>>> = reward_percent.map(|rewards| {
			rewards.iter().map(|inner| inner.iter().map(|u256| *u256).collect()).collect()
		});
		let reward = match reward_view {
			Some(output) => output,
			None => return Err(error_object!("Unsupported reward data")),
		};
		let fee_history = ethers::types::FeeHistory {
			oldest_block: start_block.into(),
			base_fee_per_gas,
			gas_used_ratio,
			reward,
		};
		Ok(fee_history)
	}

	async fn get_logs(&self, filter: Value) -> Result<Vec<Log>, ErrorObjectOwned> {
		let db_pool_conn = Database::get_pool_connection()
			.await
			.map_err(|e| error_object!(&format!("Failed to get db_pool_conn: {}", e)))?;
		let filter: Filter = serde_json::from_value(filter)
			.map_err(|e| NodeError::ParseError(format!("Failed to parse log filter: {}", e)))?;
		println!("FILTER: {filter:?}");

		let event_state = EventState::new(&db_pool_conn)
			.await
			.map_err(|e| NodeError::DBError(format!("Failed to access database: {}", e)))?;

		// Get filtered logs
		let filtered_logs = event_state
			.get_filtered_events(filter)
			.await
			.map_err(|e| NodeError::EventFetchError(format!("Failed to get logs: {}", e)))?;

		Ok(filtered_logs)
	}

	// needs to stream:
	// {
	//     "removed":false,
	//     "logIndex":"0x0",
	//     "transactionIndex":"0x0",
	//     "transactionHash":"0x6f8524f68597146124ae43dfa4997cbdf63ba98ccd5b5114c0c502c49f244d0d",
	//     "blockHash":"0x7a8decff523ca5f0f79bd03b05d657715773760b963abd18dbf23065c3b42597",
	//     "blockNumber":"0x1544a6",
	//     "address":"0x537393a37a3be4a8129e6ca9dad8329ba6eef228",
	//     "data":"0x0000000000000000000000000000000000000000000000000000000000000020000000000000000000000000000000000000000000000000000000000000000d48656c6c6f2c20776f726c642100000000000000000000000000000000000000",
	//     "topics":["0x8da45d748eefefd09cc1491cd32086b6d6a0bd7063d08f05c94df9eb1404bd80","
	// 0x0000000000000000000000002cbd9e754f49a520497ae12f7f407301ddf96ee9"] }
	async fn subscribe(
		&self,
		pending: PendingSubscriptionSink,
		kind: Kind,
		params: Option<Params>,
	) -> SubscriptionResult {
		// TBD: fetch the historical logs

		#[cfg(not(test))]
		if kind == Kind::Logs {
			let sink = pending.accept().await?;
			subscribe_future_events(&self.service.node.node_evm_event_tx, sink, params, None)
				.await?;
		} else {
			pending.reject(error_object!("Block number exceed")).await;
		}

		Ok(())
	}

	async fn block_number(&self) -> Result<String, ErrorObjectOwned> {
		let chain_state_resp = self.service.get_chain_state(GetChainStateRequest {}).await?;
		let block_number: u128 = chain_state_resp
			.head_block_number
			.parse()
			.map_err(|_| error_object!("Invalid block number"))?;
		Ok(format!("0x{block_number:x}"))
	}

	async fn chain_id(&self) -> Result<Option<U64>, ErrorObjectOwned> {
		Ok(Some(*CHAIN_ID))
	}

	async fn estimate_gas(
		&self,
		_eth_calls: TransactionRequest,
		_block_number: Option<BlockNumber>,
	) -> Result<U256, ErrorObjectOwned> {
		// FIXME: calculate cost
		Ok(*DEFAULT_GAS_AMOUNT)
	}
	async fn gas_price(&self) -> Result<U256, ErrorObjectOwned> {
		// FIXME: fetch the gas price
		Ok(*DEFAULT_GAS_PRICE)
	}

	/// Retrieves a block by its number.
	///
	/// This function fetches a block based on the specified block number. The block number can be a
	/// specific number or a special value like the earliest or latest block. The function currently
	/// has a placeholder for optionally including transactions in the block, which is not
	/// implemented yet.
	///
	/// # Arguments
	///
	/// * `block_number`: `BlockNumber` - The number of the block to retrieve. Can be a specific
	///   number or a special value like `Earliest` or `Latest`.
	/// * `_include_tx`: `bool` - A placeholder parameter for optionally including transactions in
	///   the block. This feature is currently not implemented.
	///
	/// # Returns
	///
	/// * `Result<Option<Block<Transaction>>, ErrorObjectOwned>` - On success, returns an
	///   `Option<Block<Transaction>>` containing the block details if the block is found. Returns
	///   `None` if the block is not found. On failure, returns an `ErrorObjectOwned` indicating the
	///   cause of the error.
	///
	/// # Errors
	///
	/// * Returns an error if there is a failure in initializing the block state, loading the chain
	///   state, or fetching the block.
	/// * Returns an error if the provided block number is unsupported or if the block cannot be
	///   found.
	///
	/// # Example
	///
	/// ```no_run
	/// # // Mock struct and function for demonstration purposes
	/// # #[derive(Debug)] // Implementing the Debug trait
	/// # struct Block<T> { block_number: u64, transactions: Vec<T> } // Example block structure
	/// #[derive(Debug)]
	/// # struct Transaction { /* transaction details */ } // Example transaction structure
	/// # enum BlockNumber {
	/// #     Earliest, Latest,
	/// #     // ... possibly other variants ...
	/// # }
	/// # async fn get_block_by_number(block_number: BlockNumber, _include_tx: bool) -> Result<Option<Block<Transaction>>, String> {
	/// #     Ok(Some(Block { block_number: 1, transactions: vec![] })) // Mocked block details
	/// # }
	/// # fn main() {
	/// let block_number = BlockNumber::Latest; // Example block number
	/// let block = tokio::runtime::Runtime::new().unwrap().block_on(async {
	///     get_block_by_number(block_number, false).await // '_include_tx' is not functional yet
	/// });
	///
	/// match block {
	///     Ok(Some(block)) => println!("Block details: {:?}", block),
	///     Ok(None) => println!("Block not found"),
	///     Err(e) => println!("Error fetching block: {:?}", e),
	/// }
	/// # }
	/// ```
	///
	/// # Panics
	///
	/// This function does not panic under normal operation. However, it will panic if there's an
	/// issue with converting transaction details to the expected format.
	///
	/// # Safety
	///
	/// This function is generally safe to use but relies on correct implementation of external
	/// dependencies like `BlockState` and proper error handling of async operations.
	///
	/// # Notes
	///
	/// * This function is an async function and requires `.await` for execution.
	/// * The function currently has a placeholder for including transactions in the block, which is
	///   not implemented.
	async fn get_block_by_number(
		&self,
		block_number: BlockNumber,
		_include_tx: bool, // FIXME: optionally include the txn
	) -> Result<Option<Block>, ErrorObjectOwned> {
		let db_pool_conn = Database::get_pool_connection()
			.await
			.map_err(|e| error_object!(&format!("Failed to get db_pool_conn: {}", e)))?;
		let block_state = BlockState::new(&db_pool_conn)
			.await
			.map_err(|e| error_object!(&format!("Failed to start BlockState due to {e}")))?;
		let block_number = match block_number {
			BlockNumber::Earliest => 0,
			BlockNumber::Latest => {
				let chain_state = block_state
					.load_chain_state(self.service.node.cluster_address)
					.await
					.map_err(|e| error_object!(&format!("Failed to load block due to {e}")))?;
				chain_state.block_number
			},
			BlockNumber::Num(x) => x as u128,
			_ => return Err(error_object!("Unsupported block number")),
		};

		let block = block_state
			.load_block(block_number, &self.service.node.cluster_address)
			.await
			.map_err(|_| error_object!("Failed to find block number", Some(block_number)))?;

		// FIXME: how to detect lack of block, ie result = None?
		let timestamp = SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.expect("Unable to get timestamp")
			.as_micros();
		let txn_block = Some(Block {
			header: Header {
				hash: Some(block.block_header.block_hash.into()),
				parent_hash: block.block_header.parent_hash.into(),
				gas_used: *DEFAULT_GAS_AMOUNT,
				timestamp: timestamp.into(),
				number: Some((block.block_header.block_number as u64).into()),
				..Header::default()
			},
			// FIXME downcasting
			transactions: block
				.transactions
				.into_iter()
				.map(|t| {
					let from: H160 = TryInto::<[u8; 20]>::try_into(t.verifying_key).unwrap().into();
					Transaction {
						nonce: t.nonce.into(),
						max_fee_per_gas: Some(t.fee_limit.into()), // FIXME: verify
						from,
						..Transaction::default()
					}
				})
				.collect(),
			..Block::default()
		});
		Ok(txn_block)
	}

	/// Retrieves the number of transactions sent from a given address up to a specified block
	/// number.
	///
	/// This function counts the number of transactions sent from the specified address. It allows
	/// an optional block number parameter to specify up to which block the transactions should be
	/// counted. The block number can be a specific number or one of several special values
	/// indicating the earliest, latest, pending, safe, or finalized block.
	///
	/// # Arguments
	///
	/// * `address`: `H160` - The address for which the transaction count is to be retrieved.
	/// * `block_number`: `Option<BlockNumber>` - An optional parameter specifying the block number
	///   up to which transactions should be counted. If `None`, it counts for all blocks.
	///
	/// # Returns
	///
	/// * `Result<U256, ErrorObjectOwned>` - On success, returns the count of transactions as
	///   `U256`. On failure, returns an `ErrorObjectOwned` indicating the cause of the error.
	///
	/// # Errors
	///
	/// * Returns an error if there is a failure in initializing the block state or loading the
	///   chain state.
	/// * Returns an error if the provided block number is unsupported.
	/// * Returns an error if there's an issue fetching the transaction count for the given address.
	///
	/// # Example
	///
	/// ```no_run
	/// # use primitive_types::H160;
	/// # use ethers::types::U256;
	/// # // Mock enum and function for demonstration purposes
	/// # enum BlockNumber {
	/// #     Earliest, Latest,
	/// #     // ... possibly other variants ...
	/// # }
	/// # async fn get_transaction_count(address: H160, block_number: Option<BlockNumber>) -> Result<U256, String> {
	/// #     Ok(U256::from(10)) // Mocked transaction count
	/// # }
	/// # fn main() {
	/// // Assuming valid Ethereum address bytes for demonstration purposes
	/// let address_bytes = [0u8; 20]; // Replace with actual Ethereum address bytes
	/// let address = H160::from_slice(&address_bytes);
	/// let block_number = Some(BlockNumber::Latest); // Example block number
	/// let transaction_count = tokio::runtime::Runtime::new().unwrap().block_on(async {
	///     get_transaction_count(address, block_number).await
	/// });
	///
	/// match transaction_count {
	///     Ok(count) => println!("Number of transactions: {}", count),
	///     Err(e) => println!("Error getting transaction count: {:?}", e),
	/// }
	/// # }
	/// ```
	///
	/// # Safety
	///
	/// This function is generally safe to use but relies on correct implementation of external
	/// dependencies like `BlockState` and proper error handling of async operations.
	///
	/// # Notes
	///
	/// * This function is an async function and requires `.await` for execution.
	/// * The function caters to different scenarios based on the block number provided.
	async fn get_transaction_count(
		&self,
		address: H160,
		block_number: Option<BlockNumber>,
	) -> Result<U256, ErrorObjectOwned> {
		let db_pool_conn = Database::get_pool_connection()
			.await
			.map_err(|e| error_object!(&format!("Failed to get db_pool_conn: {}", e)))?;
		let block_state = BlockState::new(&db_pool_conn)
			.await
			.map_err(|e| error_object!(&format!("Failed to start BlockState due to {e}")))?;

		let block_number = if let Some(block_number) = block_number {
			let block_number = match block_number {
				BlockNumber::Earliest => 0,
				BlockNumber::Latest |
				BlockNumber::Pending |
				BlockNumber::Safe |
				BlockNumber::Finalized => {
					let chain_state = block_state
						.load_chain_state(self.service.node.cluster_address)
						.await
						.map_err(|e| error_object!(&format!("Failed to load block due to {e}")))?;
					chain_state.block_number
				},
				BlockNumber::Num(x) => x as u128,
				_x => return Err(error_object!("Unsupported block number")),
			};
			Some(block_number)
		} else {
			None
		};

		let block_state = BlockState::new(&db_pool_conn)
			.await
			.map_err(|e| error_object!(&format!("Failed to start BlockState due to {e}")))?;

		let txn_count =
			block_state.get_transaction_count(&address.into(), block_number).await.unwrap();
		Ok(txn_count.into())
	}

	async fn get_block_transaction_count_by_number(
		&self,
		block: BlockNumber,
	) -> Result<U256, ErrorObjectOwned> {
		let db_pool_conn = Database::get_pool_connection().await.map_err(|e| {
			NodeError::EventFetchError(format!("Failed to get db_pool_conn: {}", e))
		})?;
		let block_state = BlockState::new(&db_pool_conn)
			.await
			.map_err(|e| error_object!(&format!("Failed to start BlockState due to {e}")))?;

		let block_number = match block {
			BlockNumber::Earliest => 0,
			BlockNumber::Latest |
			BlockNumber::Pending |
			BlockNumber::Safe |
			BlockNumber::Finalized => {
				let chain_state = block_state
					.load_chain_state(self.service.node.cluster_address)
					.await
					.map_err(|e| error_object!(&format!("Failed to load block due to {e}")))?;
				chain_state.block_number
			},
			BlockNumber::Num(x) => x as u128,
			_x => return Err(error_object!("Unsupported block number")),
		};

		let block = block_state
			.load_block(block_number, &self.service.node.cluster_address)
			.await
			.unwrap();
		let txn_count = block.transactions.len();
		Ok(txn_count.into())
	}

	/// Retrieves a transaction by its hash.
	///
	/// This function searches for a transaction using its unique hash and returns the transaction's
	/// details if found. It covers various transaction types including native token transfers,
	/// smart contract deployments, function calls, and staking/unstaking transactions.
	///
	/// # Arguments
	///
	/// * `hash`: `H256` - The hash of the transaction to be retrieved.
	///
	/// # Returns
	///
	/// * `Result<Option<Transaction>, ErrorObjectOwned>` - On success, returns an
	///   `Option<Transaction>` containing the transaction details if the transaction is found.
	///   Returns `None` if the transaction is not found. On failure, returns an `ErrorObjectOwned`
	///   indicating the cause of the error.
	///
	/// # Errors
	///
	/// * Returns an error if there is a failure in initializing the block state or fetching the
	///   transaction.
	/// * Returns an error if the transaction hash, block hash, or other required details cannot be
	///   properly processed.
	/// * Returns an error if the transaction type does not provide necessary information like value
	///   or recipient address.
	///
	/// # Example
	///
	/// ```no_run
	/// # use ethers::types::H256;
	/// # #[derive(Debug)] // Implementing the Debug trait
	/// # struct Transaction { /* transaction details */ }
	/// # async fn transaction_by_hash(hash: H256) -> Result<Option<Transaction>, String> { Ok(Some(Transaction { /* details */ })) }
	/// # fn main() {
	/// // Assuming valid transaction hash bytes for demonstration purposes
	/// let transaction_hash_bytes = [0u8; 32]; // Replace with actual transaction hash bytes
	/// let transaction_hash = H256::from_slice(&transaction_hash_bytes);
	/// let transaction = tokio::runtime::Runtime::new().unwrap().block_on(async {
	///     transaction_by_hash(transaction_hash).await
	/// });
	///
	/// match transaction {
	///     Ok(Some(tx)) => println!("Transaction details: {:?}", tx),
	///     Ok(None) => println!("Transaction not found"),
	///     Err(e) => println!("Error fetching transaction: {:?}", e),
	/// }
	/// # }
	/// ```
	///
	/// # Panics
	///
	/// This function does not panic under normal operation.
	///
	/// # Safety
	///
	/// This function is safe to use as it does not involve any unsafe code blocks.
	///
	/// # Notes
	///
	/// * This function is an async function and requires `.await` for execution.
	/// * The function handles various types of transactions and their specific data requirements.
	async fn transaction_by_hash(
		&self,
		hash: H256,
	) -> Result<Option<Transaction>, ErrorObjectOwned> {
		let db_pool_conn = Database::get_pool_connection()
			.await
			.map_err(|e| error_object!(&format!("Failed to get db_pool_conn: {}", e)))?;
		let block_state = match BlockState::new(&db_pool_conn).await {
			Ok(state) => state,
			Err(e) =>
				return Err(error_object!(&format!(
					"Failed to initialize block state because {:?}",
					e
				))),
		};
		let hash_string = format!("{:x}", hash);
		// Convert Rust String to ::prost::alloc::string::String using into()
		let prost_string: ::prost::alloc::string::String = hash_string.into();
		let req = GetTransactionReceiptRequest { hash: prost_string.clone() };
		let tx_receipt = match self.service.get_transaction_receipt(req).await {
			Ok(res) => res,
			Err(e) => return Err(e.into()),
		};
		let response = tx_receipt.transaction;
		let tx_response = match response {
			Some(output) => output,
			None => return Err(error_object!("No transaction data found")),
		};
		let from_hash_bytes = match vec_to_bytes(tx_response.from).await {
			Ok(bytes_data) => bytes_data,
			Err(e) => return Err(e),
		};
		let from = H160::from_slice(&from_hash_bytes.0);
		let txn = match tx_response.transaction {
			Some(tx) => tx,
			None => return Err(error_object!("No txn hash computed")),
		};
		let block_hash_bytes = match vec_to_bytes(tx_response.block_hash).await {
			Ok(bytes_data) => bytes_data,
			Err(e) => return Err(e),
		};
		let block_hash = H256::from_slice(&block_hash_bytes.0);
		let block = block_state
			.load_block(tx_response.block_number as u128, &self.service.node.cluster_address)
			.await
			.map_err(|e| error_object!(&format!("Failed to get block due to {e}")))?;
		let mut transaction_index = None;
		for (index, response) in block.transactions.iter().enumerate() {
			if let Ok(response_hash) = response.transaction_hash() {
				if response_hash == hash.0 {
					transaction_index = Some(U256::from(index as u64));
					break; // Break the loop once the transaction is found
				}
			}
		}
		let nonce = match U256::from_dec_str(&txn.nonce) {
			Ok(output) => output,
			Err(_) => return Err(error_object!("No txn hash computed")),
		};
		let max_fee = match U256::from_dec_str(&txn.fee_limit) {
			Ok(output) => output,
			Err(_) => return Err(error_object!("No txn hash computed")),
		};
		let gas = U256::from(*DEFAULT_GAS_AMOUNT);
		let gas_price = Some(U256::from(*DEFAULT_GAS_PRICE));
		let chain_id = match self.chain_id().await {
			Ok(id) => match id {
				Some(id) => id.as_u64(),
				None => return Err(error_object!("Bad type chain_id found")),
			},
			Err(_) => return Err(error_object!("No chain_id found")),
		};
		// Assuming you have a Signature instance
		let signature = match Signature::from_compact(txn.signature.as_slice()) {
			Ok(sig) => sig,
			Err(_) => return Err(error_object!("Couldn't convert signature bytes")),
		};
		let (value, to, input) = match txn.transaction {
			Some(cipher_rpc::rpc_model::transaction::Transaction::NativeTokenTransfer(data)) => {
				let parsed_amount = data
					.amount
					.parse::<u64>()
					.map_err(|_| error_object!("Failed to parse amount"))?;
				let to_address = match vec_to_bytes(data.address).await {
					Ok(bytes_data) => Some(H160::from_slice(&bytes_data.0)),
					Err(_) => None,
				};
				(Some(parsed_amount), to_address, None)
			},
			Some(cipher_rpc::rpc_model::transaction::Transaction::SmartContractDeployment(
				data,
			)) => (Some(data.value), None, Some(data.contract_code)),
			Some(cipher_rpc::rpc_model::transaction::Transaction::SmartContractInit(_data)) => {
				return Err(error_object!("Init has not supported in tx receipt"));
			},
			Some(cipher_rpc::rpc_model::transaction::Transaction::SmartContractFunctionCall(
				data,
			)) => (None, None, Some(data.arguments)),
			Some(cipher_rpc::rpc_model::transaction::Transaction::Stake(_data)) => {
				return Err(error_object!("Stake not supported in tx receipt"));
			},
			Some(cipher_rpc::rpc_model::transaction::Transaction::Unstake(_data)) => {
				return Err(error_object!("Unstake not supported in tx receipt"));
			},
			None => return Err(error_object!("No value found")),
		};
		let input = match input {
			Some(input) => Bytes::from(input),
			None => return Err(error_object!("No input found")),
		};
		let value = match value {
			Some(val) => U256::from(val),
			None => U256::zero(),
		};
		let v = signature.serialize_compact()[3]; // Use array indexing
		let r = &signature.serialize_compact()[0..32];
		let s = &signature.serialize_compact()[32..64];

		let transaction = Transaction {
			hash,
			nonce,
			block_hash: Some(block_hash),
			block_number: Some(U256::from(tx_response.block_number)),
			transaction_index,
			from,
			to,
			value,
			max_priority_fee_per_gas: Some(max_fee),
			max_fee_per_gas: Some(max_fee),
			gas_price,
			gas,
			input,
			v: Some(U256::from(v)),
			r: U256::from(r),
			s: U256::from(s),
			chain_id: Some(U64::from(chain_id)),
			..Transaction::default()
		};
		Ok(Some(transaction))
	}

	/// Retrieves the transaction receipt for a given transaction hash.
	///
	/// This function fetches the transaction receipt associated with the specified transaction
	/// hash. It includes various details such as the transaction index, block hash, gas used,
	/// contract address, and logs.
	///
	/// # Arguments
	///
	/// * `hash`: `H256` - The hash of the transaction for which the receipt is being requested.
	///
	/// # Returns
	///
	/// * `Result<Option<TransactionReceipt>, ErrorObjectOwned>` - On success, returns an
	///   `Option<TransactionReceipt>` containing the transaction receipt if found. If the
	///   transaction receipt is not found, it returns `None`. On failure, returns an
	///   `ErrorObjectOwned` indicating the cause of the error.
	///
	/// # Errors
	///
	/// * Returns an error if there is a failure in initializing the block state, fetching the
	///   transaction, or if the transaction receipt is not found.
	/// * Returns an error if the transaction hash, block hash, or other required details cannot be
	///   properly processed.
	/// * Returns an error if there is a failure in fetching events related to the transaction.
	///
	/// # Example
	///
	/// ```no_run
	/// # // Mock function for demonstration purposes
	/// # async fn transaction_receipt(hash: ethers::types::H256) -> Result<Option<String>, String> { Ok(Some("mock_receipt".to_string())) }
	/// # fn main() {
	/// use ethers::types::H256;
	/// // Assuming valid transaction hash bytes for demonstration purposes
	/// let transaction_hash_bytes = [0u8; 32]; // Replace with actual transaction hash bytes
	/// let transaction_hash = H256::from_slice(&transaction_hash_bytes);
	/// let receipt = tokio::runtime::Runtime::new().unwrap().block_on(async {
	///     transaction_receipt(transaction_hash).await
	/// });
	///
	/// match receipt {
	///     Ok(Some(receipt)) => println!("Transaction Receipt: {:?}", receipt),
	///     Ok(None) => println!("No receipt found for the transaction"),
	///     Err(e) => println!("Error fetching transaction receipt: {:?}", e),
	/// }
	/// # }
	/// ```
	///
	/// # Panics
	///
	/// This function does not panic under normal operation.
	///
	/// # Safety
	///
	/// This function is safe to use as it does not involve any unsafe code blocks.
	///
	/// # Notes
	///
	/// * This function is an async function and requires `.await` for execution.
	/// * The function covers various scenarios including native token transfers, smart contract
	///   deployments, function calls, and staking/unstaking transactions.
	async fn transaction_receipt(
		&self,
		hash: H256,
	) -> Result<Option<TransactionReceipt>, ErrorObjectOwned> {
		let db_pool_conn = Database::get_pool_connection()
			.await
			.map_err(|e| error_object!(&format!("Failed to get db_pool_conn: {}", e)))?;
		let block_state = match BlockState::new(&db_pool_conn).await {
			Ok(res) => res,
			Err(e) =>
				return Err(error_object!(&format!(
					"Failed to initialize block state because {:?}",
					e
				))),
		};
		let hash_string = format!("{:x}", hash);
		// Convert Rust String to ::prost::alloc::string::String using into()
		let prost_string: ::prost::alloc::string::String = hash_string.into();
		let req = GetTransactionReceiptRequest { hash: prost_string.clone() };
		let tx_receipt = match self.service.get_transaction_receipt(req).await {
			Ok(res) => res,
			Err(e) => return Err(e.into()),
		};
		let response = tx_receipt.transaction;
		let tx_response = match response {
			Some(output) => output,
			None => return Err(error_object!("No transaction data found")),
		};
		// let txn = match tx_response.transaction{
		// 	Some(output) => output.transaction,
		// 	None => return Err(error_object!("No transaction data found")),
		// };
		let block_hash_bytes = match vec_to_bytes(tx_response.block_hash).await {
			Ok(bytes_data) => bytes_data,
			Err(e) => return Err(e),
		};
		let block_hash = H256::from_slice(&block_hash_bytes.0);
		let fees_used = match U256::from_dec_str(&tx_response.fee_used) {
			Ok(fee) => fee,
			Err(e) => return Err(error_object!(&format!("Fee used not got because: {:?}", e))),
		};
		let tx_hash_bytes = match vec_to_bytes(tx_response.transaction_hash).await {
			Ok(bytes_data) => bytes_data,
			Err(e) => return Err(e),
		};
		let block = block_state
			.load_block(tx_response.block_number as u128, &self.service.node.cluster_address)
			.await
			.map_err(|e| error_object!(&format!("Failed to get block due to {e}")))?;
		let mut transaction_index = None;
		for (index, response) in block.transactions.iter().enumerate() {
			if let Ok(response_hash) = response.transaction_hash() {
				if response_hash == hash.0 {
					transaction_index = Some(U256::from(index as u64));
					break; // Break the loop once the transaction is found
				}
			} else {
				// Optionally handle the error from transaction_hash() if necessary
				NodeError::ParseError(format!("Transaction hash incorrect/transaction not found"));
			}
		}

		// Handle the case where the transaction was not found
		if transaction_index.is_none() {
			return Err(error_object!(&format!("Transaction not found")));
		}
		// After the loop, check if the transaction was found
		let index = if let Some(index) = transaction_index {
			index
		} else {
			// Handle the case where the transaction was not found
			return Err(error_object!(&format!("Transaction not found")));
		};
		let transaction_hash = H256::from_slice(&tx_hash_bytes.0);
		let from_hash_bytes = match vec_to_bytes(tx_response.from).await {
			Ok(bytes_data) => bytes_data,
			Err(e) => return Err(e),
		};
		let from = H160::from_slice(&from_hash_bytes.0);
		let event_req = GetEventsRequest { tx_hash: prost_string, timestamp: 0 };
		let response = match self.service.get_events(event_req).await {
			Ok(res) => res,
			Err(e) => return Err(e.into()),
		};
		let result = match response.into_inner().recv().await {
			Some(Ok(res)) => res,
			Some(Err(e)) =>
				return Err(error_object!(&format!(
					"Failed to get send raw transaction response due to {e}"
				)))?,
			None => return Err(error_object!("No response found")),
		};
		let events: Vec<Vec<u8>> = result.events_data;
		let mut logs = Vec::new();
		for event in events {
			match serde_json::from_slice::<Value>(&event) {
				Ok(output) => {
					match convert_ethereum_log(output).await {
						Ok(log) => {
							let data = Log {
								address: log.address,
								topics: log.topics,
								data: log.data,
								block_hash: Some(block_hash),
								block_number: Some(tx_response.block_number.into()),
								transaction_hash: log.transaction_hash,
								transaction_index,
								log_index: log.log_index,
								logs_bloom: log.logs_bloom,
								..Log::default()
							};
							logs.push(data);
						},
						Err(e) => {
							log::error!("Error converting Ethereum log: {}", e);
							// Handle the error, e.g., continue to the next event, return an error,
							// etc.
							continue;
						},
					}
				},
				Err(e) => {
					log::error!("Error deserializing event: {}", e);
					// Handle the error, e.g., continue to the next event, return an error, etc.
					continue;
				},
			}
		}
		let mut address: Option<H160> = None;
		for log in logs.clone() {
			if log.transaction_hash == Some(hash) {
				address = Some(log.address)
			}
		}

		let receipt = TransactionReceipt {
			transaction_hash: Some(transaction_hash),
			transaction_index: Some(index),
			block_hash: Some(block_hash),
			block_number: Some((tx_response.block_number as u64).into()),
			from: Some(from),
			gas_used: Some(fees_used),
			contract_address: address,
			logs,
			..TransactionReceipt::default()
		};
		Ok(Some(receipt))
	}

	/// Sends a transaction to the network.
	///
	/// This function handles the creation and submission of a transaction based on the provided
	/// `TransactionRequest`. It supports sending both smart contract deployment and function call
	/// transactions.
	///
	/// # Arguments
	///
	/// * `request`: `TransactionRequest` - A struct containing the details of the transaction to be
	///   sent. This includes the sender, recipient, value, data, and other transaction-related
	///   information.
	///
	/// # Returns
	///
	/// * `Result<H256, ErrorObjectOwned>` - On success, returns the hash (`H256`) of the processed
	///   transaction. On failure, returns an `ErrorObjectOwned` with details of the error.
	///
	/// # Errors
	///
	/// * Returns an error if it fails to initialize the account state.
	/// * Returns an error if no signer is available or no suitable signer is found for the
	///   transaction.
	/// * Returns an error if fetching the nonce or the chain ID fails.
	/// * Returns an error if the transaction type is not Legacy.
	/// * Returns an error if the transaction fails to be sent to the mempool or is not included in
	///   a block.
	/// * Returns an error if the contract address is invalid or if the data in the request is
	///   insufficient for a function call.
	///
	/// # Example
	///
	/// ```no_run
	/// # // Mock struct and function for demonstration purposes
	/// # struct TransactionRequest {
	/// #     sender: String,
	/// #     recipient: String,
	/// #     value: u64,
	/// # }
	/// # async fn send_transaction(request: TransactionRequest) -> Result<String, String> { Ok("mock_hash".to_string()) }
	/// # fn main() {
	/// let transaction_request = TransactionRequest {
	///     sender: "0xSenderAddress".to_string(),
	///     recipient: "0xRecipientAddress".to_string(),
	///     value: 1000,
	/// };
	/// let result = tokio::runtime::Runtime::new().unwrap().block_on(async {
	///     send_transaction(transaction_request).await
	/// });
	///
	/// match result {
	///     Ok(hash) => println!("Transaction sent successfully. Hash: {:?}", hash),
	///     Err(e) => println!("Error sending transaction: {:?}", e),
	/// }
	/// # }
	/// ```
	///
	/// # Panics
	///
	/// This function does not panic under normal operation.
	///
	/// # Safety
	///
	/// This function is safe to use as it does not involve any unsafe code blocks.
	///
	/// # Notes
	///
	/// * This function is an async function and requires `.await` for execution.
	/// * It handles both deployment of new smart contracts and calling functions on existing
	///   contracts, determined by the presence or absence of a recipient address in the request.
	async fn send_transaction(
		&self,
		request: TransactionRequest,
	) -> Result<H256, ErrorObjectOwned> {
		let db_pool_conn = Database::get_pool_connection()
			.await
			.map_err(|e| error_object!(&format!("Failed to get db_pool_conn: {}", e)))?;
		let account_state = AccountState::new(&db_pool_conn)
			.await
			.map_err(|e| error_object!(&format!("Failed to initialize AccountState due to {e}")))?;

		// if you need to get the first account from the signers if request.from is None
		let from = if let Some(from_address) = request.from {
			from_address
		} else {
			self.service
				.signers
				.as_ref()
				.and_then(|signers| {
					signers.first().and_then(|signer| signer.accounts().first().cloned())
				})
				.ok_or_else(|| error_object!("no signer available"))?
		};

		let to = match request.to {
			Some(a) => Some(a),
			None => None,
		};

		//Determine the nonce
		let nonce = match request.nonce {
			Some(n) => U256::from(n + 1),
			None => {
				let account = account_state.get_account(&from.0).await.map_err(|e| {
					NodeError::AccountFetchError(format!("Account does not exist: {}", e))
				})?;
				U256::from(account.nonce + 1)
			},
		};

		// TODO: we need an oracle to fetch the gas price of the current chain
		let gas_price = *DEFAULT_GAS_PRICE;
		let gas_limit = *DEFAULT_GAS_AMOUNT;

		let contract_state = match ContractState::new(&db_pool_conn).await {
			Ok(state) => state,
			Err(_) =>
				return Err(error_object!("failed to intialize contract state in send transaction")),
		};

		let secret_key = self
			.service
			.signers
			.as_ref()
			.ok_or_else(|| error_object!("no signers configured"))?
			.iter()
			.find(|signer| signer.accounts().contains(&from))
			.ok_or_else(|| error_object!("no suitable signer found"))?
			.secret_key(&from)
			.ok_or_else(|| error_object!("no secret key found"))?;

		let secp = Secp256k1::new();
		let verifying_key = secret_key.public_key(&secp);
		let val = match request.value {
			Some(v) => v,
			None => U256::zero(),
		};
		let value = u256_to_balance(&val).unwrap_or(0);
		let tx_type = if request.to.is_some() &&
			contract_state.is_valid_contract(&request.to.unwrap().into()).await.is_ok()
		{
			let data = request.data.ok_or_else(|| error_object!("No data found in the request"))?;

			let data = data.into_vec();

			if data.len() < 4 {
				return Err(error_object!("Data too short for a function"));
			}
			cipher_rpc::rpc_model::submit_transaction_request::TransactionType::SmartContractFunctionCall (
				cipher_rpc::rpc_model::SmartContractFunctionCall{
					contract_address: to.unwrap().as_bytes().to_vec(),
					function_name: "".as_bytes().to_vec(),
					arguments: data,
				}
			)
		} else if request.to.is_none() && request.data.is_some() {
			let data = request.data.ok_or_else(|| error_object!("No data found in the request"))?;
			cipher_rpc::rpc_model::submit_transaction_request::TransactionType::SmartContractDeployment(
				cipher_rpc::rpc_model::SmartContractDeployment {
					access_type: 1i32,
					contract_type: 1i32,
					contract_code: data.into_vec(),
					value: value as u64,
					salt: [0; 32].to_vec(),
				},
			)
		} else {
			return Err(error_object!("Failed to initialize contract"))
		};

		let req = SubmitTransactionRequest {
			nonce: nonce.to_string(),
			fee_limit: gas_limit.to_string(),
			signature: match cipher_rpc::sign(
				secret_key.to_owned(),
				tx_type.clone(),
				gas_price.as_u128(),
				nonce.as_u128(),
			) {
				Ok(sig) => sig,
				Err(_) => return Err(error_object!("Failed to sign transaction")),
			},
			verifying_key: verifying_key.serialize().to_vec(),
			transaction_type: Some(tx_type),
		};

		let transaction = match self.service.submit_transaction(req).await {
			Ok(res) => match res.into_inner().recv().await {
				Some(output) => match output {
					Ok(res) => res,
					Err(e) =>
						return Err(error_object!(format!(
							"Failed to get transaction response due to {:?}",
							e
						))),
				},
				None => return Err(error_object!("No transaction data foun")),
			},
			Err(e) => return Err(e.into()),
		};

		let hash = match H256::from_str(&transaction.hash) {
			Ok(output) => output,
			Err(e) =>
				return Err(error_object!(format!(
					"Failed to get transaction hash because of {:?}",
					e
				))),
		};
		// After sending the transaction to the mempool, start monitoring it
		let is_included = self.service.monitor_transaction_inclusion(hash).await?;

		// If the transaction is not included, return an error otherwise create hash and return it
		if !is_included {
			Err(error_object!(&format!("Transaction has been dropped or not included in a block")))
		} else {
			Ok(hash)
		}
	}

	/// Sends a raw transaction to the network.
	///
	/// This function takes a raw transaction in the form of bytes, decodes it into an Ethereum
	/// TransactionV2, and sends it to the mempool for inclusion in a block. It supports only Legacy
	/// transactions.
	///
	/// # Arguments
	///
	/// * `bytes`: `Bytes` - A byte array representing the raw transaction data. This data should be
	///   in the format of an Ethereum transaction.
	///
	/// # Returns
	///
	/// * `Result<H256, ErrorObjectOwned>` - On success, returns the hash (`H256`) of the processed
	///   transaction. On failure, returns an `ErrorObjectOwned` indicating the cause of the error.
	///
	/// # Errors
	///
	/// * Returns an error if the input byte array is empty.
	/// * Returns an error if decoding the transaction fails, indicating a parsing issue.
	/// * Returns an error if the transaction type is not a Legacy transaction.
	/// * Returns an error if extracting the signature and public key from the transaction fails.
	/// * Returns an error if sending the transaction to the mempool fails.
	/// * Returns an error if the transaction is not included in a block.
	///
	/// # Example
	///
	/// ```no_run
	/// # // Mock function for demonstration purposes
	/// # async fn send_raw_transaction(bytes: &[u8]) -> Result<String, String> { Ok("mock_hash".to_string()) }
	/// # fn main() {
	/// use ethers::types::Bytes;
	/// // Assuming the transaction bytes are valid for the demonstration
	/// let transaction_bytes = "f86c018502540be40082520894095e7baea6a6c7c4c2dfeb977efac326af552d87038d7ea4c680008026a0a6b3a5c6f6f0e1b9b2c77e4e5c14c48e9e7f4e74ecdd8d7f2bb8ca2f5e5c6a07da0666fbddeea8accb1e6e7a1b7decd1a0734c0b2f57bca0cf3a9f6b461d4f1c50".as_bytes();
	/// let result = tokio::runtime::Runtime::new().unwrap().block_on(async {
	///     send_raw_transaction(&transaction_bytes).await
	/// });
	///
	/// match result {
	///     Ok(hash) => println!("Transaction sent successfully. Hash: {:?}", hash),
	///     Err(e) => println!("Error sending transaction: {:?}", e),
	/// }
	/// # }
	/// ```
	///
	/// # Panics
	///
	/// This function does not panic under normal operation.
	///
	/// # Safety
	///
	/// This function is safe to use as it does not involve any unsafe code blocks.
	///
	/// # Notes
	///
	/// * This function is an async function and requires `.await` for execution.
	/// * The function is intended for use with the EVM, specifically handling Legacy transactions.
	async fn send_raw_transaction(&self, bytes: Bytes) -> Result<H256, ErrorObjectOwned> {
		let bytes = bytes.into_vec();
		// check if bytes are empty
		if bytes.is_empty() {
			return Err(error_object!("transaction data is empty"));
		}
		// decode bytes into ethereum TransactionV2
		let transaction: ethereum::TransactionV2 = ethereum::EnvelopedDecodable::decode(&bytes)
			.map_err(|e| {
				NodeError::ParseError(format!("Failed to parse log ethereum Transaction v2: {e:?}"))
			})?;
		if let ethereum::TransactionV2::Legacy(legacy_txn) = transaction {
			let legacy_txn2 = legacy_txn.clone();
			// convert value to primitive balance type
			let value = u256_to_balance(&legacy_txn.value).unwrap_or(0);
			// create a new transaction type
			let tx_type = system::transaction::TransactionType::SmartContractDeployment {
				access_type: system::access::AccessType::PRIVATE,
				contract_type: system::contract::ContractType::EVM,
				contract_code: legacy_txn.clone().input,
				value,
				salt: [0; 32].to_vec(),
			};

			// extract the signature and public key from the transaction
			// https://github.com/paritytech/frontier/blob/master/frame/ethereum/src/lib.rs#L373
			let (signature, pub_key): (secp256k1::ecdsa::Signature, secp256k1::PublicKey) = {
				let mut msg = [0u8; 32];
				let mut sig = [0u8; 65];
				sig[0..32].copy_from_slice(&legacy_txn.signature.r()[..]);
				sig[32..64].copy_from_slice(&legacy_txn.signature.s()[..]);
				sig[64] = legacy_txn.signature.standard_v();
				msg.copy_from_slice(
					&ethereum::LegacyTransactionMessage::from(legacy_txn2.clone()).hash()[..],
				);

				let rid =
					RecoveryId::from_i32(if sig[64] > 26 { sig[64] - 27 } else { sig[64] } as i32)
						.map_err(|_| error_object!("BadV"))?;
				let sig = secp256k1::ecdsa::RecoverableSignature::from_compact(&sig[..64], rid)
					.map_err(|_| error_object!("BadRS"))?;
				let msg = secp256k1::Message::from_slice(&msg)
					.map_err(|_| error_object!("Message is 32 bytes; qed"))?;
				let pub_key = SECP256K1
					.recover_ecdsa(&msg, &sig)
					.map_err(|_| error_object!("Bad Signature"))?;
				(sig.to_standard(), pub_key)
			};

			// Create new transaction
			let transaction = system::transaction::Transaction::new(
				legacy_txn.nonce.as_u128() + 1,
				tx_type,
				legacy_txn.gas_limit.as_u128(),
				signature,
				pub_key,
			);

			// Sending the transaction to the mempool
			self.service
				.node
				.mempool_tx
				.send(system::mempool::ProcessMempool::AddTransaction(transaction.clone()))
				.await
				.map_err(|e| {
					error_object!(&format!("Failed to send transaction to mempool due to {e}"))
				})?;

			let hash: H256 = match transaction.transaction_hash() {
				Ok(output) => output.into(),
				Err(e) =>
					return Err(error_object!(&format!(
						"Failed to convert hash in send raw txn due to {:?}",
						e
					))),
			};

			// After sending the transaction to the mempool, start monitoring it
			let is_included = self.service.monitor_transaction_inclusion(hash).await?;

			// If the transaction is not included, return an error otherwise create hash and return
			// it
			if !is_included {
				Err(error_object!(&format!(
					"Transaction has been dropped or not included in a block"
				)))
			} else {
				Ok(hash)
			}
		} else {
			Err(error_object!("Transaction type not Legacy"))
		}
	}
}

async fn subscribe_future_events(
	node_evm_event_tx: &broadcast::Sender<EventBroadcast>,
	sink: SubscriptionSink,
	params: Option<Params>,
	log_tx: Option<mpsc::Sender<Log>>, // for testing purposes
) -> anyhow::Result<()> {
	let (param_addresses, param_topics, from_block, to_block) = match params {
		Some(Params::Logs(filter)) => {
			let addresses = filter.address.to_opt_vec().unwrap_or(vec![]);
			// Topics described here: https://docs.ethers.org/v5/concepts/events/
			// Topic, supports `A` | `null` | `[A,B,C]` | `[A,[B,C]]` | `[null,[B,C]]` |
			// `[null,[null,C]]`
			let topics = filter
				.topics
				.into_iter()
				.map(|topics| match topics.unwrap().to_opt_vec() {
					Some(topics) if topics.contains(&None) => None,
					Some(topics) =>
						Some(topics.into_iter().map(|x| x.unwrap()).collect::<Vec<_>>()),
					None => None,
				})
				.collect::<Vec<_>>();
			(addresses, topics, filter.from_block, filter.to_block)
		},
		_ => (vec![], vec![], Some(BlockNumber::Num(u64::MIN)), Some(BlockNumber::Num(u64::MAX))),
	};
	// receive the latest events and stream them
	let mut evm_event_rx = node_evm_event_tx.subscribe();

	loop {
		match time::timeout(time::Duration::from_millis(100), async { evm_event_rx.recv() }).await {
			Ok(fut_res) => {
				match fut_res.await {
					Ok(EventBroadcast::Evm(
						address,
						topics,
						data,
						block_number,
						block_hash,
						txn_hash,
						txn_index,
						log_index,
					)) => {
						// entry criteria
						let prior_to_block = to_block.map_or(true, |to| to.gte(block_number));

						if !prior_to_block {
							debug!("not reached from block number, unsubscribing");
							break
						}

						// exit criteria
						let after_from_block =
							from_block.map_or(true, |from| from.lte(block_number));
						let address_match_criteria = param_addresses.contains(&address);
						let topic_match_criteria = topics
							.iter()
							.zip(param_topics.iter())
							.all(|(x, y)| y.as_ref().map_or(true, |z| z.contains(&x)));

						if after_from_block && address_match_criteria && topic_match_criteria {
							let log = Log {
								address,
								topics,
								data: data.into(),
								block_hash: Some(block_hash.into()),
								block_number: Some(
									TryInto::<u64>::try_into(block_number).unwrap().into(),
								),
								transaction_hash: Some(txn_hash.into()),
								transaction_index: Some(txn_index.into()),
								log_index: Some(log_index.into()),
								logs_bloom: None,
								transaction_log_index: None,
								removed: false,
							};

							let json_msg = &serde_json::to_value(&log)?;
							sink.send(SubscriptionMessage::from_json(json_msg)?).await?;

							// for testing purposes
							match log_tx {
								Some(ref log_tx) => {
									log_tx.send(log).await?;
								},
								_ => {},
							}
						}
					},
					Err(RecvError::Lagged(x)) => {
						debug!("receiver lagging {x}, continuing")
					},
					Err(RecvError::Closed) => {
						debug!("receiver closed, subscription revoked");
						break
					},
				}
			},
			Err(elapsed) => {
				debug!("elapsed, continuing {:?}", elapsed);
			},
		}
	}

	Ok(())
}

#[tonic::async_trait]
impl FullNodeJsonServer for FullNodeJsonImpl {
	async fn get_account_state(
		&self,
		request: GetAccountStateRequest,
	) -> Result<GetAccountStateResponse, ErrorObjectOwned> {
		Ok(self.service.get_account_state(request).await?)
	}

	async fn submit_transaction(
		&self,
		request: SubmitTransactionRequest,
	) -> Result<SubmitTransactionResponse, ErrorObjectOwned> {
		let result = self.service.submit_transaction(request).await.map_err(|e| {
			println!("ORIGINAL Error in submit transaction: {:?}", e);
			error_object!(&format!("Failed to get send raw transaction response due to {e}"))
		})?;
		// println!("SUBMIT TX result: {:?}", result.into_inner());
		let response = match result.into_inner().recv().await {
			Some(Ok(res)) => res,
			Some(Err(e)) =>
				return Err(error_object!(&format!(
					"Failed to get send raw transaction response due to {e}"
				)))?,
			None => return Err(error_object!("No response found")),
		};
		Ok(response)
	}

	async fn get_transaction_receipt(
		&self,
		request: GetTransactionReceiptRequest,
	) -> Result<GetTransactionReceiptResponse, ErrorObjectOwned> {
		Ok(self.service.get_transaction_receipt(request).await?)
	}

	async fn get_transactions_by_account(
		&self,
		request: GetTransactionsByAccountRequest,
	) -> Result<GetTransactionsByAccountResponse, ErrorObjectOwned> {
		Ok(self.service.get_transactions_by_account(request).await?)
	}

	async fn smart_contract_read_only_call(
		&self,
		request: SmartContractReadOnlyCallRequest,
	) -> Result<SmartContractReadOnlyCallResponse, ErrorObjectOwned> {
		Ok(self.service.smart_contract_read_only_call(request).await?)
	}

	async fn get_chain_state(
		&self,
		request: GetChainStateRequest,
	) -> Result<GetChainStateResponse, ErrorObjectOwned> {
		Ok(self.service.get_chain_state(request).await?)
	}

	async fn get_block_by_number(
		&self,
		request: GetBlockByNumberRequest,
	) -> Result<GetBlockByNumberResponse, ErrorObjectOwned> {
		Ok(self.service.get_block_by_number(request).await?)
	}

	async fn get_latest_block_headers(
		&self,
		pending: PendingSubscriptionSink,
		request: GetLatestBlockHeadersRequest,
	) -> SubscriptionResult {
		let db_pool_conn = Database::get_pool_connection()
			.await
			.map_err(|e| NodeError::DBError(format!("Failed to get db_pool_conn: {}", e)))?;
		let sink = pending.accept().await?;

		BlockState::new(&db_pool_conn).await.map_err(|e| {
			NodeError::DBError(format!("Failed to access BlockState database: {}", e))
		})?;

		let num_headers: u32 = request.number_of_blocks;
		let headers_per_page: usize = request.blocks_per_page.try_into().unwrap_or(usize::MAX);

		let block_headers = self.service.get_latest_block_headers(num_headers).await?;

		let mut chunks = block_headers.chunks(headers_per_page);
		let mut page_number: u32 = 1;
		while let Some(chunk) = chunks.next().clone() {
			let headers = chunk.to_vec();
			let tx_response = GetLatestBlockHeadersResponse { page_number, page: headers };
			let response = SubscriptionMessage::from_json(&tx_response)?;
			sink.send(response).await?;

			page_number += 1;
		}

		Ok(())
	}

	/// Receive the given number of latest transactions in a paged response format
	async fn get_latest_transactions(
		&self,
		pending: PendingSubscriptionSink,
		request: GetLatestTransactionsRequest,
	) -> SubscriptionResult {
		let sink = pending.accept().await?;

		let db_pool_conn = Database::get_pool_connection()
			.await
			.map_err(|e| NodeError::DBError(format!("Failed to get db_pool_conn: {}", e)))?;
		let block_state = BlockState::new(&db_pool_conn).await.map_err(|e| {
			NodeError::DBError(format!("Failed to access BlockState database: {}", e))
		})?;

		let num_transactions: u32 = request.number_of_transactions;
		let tx_per_page: usize = request.transactions_per_page.try_into().unwrap_or(usize::MAX);

		let transactions =
			block_state.load_latest_transactions(num_transactions).await.map_err(|e| {
				NodeError::BlockFetchError(format!("Failed to get latest transactions: {}", e))
			})?;

		let mut chunks = transactions.chunks(tx_per_page);
		let mut page_number: u32 = 1;
		while let Some(chunk) = chunks.next().clone() {
			let txs = chunk.to_vec();
			let tx_response = GetLatestTransactionsResponse { page_number, page: txs };
			let response = SubscriptionMessage::from_json(&tx_response)?;
			sink.send(response).await?;

			page_number += 1;
		}

		Ok(())
	}

	async fn get_stake(
		&self,
		request: GetStakeRequest,
	) -> Result<GetStakeResponse, ErrorObjectOwned> {
		Ok(self.service.get_stake(request).await?)
	}

	async fn get_current_nonce(
		&self,
		request: GetCurrentNonceRequest,
	) -> Result<GetCurrentNonceResponse, ErrorObjectOwned> {
		Ok(self.service.get_current_nonce(request).await?)
	}

	async fn get_events(
		&self,
		request: GetEventsRequest,
	) -> Result<GetEventsResponse, ErrorObjectOwned> {
		let response = match self.service.get_events(request).await {
			Ok(res) => res,
			Err(e) => return Err(e.into()),
		};
		let result = match response.into_inner().recv().await {
			Some(Ok(res)) => res,
			Some(Err(e)) =>
				return Err(error_object!(&format!(
					"Failed to get send raw transaction response due to {e}"
				)))?,
			None => return Err(error_object!("No response found")),
		};
		Ok(result)
	}

	/// Subscribes to events via jrpc
	///
	/// Usage:
	/// ```sh
	/// wscat -c ws://127.0.0.1:50051
	/// Connected (press CTRL+C to quit)
	/// > {"id":1,"jsonrpc":"2.0","method":"cipher_subscribeEvents","params":[]}
	/// < {"jsonrpc":"2.0","result":6058624328756215,"id":1}
	///
	/// ...send an event from cli dir:
	/// ../target/debug/cli --private-key $PRIV_KEY submit-txn --payload-file-path txn-payload/native_token_transfer.json
	///
	/// ...and see the event in the wscat
	/// < {"jsonrpc":"2.0","method":"cipher_subscribeEvents","params":{"subscription":6058624328756215,"result":...msg...}}
	/// ```
	async fn subscribe_events(
		&self,
		pending: PendingSubscriptionSink,
		_kind: Kind,
		_params: Option<Params>,
	) -> SubscriptionResult {
		let sink = pending.accept().await?;

		let mut node_event_tx = self.service.node.node_event_tx.subscribe();
		while let Ok(event) = node_event_tx.recv().await {
			let event_str = serde_json::from_slice::<Value>(&event)
				.unwrap_or_else(|_| json!({ "error": format!("Unparsable event {:?}", &event) }));
			let event = SubscriptionMessage::from_json(&event_str)?;
			sink.send(event).await?;
		}
		Ok(())
	}
}

/*pub async fn run_server(
	address: String,
	service: FullNodeService,
	mempool_res_rx: mpsc::Receiver<ResponseMempool>,
) -> anyhow::Result<SocketAddr> {
	// Add a CORS middleware for handling HTTP requests.
	// This middleware does affect the response, including appropriate
	// headers to satisfy CORS. Because any origins are allowed, the
	// "Access-Control-Allow-Origin: *" header is appended to the response.
	let cors = CorsLayer::new()
		// Allow `POST` when accessing the resource
		.allow_methods([Method::POST, Method::GET].into())
		// Allow requests from any origin
		.allow_origin(Any)
		.allow_headers([tonic::codegen::http::header::CONTENT_TYPE].into());
	let middleware = tower::ServiceBuilder::new().layer(cors);

	// The RPC exposes the access control for filtering and the middleware for
	// modifying requests / responses. These features are independent of one another
	// and can also be used separately.
	// In this example, we use both features.
	// TODO: Extract this to a builder function
	let server = Server::builder()
		.max_connections(MAX_CONNECTIONS)
		.set_middleware(middleware)
		.build(address.parse::<SocketAddr>()?)
		.await?;

	let addr = server.local_addr()?;

	info!("JSON RPC on {}", addr);

	let (mempool_json_evm_tx, mempool_json_evm_rx) = mpsc::channel(1000);
	let (mempool_json_tx, mempool_json_rx) = mpsc::channel(1000);

	let rpc_server_evm_impl =
		FullNodeJsonImpl { service: service.clone(), mempool_json_rx: mempool_json_evm_rx };
	let rpc_server_impl = FullNodeJsonImpl { service, mempool_json_rx };
	task::spawn(mempool_response(mempool_res_rx, mempool_json_evm_tx, mempool_json_tx));
	let mut methods = Methods::new();
	let eth_methods: Methods = EvmCompatibilityServer::into_rpc(rpc_server_evm_impl).into();
	let cipher_methods: Methods = FullNodeJsonServer::into_rpc(rpc_server_impl).into();

	methods.merge(eth_methods)?;
	methods.merge(cipher_methods)?;

	let handle = server.start(methods);

	// Runs forever!
	let _ = tokio::spawn(handle.stopped()).await;

	Ok(addr)
}*/

pub async fn run_server(
	address: String,
	service: FullNodeService,
	mempool_res_rx: mpsc::Receiver<ResponseMempool>,
) -> anyhow::Result<SocketAddr> {
	let addr = address.parse::<SocketAddr>()?;
	let (stop_handle, server_handle) = stop_channel();
	let svc_builder = jsonrpsee_server::Server::builder()
		.max_connections(MAX_CONNECTIONS)
		.to_service_builder();
	let methods = Methods::new();
	let stop_handle2 = stop_handle.clone();

	let make_service = make_service_fn(move |_conn: &AddrStream| {
		// You may use `conn` or the actual HTTP request to get connection related details.
		let stop_handle = stop_handle2.clone();
		let svc_builder = svc_builder.clone();
		let methods = methods.clone();

		async move {
			Ok::<_, Box<dyn StdError + Send + Sync>>(service_fn(move |req| {
				let stop_handle = stop_handle.clone();
				let svc_builder = svc_builder.clone();
				let methods = methods.clone();
				let mut svc = svc_builder.build(methods, stop_handle);

				// It's not possible to know whether the websocket upgrade handshake failed or not
				// here.
				let is_websocket = ws::is_upgrade_request(&req);

				if is_websocket {
					println!("websocket")
				} else {
					println!("http")
				}

				/// Call the jsonrpsee service which
				/// may upgrade it to a WebSocket connection
				/// or treat it as "ordinary HTTP request".
				svc.call(req)
			}))
		}
	});

	let server = Server::bind(&addr).serve(make_service);

	let (mempool_json_evm_tx, mempool_json_evm_rx) = mpsc::channel(1000);
	let (mempool_json_tx, mempool_json_rx) = mpsc::channel(1000);

	let rpc_server_evm_impl =
		FullNodeJsonImpl { service: service.clone(), mempool_json_rx: mempool_json_evm_rx };
	let rpc_server_impl = FullNodeJsonImpl { service, mempool_json_rx };
	task::spawn(mempool_response(mempool_res_rx, mempool_json_evm_tx, mempool_json_tx));
	let mut methods = Methods::new();
	let eth_methods: Methods = EvmCompatibilityServer::into_rpc(rpc_server_evm_impl).into();
	let cipher_methods: Methods = FullNodeJsonServer::into_rpc(rpc_server_impl).into();

	methods.merge(eth_methods)?;
	methods.merge(cipher_methods)?;

	let handle = server.start(methods);

	// Runs forever!
	let _ = tokio::spawn(handle.stopped()).await;

	tokio::spawn(async move {
		let graceful = server.with_graceful_shutdown(async move { stop_handle.shutdown().await });
		graceful.await.unwrap()
	});

	info!("JSON RPC on {}", address);

	Ok(addr)
}

pub async fn mempool_response(
	mut mempool_rx: mpsc::Receiver<ResponseMempool>,
	mempool_json_evm_tx: mpsc::Sender<ResponseMempool>,
	mempool_json_tx: mpsc::Sender<ResponseMempool>,
) {
	while let Some(mempool_response) = mempool_rx.recv().await {
		if let Err(e) = mempool_json_evm_tx.send(mempool_response.clone()).await {
			error!("Unable to write mempool_response to mempool_grpc_tx channel: {:?}", e);
		}
		if let Err(e) = mempool_json_tx.send(mempool_response.clone()).await {
			error!("Unable to write mempool_response to mempool_json_tx channel: {:?}", e);
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use anyhow::Result;
	use ethereum_types::{H160, H256};
	use tokio::sync::{broadcast, mpsc};
	use types::eth::filter::VariadicValue;

	async fn validate_events(
		params: Option<Params>,
		sent_events: Vec<EventBroadcast>,
		exp_logs: Vec<String>,
	) {
		let mut subscription_sink = SubscriptionSink::new();
		subscription_sink
			.expect_send()
			.times(exp_logs.len())
			.returning(move |msg| Ok(()));

		let (node_evm_event_tx, _) = broadcast::channel(32);
		let (log_tx, mut log_rx) = mpsc::channel(32);

		let node_evm_event_tx_clone = node_evm_event_tx.clone();
		let sub_handle = tokio::spawn(async move {
			subscribe_future_events(
				&node_evm_event_tx_clone,
				subscription_sink,
				params,
				Some(log_tx),
			)
			.await
			.unwrap();
		});

		tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

		// send the events
		for ev in sent_events {
			node_evm_event_tx.send(ev).unwrap();
		}

		tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

		sub_handle.abort();

		let mut collected_logs = Vec::new();
		while let Some(log) = log_rx.recv().await {
			collected_logs.push(format!("{}", log.data));
		}
		assert_eq!(exp_logs, collected_logs);
	}

	#[tokio::test]
	async fn test_events_block_number_restraints() -> Result<()> {
		let contract_address = "0x537393a37a3be4a8129e6ca9dad8329ba6eef228".parse::<H160>()?;
		validate_events(
			Some(Params::Logs(Filter {
				address: VariadicValue::Single(contract_address.clone()),
				topics: [
					Some(VariadicValue::Single(None)),
					Some(VariadicValue::Single(None)),
					Some(VariadicValue::Single(None)),
					Some(VariadicValue::Single(None)),
				],
				from_block: Some(BlockNumber::Num(2)),
				to_block: Some(BlockNumber::Num(5)),
				block_hash: None,
			})),
			vec![
				EventBroadcast::Evm(
					contract_address.clone(), // address
					vec![],                   // topics
					vec![1, 1, 1],            // data
					1,                        // block_number - too early
					[0; 32],                  // block_hash
					[0; 32],                  // txn_hash
					0,                        // txn_index
					0,                        // log_index
				),
				EventBroadcast::Evm(
					contract_address.clone(), // address
					vec![],                   // topics
					vec![1, 2, 3],            // data
					2,                        // block_number
					[0; 32],                  // block_hash
					[0; 32],                  // txn_hash
					0,                        // txn_index
					0,                        // log_index
				),
				EventBroadcast::Evm(
					contract_address.clone(), // address
					vec![],                   // topics
					vec![4, 5, 6],            // data
					5,                        // block_number
					[0; 32],                  // block_hash
					[0; 32],                  // txn_hash
					0,                        // txn_index
					0,                        // log_index
				),
				EventBroadcast::Evm(
					contract_address.clone(), // address
					vec![],                   // topics
					vec![9, 9, 9],            // data
					6,                        // block_number - too late
					[0; 32],                  // block_hash
					[0; 32],                  // txn_hash
					0,                        // txn_index
					0,                        // log_index
				),
			],
			vec!["0x010203".to_owned(), "0x040506".to_owned()],
		)
		.await;
		Ok(())
	}

	#[tokio::test]
	async fn test_events_block_number_restraints_2() -> Result<()> {
		let contract_address = "0x537393a37a3be4a8129e6ca9dad8329ba6eef228".parse::<H160>()?;
		validate_events(
			Some(Params::Logs(Filter {
				address: VariadicValue::Single(contract_address.clone()),
				topics: [
					Some(VariadicValue::Single(None)),
					Some(VariadicValue::Single(None)),
					Some(VariadicValue::Single(None)),
					Some(VariadicValue::Single(None)),
				],
				from_block: Some(BlockNumber::Earliest),
				to_block: Some(BlockNumber::Latest),
				block_hash: None,
			})),
			vec![
				EventBroadcast::Evm(
					contract_address.clone(), // address
					vec![],                   // topics
					vec![1, 2, 3],            // data
					2,                        // block_number
					[0; 32],                  // block_hash
					[0; 32],                  // txn_hash
					0,                        // txn_index
					0,                        // log_index
				),
				EventBroadcast::Evm(
					contract_address.clone(), // address
					vec![],                   // topics
					vec![4, 5, 6],            // data
					5,                        // block_number
					[0; 32],                  // block_hash
					[0; 32],                  // txn_hash
					0,                        // txn_index
					0,                        // log_index
				),
			],
			vec!["0x010203".to_owned(), "0x040506".to_owned()],
		)
		.await;
		Ok(())
	}

	#[tokio::test]
	async fn test_events_block_number_restraints_3() -> Result<()> {
		let contract_address = "0x537393a37a3be4a8129e6ca9dad8329ba6eef228".parse::<H160>()?;
		validate_events(
			Some(Params::Logs(Filter {
				address: VariadicValue::Single(contract_address.clone()),
				topics: [
					Some(VariadicValue::Single(None)),
					Some(VariadicValue::Single(None)),
					Some(VariadicValue::Single(None)),
					Some(VariadicValue::Single(None)),
				],
				from_block: None,
				to_block: None,
				block_hash: None,
			})),
			vec![
				EventBroadcast::Evm(
					contract_address.clone(), // address
					vec![],                   // topics
					vec![1, 2, 3],            // data
					2,                        // block_number
					[0; 32],                  // block_hash
					[0; 32],                  // txn_hash
					0,                        // txn_index
					0,                        // log_index
				),
				EventBroadcast::Evm(
					contract_address.clone(), // address
					vec![],                   // topics
					vec![4, 5, 6],            // data
					5,                        // block_number
					[0; 32],                  // block_hash
					[0; 32],                  // txn_hash
					0,                        // txn_index
					0,                        // log_index
				),
			],
			vec!["0x010203".to_owned(), "0x040506".to_owned()],
		)
		.await;
		Ok(())
	}

	#[tokio::test]
	async fn test_events_topic_restraints() -> Result<()> {
		let contract_address = "0x537393a37a3be4a8129e6ca9dad8329ba6eef228".parse::<H160>()?;
		let event_signature =
			"0x537393a37a3be4a8129e6ca9dad8329ba6eef228537393a37a3be4a8129e6ca9".parse::<H256>()?;
		let bogus =
			"0x1111111111111111111111111111111111111111111111111111111111111111".parse::<H256>()?;
		let field1 =
			"0x0000000000000000000000000000000000000000000000000000000000000001".parse::<H256>()?;
		let field2_1 =
			"0x0000000000000000000000000000000000000000000000000000000000000021".parse::<H256>()?;
		let field2_2 =
			"0x0000000000000000000000000000000000000000000000000000000000000022".parse::<H256>()?;
		let field3_1 =
			"0x0000000000000000000000000000000000000000000000000000000000000031".parse::<H256>()?;
		let field3_2 =
			"0x0000000000000000000000000000000000000000000000000000000000000032".parse::<H256>()?;

		validate_events(
			Some(Params::Logs(Filter {
				address: VariadicValue::Single(contract_address.clone()),
				topics: [
					Some(VariadicValue::Single(Some(event_signature))),
					Some(VariadicValue::Single(Some(field1))),
					Some(VariadicValue::Null),
					Some(VariadicValue::Multiple(vec![Some(field3_1), Some(field3_2)])),
				],
				from_block: None,
				to_block: None,
				block_hash: None,
			})),
			vec![
				EventBroadcast::Evm(
					contract_address.clone(), // address
					vec![event_signature],    // topics
					vec![1, 2, 3],            // data
					2,                        // block_number
					[0; 32],                  // block_hash
					[0; 32],                  // txn_hash
					0,                        // txn_index
					0,                        // log_index
				),
				EventBroadcast::Evm(
					contract_address.clone(), // address
					vec![bogus],              // topics
					vec![2, 3, 4],            // data
					5,                        // block_number
					[0; 32],                  // block_hash
					[0; 32],                  // txn_hash
					0,                        // txn_index
					0,                        // log_index
				),
				EventBroadcast::Evm(
					contract_address.clone(),                          // address
					vec![event_signature, field1, field2_1, field3_1], // topics
					vec![3, 4, 5],                                     // data
					5,                                                 // block_number
					[0; 32],                                           // block_hash
					[0; 32],                                           // txn_hash
					0,                                                 // txn_index
					0,                                                 // log_index
				),
				EventBroadcast::Evm(
					contract_address.clone(),                          // address
					vec![event_signature, field1, field2_2, field3_2], // topics
					vec![4, 5, 6],                                     // data
					5,                                                 // block_number
					[0; 32],                                           // block_hash
					[0; 32],                                           // txn_hash
					0,                                                 // txn_index
					0,                                                 // log_index
				),
				EventBroadcast::Evm(
					contract_address.clone(),                         // address
					vec![event_signature, bogus, field2_2, field3_2], // topics
					vec![4, 5, 6],                                    // data
					5,                                                // block_number
					[0; 32],                                          // block_hash
					[0; 32],                                          // txn_hash
					0,                                                // txn_index
					0,                                                // log_index
				),
			],
			vec!["0x010203".to_owned(), "0x030405".to_owned(), "0x040506".to_owned()],
		)
		.await;
		Ok(())
	}

	#[tokio::test]
	async fn test_events_invalid_address() -> Result<()> {
		let contract_address = "0x537393a37a3be4a8129e6ca9dad8329ba6eef228".parse::<H160>()?;
		validate_events(
			Some(Params::Logs(Filter {
				address: VariadicValue::Single(contract_address.clone()),
				topics: [
					Some(VariadicValue::Single(None)),
					Some(VariadicValue::Single(None)),
					Some(VariadicValue::Single(None)),
					Some(VariadicValue::Single(None)),
				],
				from_block: None,
				to_block: None,
				block_hash: None,
			})),
			vec![EventBroadcast::Evm(
				H160::zero(), // address
				vec![],       // topics
				vec![],       // data
				0,            // block_number
				[0; 32],      // block_hash
				[0; 32],      // txn_hash
				0,            // txn_index
				0,            // log_index
			)],
			vec![],
		)
		.await;
		Ok(())
	}
}
