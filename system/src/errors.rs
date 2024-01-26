use jsonrpsee::types::{ErrorObject, ErrorObjectOwned};
use tonic::Status;

#[derive(thiserror::Error, Debug)]
pub enum NodeError {
	// not found errors
	#[error("account fetch error: {0}")]
	AccountFetchError(String),
	#[error("block fetch error: {0}")]
	BlockFetchError(String),
	#[error("chain state fetch error: {0}")]
	ChainStateFetchError(String),
	#[error("contract fetch error: {0}")]
	ContractFetchError(String),
	#[error("contract instance fetch error: {0}")]
	ContractInstanceFetchError(String),
	#[error("event fetch error: {0}")]
	EventFetchError(String),
	#[error("stake fetch error: {0}")]
	StakeFetchError(String),
	#[error("transaction receipt fetch error: {0}")]
	TransactionReceiptFetchError(String),
	// invalid errors
	#[error("parse error: {0}")]
	ParseError(String),
	#[error("invalid address: {0}")]
	InvalidAddress(String),
	#[error("invalid transaction type: {0}")]
	InvalidTransactionType(String),
	#[error("invalid contract type: {0}")]
	InvalidContractType(String),
	#[error("invalid access type: {0}")]
	InvalidAccessType(String),
	#[error("invalid read only function call: {0}")]
	InvalidReadOnlyFunctionCall(String),
	// internal errors
	#[error("failed to access database: {0}")]
	DBError(String),
	#[error("unexpected error: {0}")]
	UnexpectedError(String),
	#[error("smart contract call failed: {0}")]
	SmartContractCallFailed(String),
	// precondition errors
	#[error("Insufficient balance: {0}")]
	InsufficientBalance(String),
}

impl From<NodeError> for Status {
	fn from(error: NodeError) -> Self {
		match error {
			NodeError::AccountFetchError(msg) |
			NodeError::ContractFetchError(msg) |
			NodeError::ChainStateFetchError(msg) |
			NodeError::ContractInstanceFetchError(msg) |
			NodeError::BlockFetchError(msg) |
			NodeError::StakeFetchError(msg) |
			NodeError::EventFetchError(msg) |
			NodeError::TransactionReceiptFetchError(msg) => Status::not_found(msg),
			NodeError::ParseError(msg) |
			NodeError::InvalidAddress(msg) |
			NodeError::InvalidTransactionType(msg) |
			NodeError::InvalidContractType(msg) |
			NodeError::InvalidAccessType(msg) |
			NodeError::InvalidReadOnlyFunctionCall(msg) => Status::invalid_argument(msg),
			NodeError::DBError(msg) |
			NodeError::UnexpectedError(msg) |
			NodeError::SmartContractCallFailed(msg) => Status::internal(msg),
			NodeError::InsufficientBalance(msg) => Status::failed_precondition(msg),
		}
	}
}

impl From<NodeError> for ErrorObject<'_> {
	fn from(err: NodeError) -> Self {
		ErrorObjectOwned::owned(400, err.to_string(), None::<()>)
	}
}
