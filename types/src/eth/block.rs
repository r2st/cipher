// Concepts and implementations inspired by frontier project
// https://github.com/polkadot-evm/frontier/blob/master/client/rpc-core/src/types/block.rs

use super::{bytes::Bytes, transaction::Transaction};
use ethereum_types::{Bloom, H160, H256, H64, U256};
use serde::{ser::Error, Deserialize, Serialize, Serializer};
use std::{collections::BTreeMap, ops::Deref};

/// Represents rpc api block number param.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Default, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BlockNumber {
	/// Hash
	Hash {
		/// block hash
		hash: H256,
		/// only return blocks part of the canon chain
		require_canonical: bool,
	},
	/// Number
	Num(u64),
	/// Latest block
	#[default]
	Latest,
	/// Earliest block (genesis)
	Earliest,
	/// Pending block (being mined)
	Pending,
	/// The most recent crypto-economically secure block.
	/// There is no difference between Ethereum's `safe` and `finalized`
	/// in Substrate finality gadget.
	Safe,
	/// The most recent crypto-economically secure block.
	Finalized,
}

impl BlockNumber {
	/// Convert block number to min block target.
	pub fn to_min_block_num(&self) -> Option<u64> {
		match *self {
			BlockNumber::Num(ref x) => Some(*x),
			BlockNumber::Earliest => Some(0),
			_ => None,
		}
	}

	pub fn gte(&self, block_number: u128) -> bool {
		match *self {
			BlockNumber::Num(x) if x as u128 >= block_number => true,
			BlockNumber::Latest => true,
			_ => false,
		}
	}

	pub fn lte(&self, block_number: u128) -> bool {
		match *self {
			BlockNumber::Num(x) if x as u128 <= block_number => true,
			BlockNumber::Earliest => true,
			_ => false,
		}
	}
}

/// Block Transactions
#[derive(Debug, Clone)]
pub enum BlockTransactions {
	/// Only hashes
	Hashes(Vec<H256>),
	/// Full transactions
	Full(Vec<Transaction>),
}

impl Serialize for BlockTransactions {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		match *self {
			BlockTransactions::Hashes(ref hashes) => hashes.serialize(serializer),
			BlockTransactions::Full(ref ts) => ts.serialize(serializer),
		}
	}
}

impl Default for BlockTransactions {
	fn default() -> Self {
		BlockTransactions::Hashes(Vec::new())
	}
}

impl FromIterator<Transaction> for BlockTransactions {
	fn from_iter<I: IntoIterator<Item = Transaction>>(iter: I) -> Self {
		BlockTransactions::Full(iter.into_iter().collect())
	}
}

/// Block representation
#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Block {
	/// Header of the block
	#[serde(flatten)]
	pub header: Header,
	/// Total difficulty
	pub total_difficulty: Option<U256>,
	/// Uncles' hashes
	pub uncles: Vec<H256>,
	/// Transactions
	pub transactions: BlockTransactions,
	/// Size in bytes
	pub size: Option<U256>,
	/// Base Fee for post-EIP1559 blocks.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub base_fee_per_gas: Option<U256>,
}

/// Block header representation.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Header {
	/// Hash of the block
	pub hash: Option<H256>,
	/// Hash of the parent
	pub parent_hash: H256,
	/// Hash of the uncles
	#[serde(rename = "sha3Uncles")]
	pub uncles_hash: H256,
	/// Authors address
	pub author: H160,
	/// Alias of `author`
	pub miner: Option<H160>,
	/// State root hash
	pub state_root: H256,
	/// Transactions root hash
	pub transactions_root: H256,
	/// Transactions receipts root hash
	pub receipts_root: H256,
	/// Block number
	pub number: Option<U256>,
	/// Gas Used
	pub gas_used: U256,
	/// Gas Limit
	pub gas_limit: U256,
	/// Extra data
	pub extra_data: Bytes,
	/// Logs bloom
	pub logs_bloom: Bloom,
	/// Timestamp
	pub timestamp: U256,
	/// Difficulty
	pub difficulty: U256,
	/// Nonce
	pub nonce: Option<H64>,
	/// Size in bytes
	pub size: Option<U256>,
}

/// Block representation with additional info.
pub type RichBlock = Rich<Block>;

/// Header representation with additional info.
pub type RichHeader = Rich<Header>;

/// Value representation with additional info
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Rich<T> {
	/// Standard value.
	pub inner: T,
	/// Engine-specific fields with additional description.
	/// Should be included directly to serialized block object.
	// TODO [ToDr] #[serde(skip_serializing)]
	pub extra_info: BTreeMap<String, String>,
}

impl<T> Deref for Rich<T> {
	type Target = T;
	fn deref(&self) -> &Self::Target {
		&self.inner
	}
}

impl<T: Serialize> Serialize for Rich<T> {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		use serde_json::{to_value, Value};

		let serialized = (to_value(&self.inner), to_value(&self.extra_info));
		if let (Ok(Value::Object(mut value)), Ok(Value::Object(extras))) = serialized {
			// join two objects
			value.extend(extras);
			// and serialize
			value.serialize(serializer)
		} else {
			Err(S::Error::custom("Unserializable structures: expected objects"))
		}
	}
}
