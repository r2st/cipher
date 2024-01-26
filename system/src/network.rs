use crate::{
	block::{Block, BlockPayload},
	block_header::{BlockHeader, BlockHeaderPayload},
	block_proposer::BlockProposerPayload,
	transaction::Transaction,
	vote::Vote,
	vote_result::VoteResult,
};
use primitive_types::{H160, H256};
use primitives::*;

use crate::node_info::NodeInfo;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug)]
pub enum BroadcastNetwork {
	BroadcastNodeInfo(NodeInfo),
	BroadcastTransaction(Transaction),
	BroadcastValidateBlock(Block),
	BroadcastBlock(Block),
	BroadcastBlockHeader(BlockHeader),
	BroadcastBlockProposer(HashMap<Address, HashMap<BlockNumber, Address>>),
	BroadcastVote(Vote),
	BroadcastVoteResult(VoteResult),
}

#[derive(Debug)]
pub enum ReceiveNetwork {
	ReceiveNodeInfo(NodeInfo),
	ReceiveValidateBlock(BlockPayload),
	ReceiveBlock(BlockPayload),
	ReceiveBlockHeader(BlockHeaderPayload),
	ReceiveBlockProposer(BlockProposerPayload),
	ReceiveVote(Vote),
	ReceiveVoteResult(VoteResult),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EventBroadcast {
	Evm(
		H160,
		Vec<H256>,
		Vec<u8>,
		BlockNumber,
		BlockHash,
		TransactionHash,
		u64, // txn_index
		u64, // log_index
	),
}
