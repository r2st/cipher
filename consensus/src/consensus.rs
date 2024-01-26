use anyhow::Error;
use block::block_state::BlockState;
use block_proposer::block_proposer_state::BlockProposerState;
use db::db::Database;
use execute::execute_block::ExecuteBlock;
use log::{debug, warn};
use node_info::node_info_state::NodeInfoState;
use primitives::*;
use secp256k1::{PublicKey, SecretKey};
use system::{
	block::BlockPayload,
	block_header::BlockHeaderPayload,
	block_proposer::BlockProposerPayload,
	network::{BroadcastNetwork, EventBroadcast},
	node_info::NodeInfo,
	vote::{Vote, VoteSignPayload},
	vote_result::VoteResult,
};
use tokio::sync::{broadcast, mpsc};
use validate::{
	validate_block::ValidateBlock, validate_block_header::ValidateBlockHeader,
	validate_block_proposer::ValidateBlockProposer, validate_node_info::ValidateNodeInfo,
	validate_vote::ValidateVote, validate_vote_result::ValidateVoteResult,
};
use validator::validator_state::ValidatorState;
use vote::vote_state::VoteState;
use vote_result::{vote_result_manager::VoteResultManager, vote_result_state::VoteResultState};
use vrf_helper::common::SecpVRF;

pub struct Consensus {
	pub event_tx: broadcast::Sender<EventBroadcast>,
	pub node_event_tx: broadcast::Sender<EventData>,
	pub network_client_tx: mpsc::Sender<BroadcastNetwork>,
	pub node_address: Address,
	pub pool_address: Address,
	pub cluster_address: Address,
	pub secret_key: SecretKey,
	pub verifying_key: PublicKey,
	pub multinode_mode: bool,
}

impl Consensus {
	pub fn new(
		event_tx: broadcast::Sender<EventBroadcast>,
		node_event_tx: broadcast::Sender<EventData>,
		network_client_tx: mpsc::Sender<BroadcastNetwork>,
		node_address: Address,
		pool_address: Address,
		cluster_address: Address,
		secret_key: SecretKey,
		verifying_key: PublicKey,
		multinode_mode: bool,
	) -> Consensus {
		Consensus {
			event_tx,
			node_event_tx,
			network_client_tx,
			node_address,
			pool_address,
			cluster_address,
			secret_key,
			verifying_key,
			multinode_mode,
		}
	}

	/// Validate the node info and store it in the database
	pub async fn receive_node_info(&mut self, node_info: NodeInfo) -> Result<(), Error> {
		let db_pool_conn = Database::get_pool_connection().await?;
		// Verify the signature of the node info
		ValidateNodeInfo::validate_node_info(&node_info).await?; // If there is any error it will return from here or else its a valid node info

		// Only store if the node info is valid
		let node_info_state = NodeInfoState::new(&db_pool_conn).await?;
		node_info_state.store_node_info(&node_info).await?;
		Ok(())
	}

	/// Blocks are broadcast twice, once for validation (only to validators) and once when the block
	/// is finalized (to all the nodes). The network code hands off the just received block here
	pub async fn receive_validate_block(
		&mut self,
		block_payload: BlockPayload,
	) -> Result<(), Error> {
		let db_pool_conn = Database::get_pool_connection().await?;
		debug!("Node has received new block from the network");
		// can call it here
		// BlockPayload has the blockHash / blockNumber / clusterId / transactions / verifyingKey
		let valid_block = match ValidateBlock::validate_block(&block_payload, &db_pool_conn).await {
			Ok(_) => true,
			Err(e) => return Err(e),
		};

		// Only store if a valid block
		if valid_block {
			let block_state = BlockState::new(&db_pool_conn).await?;
			block_state.store_block(block_payload.block.clone()).await?;
		}
		let validator_state = ValidatorState::new(&db_pool_conn).await?;

		if validator_state
			.is_validator(&self.node_address, block_payload.block.block_header.block_number)
			.await?
		{
			let vote_sign_payload = VoteSignPayload::new(
				block_payload.block.block_header.block_number,
				block_payload.block.block_header.block_hash,
				self.cluster_address.clone(),
				valid_block,
			);

			let sig = vote_sign_payload.sign_with_ecdsa(self.secret_key)?;

			let vote = Vote::new(
				vote_sign_payload,
				self.node_address.clone(),
				sig.serialize_compact().to_vec(),
				self.verifying_key.serialize().to_vec(),
			);
			let vote_state = VoteState::new(&db_pool_conn).await?;
			vote_state.store_vote(&vote).await?;
			if self.multinode_mode {
				if let Err(e) = self
					.network_client_tx
					.send(BroadcastNetwork::BroadcastValidateBlock(block_payload.block))
					.await
				{
					warn!("Unable to write block to network_client_tx channel: {:?}", e)
				}
				if let Err(e) =
					self.network_client_tx.send(BroadcastNetwork::BroadcastVote(vote)).await
				{
					warn!("Unable to write transaction to network_client_tx channel: {:?}", e)
				}
			}
		} else {
			warn!("Node is not a validator for this block");
		}
		Ok(())
	}

	pub async fn receive_block(&mut self, block_payload: BlockPayload) -> Result<(), Error> {
		let db_pool_conn = Database::get_pool_connection().await?;
		let validator_state = ValidatorState::new(&db_pool_conn).await?;

		if !validator_state
			.is_validator(&self.node_address, block_payload.block.block_header.block_number)
			.await?
		{
			let block_state = BlockState::new(&db_pool_conn).await?;
			block_state.store_block(block_payload.block.clone()).await?;
		}

		// Broadcast block if in multinode mode
		if self.multinode_mode {
			if let Err(e) = self
				.network_client_tx
				.send(BroadcastNetwork::BroadcastBlock(block_payload.block))
				.await
			{
				warn!("Unable to write block to network_client_tx channel: {:?}", e)
			}
		}
		Ok(())
	}
	pub async fn receive_block_header(
		&mut self,
		block_header_payload: BlockHeaderPayload,
	) -> Result<(), Error> {
		// Verify signatures , verify block header and then store it
		let valid_block_header =
			ValidateBlockHeader::validate_block_header(&block_header_payload).await.is_ok();
		if self.multinode_mode && valid_block_header {
			if let Err(e) = self
				.network_client_tx
				.send(BroadcastNetwork::BroadcastBlockHeader(block_header_payload.block_header))
				.await
			{
				warn!("Unable to write block to network_client_tx channel: {:?}", e)
			}
		}
		Ok(())
	}

	pub async fn receive_block_proposer(
		&mut self,
		block_proposer_payload: BlockProposerPayload,
	) -> Result<(), Error> {
		let db_pool_conn = Database::get_pool_connection().await?;
		//verify signatures
		let _valid_block_proposer =
			ValidateBlockProposer::validate_block_proposer(&block_proposer_payload).await?;
		let block_proposer_state = BlockProposerState::new(&db_pool_conn).await?;
		block_proposer_state
			.store_block_proposers(
				&block_proposer_payload.cluster_block_proposers.clone(),
				None,
				None,
			)
			.await?;

		// Broadcast block proposer if in multinode mode and the block proposer selection is valid
		if self.multinode_mode {
			if let Err(e) = self
				.network_client_tx
				.send(BroadcastNetwork::BroadcastBlockProposer(
					block_proposer_payload.cluster_block_proposers,
				))
				.await
			{
				warn!("Unable to write block_proposer to network_client_tx channel: {:?}", e)
			}
		}
		Ok(())
	}

	pub async fn receive_vote(&mut self, vote: Vote) -> Result<(), Error> {
		let db_pool_conn = Database::get_pool_connection().await?;
		// Verify signature , if valid store it
		let _valid_vote = ValidateVote::validate_vote(&vote).await?;

		let vote_state = VoteState::new(&db_pool_conn).await?;
		vote_state.store_vote(&vote.clone()).await?;

		let vote_result_manager =
			VoteResultManager::new(self.network_client_tx.clone(), self.multinode_mode);
		// Everytime you receive a new vote, check the result.
		let vote_result = vote_result_manager
			.vote_result(
				vote.data.block_number,
				&vote.data.block_hash,
				&self.pool_address,
				&self.node_address,
				&self.cluster_address,
				&self.secret_key,
				&self.verifying_key,
				&db_pool_conn,
			)
			.await?;

		if vote_result && self.multinode_mode {
			let block_state = BlockState::new(&db_pool_conn).await?;
			let block =
				block_state.load_block(vote.data.block_number, &self.cluster_address).await?;
			// println!("LOADED BLOCK: {}", block.clone());

			// Broadcast block & block header to all nodes
			if let Err(e) = self
				.network_client_tx
				.send(BroadcastNetwork::BroadcastBlock(block.clone()))
				.await
			{
				warn!("Unable to write block to network_client_tx channel: {:?}", e)
			}
			if let Err(e) = self
				.network_client_tx
				.send(BroadcastNetwork::BroadcastBlockHeader(block.block_header.clone()))
				.await
			{
				warn!("Unable to write block_header to network_client_tx channel: {:?}", e)
			}
			// Execute the block
			let events =
				ExecuteBlock::execute_block(&block, self.event_tx.clone(), &db_pool_conn).await?;
			self.broadcast_events(events);
		}
		if self.multinode_mode {
			if let Err(e) = self.network_client_tx.send(BroadcastNetwork::BroadcastVote(vote)).await
			{
				warn!("Unable to write transaction to network_client_tx channel: {:?}", e)
			}
		}
		Ok(())
	}

	pub async fn receive_vote_result(&mut self, vote_result: VoteResult) -> Result<(), Error> {
		let db_pool_conn = Database::get_pool_connection().await?;
		//verify signatures , verify vote result and then store it
		let _valid_vote_result = ValidateVoteResult::validate_vote_result(&vote_result).await?;
		let vote_result_state = VoteResultState::new(&db_pool_conn).await?;
		vote_result_state.store_vote_result(&vote_result.clone()).await?;
		let block_state = BlockState::new(&db_pool_conn).await?;
		let block = block_state
			.load_block(vote_result.data.block_number, &vote_result.data.cluster_address.clone())
			.await?;
		if vote_result.data.vote_passed {
			let events =
				ExecuteBlock::execute_block(&block, self.event_tx.clone(), &db_pool_conn).await?;
			self.broadcast_events(events);
		}
		if self.multinode_mode {
			if let Err(e) = self
				.network_client_tx
				.send(BroadcastNetwork::BroadcastVoteResult(vote_result))
				.await
			{
				warn!("Unable to write transaction to network_client_tx channel: {:?}", e)
			}
		}
		Ok(())
	}

	pub fn broadcast_events(&self, events: Vec<EventData>) {
		for event in events {
			if let Err(err) = self.node_event_tx.send(event) {
				warn!("Failed to publish ReceiveBlock event due to {:?}", err);
			}
		}
	}
}
