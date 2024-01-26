use crate::block_proposer_state::BlockProposerState;
use anyhow::{anyhow, Error};
use db::db::DbTxConn;
use node_info::node_info_state::NodeInfoState;
use primitives::{Address, BlockNumber};
use rand::prelude::IteratorRandom;
use std::collections::HashMap;
use system::block_proposer::BlockProposer;

pub struct BlockProposerManager {}

impl<'a> BlockProposerManager {
	pub async fn select_block_proposers(
		&mut self,
		block_proposer: BlockProposer,
		n: BlockNumber,
		db_pool_conn: &'a DbTxConn<'a>,
	) -> Result<(), Error> {
		let block_proposer_state = BlockProposerState::new(db_pool_conn).await?;
		if block_proposer_state.is_selected_next(&block_proposer.address).await? {
			return Ok(());
		}
		// Find the cluster_address using find_full_node_info function
		let cluster_address = block_proposer.cluster_address.clone();
		let node_info_state = NodeInfoState::new(db_pool_conn).await?;
		// Load all the cluster nodes
		let nodes = node_info_state.load_nodes(&cluster_address).await?;

		// Randomly select one node from the loaded clusters
		let mut rng = rand::thread_rng();
		let selected_node = nodes.keys().choose(&mut rng);
		let last_block_number = block_proposer_state
			.load_last_block_proposer_block_number(cluster_address)
			.await?;

		if !self
			.is_block_proposer(
				last_block_number,
				cluster_address,
				block_proposer.clone(),
				db_pool_conn,
			)
			.await?
		{
			return Err(anyhow!("Account is not the proposer for last block"));
		}
		if let Some(node) = selected_node {
			// Store the selected node as the block proposer for the next n blocks
			let block_proposers: HashMap<BlockNumber, Address> = (last_block_number + 1..
				n + last_block_number + 1)
				.map(|i| (i, nodes[node].address.clone()))
				.collect();

			let mut cluster_block_proposers = HashMap::new();
			cluster_block_proposers.insert(cluster_address.clone(), block_proposers);
			// Store the block proposers using store_block_proposers function
			block_proposer_state
				.store_block_proposers(
					&cluster_block_proposers,
					Some(block_proposer.address),
					Some(cluster_address),
				)
				.await?;
		}
		Ok(())
	}

	pub async fn is_block_proposer(
		&mut self,
		block_number: BlockNumber,
		cluster_address: Address,
		block_proposer: BlockProposer,
		db_pool_conn: &'a DbTxConn<'a>,
	) -> Result<bool, Error> {
		let block_proposer_state = BlockProposerState::new(db_pool_conn).await?;
		let loaded_proposer =
			block_proposer_state.load_block_proposer(cluster_address, block_number).await?;
		// Compare after converting loaded_proposer to Option<BlockProposer>
		Ok(loaded_proposer.as_ref() == Some(&block_proposer))
	}
}
