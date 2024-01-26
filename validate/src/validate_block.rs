use crate::validate_common::ValidateCommon;
use anyhow::Error;
use block_proposer::block_proposer_manager::BlockProposerManager;
use db::db::DbTxConn;
use system::{account::Account, block::BlockPayload, block_proposer::BlockProposer};
pub struct ValidateBlock {}

impl<'a> ValidateBlock {
	pub async fn validate_block(
		block_payload: &BlockPayload,
		db_pool_conn: &'a DbTxConn<'a>,
	) -> Result<(), Error> {
		// Verify the signature of the block to know definitively which node sent it
		let _ = block_payload.verify_signature().await?;

		let block_proposer_address = Account::address(&block_payload.verifying_key)?;
		let mut block_proposer_manager = BlockProposerManager {};
		let block_proposer = BlockProposer::new(
			block_payload.block.block_header.cluster_address,
			block_payload.block.block_header.block_number,
			block_proposer_address,
		);

		let _ = block_proposer_manager
			.is_block_proposer(
				1, //block_payload.block.block_header.block_number,
				block_payload.block.block_header.cluster_address,
				block_proposer,
				db_pool_conn,
			)
			.await?;

		// Validate each transaction in the block
		for tx in &block_payload.block.transactions {
			let sender = Account::address(&tx.verifying_key)?;
			ValidateCommon::validate_tx(&tx.clone(), &sender, db_pool_conn).await?;
		}

		Ok(())
	}
}
