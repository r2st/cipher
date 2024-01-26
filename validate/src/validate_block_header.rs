use anyhow::Error;
use system::block_header::BlockHeaderPayload;

pub struct ValidateBlockHeader;

impl ValidateBlockHeader {
	pub async fn validate_block_header(
		block_header_payload: &BlockHeaderPayload,
	) -> Result<(), Error> {
		// Verify the signature of the block header to know definitively which node sent it
		block_header_payload.verify_signature().await
	}
}
