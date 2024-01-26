use anyhow::Error;
use async_trait::async_trait;
use db_traits::{base::BaseState, block_proposer::BlockProposerState};
use primitives::BlockNumber;
use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::{collections::HashMap, fs::remove_dir_all};
use system::block_proposer::BlockProposer;

use primitives::Address;

pub struct StateRock {
	pub(crate) db_path: String,
	pub db: DBWithThreadMode<MultiThreaded>,
}

impl StateRock {
	fn add_block_numbers_for_address(
		&self,
		address: &Address,
		block_number: BlockNumber,
	) -> Result<(), Error> {
		let key = format!("block_numbers_for_address:{:?}", address);
		let mut block_numbers: Vec<BlockNumber> = match self.db.get(&key)? {
			Some(value) => serde_json::from_slice(&value)?,
			None => Vec::new(),
		};

		block_numbers.push(block_number);

		let value = serde_json::to_vec(&block_numbers)?;
		self.db.put(key, value)?;

		Ok(())
	}

	fn get_block_numbers_for_address(&self, address: &Address) -> Result<Vec<BlockNumber>, Error> {
		let key = format!("block_numbers_for_address:{:?}", address);
		match self.db.get(&key)? {
			Some(value) => {
				let block_numbers: Vec<BlockNumber> = serde_json::from_slice(&value)?;
				Ok(block_numbers)
			},
			None => Ok(Vec::new()),
		}
	}
}
#[async_trait]
impl BaseState<BlockProposer> for StateRock {
	async fn create_table(&self) -> Result<(), Error> {
		Ok(())
	}

	async fn create(&self, block_proposer: &BlockProposer) -> Result<(), Error> {
		let key = format!("account:{:?}", block_proposer.block_number);
		let value = serde_json::to_string(block_proposer)?;
		self.db.put(&key, &value)?;
		Ok(())
	}

	async fn update(&self, _block_proposer: &BlockProposer) -> Result<(), Error> {
		// Implementation for update method
		Ok(())
	}

	async fn raw_query(&self, _query: &str) -> Result<(), Error> {
		// Explicitly drop the DB to close any open connections or file handles
		drop(&self.db);

		// Remove the database directory
		remove_dir_all(&self.db_path);

		Ok(())
	}

	async fn set_schema_version(&self, _version: u32) -> Result<(), Error> {
		// Implementation for set_schema_version method
		Ok(())
	}
}

#[async_trait]
impl BlockProposerState for StateRock {
	async fn store_block_proposer(
		&self,
		cluster_address: Address,
		block_number: BlockNumber,
		address: Address,
	) -> Result<(), Error> {
		let block_proposer = BlockProposer { cluster_address, block_number, address };
		let key = format!("block_proposer:{:?}", block_number);
		let value = serde_json::to_string(&block_proposer)?;
		self.db.put(&key, &value)?;

		// fetch from rocks db  and added to the vector
		let key = format!("block_proposer_cluster_address:{:?}", cluster_address);
		let mut block_proposer_vector: Vec<BlockProposer> = match self.db.get(&key)? {
			Some(value) => serde_json::from_slice(&value)?,
			None => Vec::new(),
		};
		block_proposer_vector.push(block_proposer.clone());
		let value = serde_json::to_string(&block_proposer_vector)?;
		self.db.put(&key, &value)?;

		// composite key
		let key = format!(
			"block_proposer_cluster_address:{:?}_block_number{:?}",
			cluster_address, block_number
		);
		let value = serde_json::to_string(&block_proposer.clone())?;
		self.db.put(&key, &value)?;

		// add new block proposer in vector and store in rocks db
		let key = format!("block_proposer_address:{:?}", address);
		let mut block_proposer_vector: Vec<BlockProposer> = match self.db.get(&key)? {
			Some(value) => serde_json::from_slice(&value)?,
			None => Vec::new(),
		};
		block_proposer_vector.push(block_proposer.clone());
		let value = serde_json::to_string(&block_proposer_vector)?;
		self.db.put(&key, &value)?;

		//self.db.put(&key, &block_number.to_be_bytes())?;
		Ok(())
	}

	async fn load_selectors_block_numbers(
		&self,
		address: &Address,
	) -> Result<Option<Vec<BlockNumber>>, Error> {
		let key = format!("block_proposer_address:{:?}", address);

		let block_proposer_vector: Vec<BlockProposer> = match self.db.get(&key)? {
			Some(value) => serde_json::from_slice(&value)?,
			None => Vec::new(),
		};

		let mut block_numbers = Vec::new();
		for r in block_proposer_vector {
			block_numbers.push(r.block_number.try_into().unwrap_or(BlockNumber::MIN));
		}

		if block_numbers.is_empty() {
			Ok(None)
		} else {
			Ok(Some(block_numbers))
		}
	}

	async fn update_selected_next(
		&self,
		_selector_address: &Address,
		_cluster_address: &Address,
	) -> Result<(), Error> {
		todo!()
	}

	async fn store_block_proposers(
		&self,
		block_proposers: &HashMap<Address, HashMap<BlockNumber, Address>>,
		selector_address: Option<Address>,
		cluster_address: Option<Address>,
	) -> Result<(), Error> {
		for (cluster_address, block_numbers) in block_proposers {
			for (block_number, address) in block_numbers {
				self.store_block_proposer(cluster_address.clone(), *block_number, address.clone())
					.await?;
			}
		}
		if let Some(selector_address) = selector_address {
			if let Some(cluster_address) = cluster_address {
				self.update_selected_next(&selector_address, &cluster_address).await?;
			}
		}
		Ok(())
	}

	async fn load_last_block_proposer_block_number(
		&self,
		cluster_address: Address,
	) -> Result<BlockNumber, Error> {
		let key = format!("block_proposer_cluster_address:{:?}", cluster_address);
		let mut max_block_number = 0;

		let serialized_data = self.db.get(&key);

		if let Ok(block_proposer_data) = serialized_data {
			// Check if the Option contains Some variant
			if let Some(block_proposer_vector) = block_proposer_data {
				let block_proposer_list: Vec<BlockProposer> =
					serde_json::from_slice(&block_proposer_vector)?;

				for block_proposer in block_proposer_list {
					let block_number =
						block_proposer.block_number.try_into().unwrap_or(BlockNumber::MIN);
					if block_number > max_block_number {
						max_block_number = block_number;
					}
				}
			}
		}
		Ok(max_block_number)
	}

	async fn load_block_proposer(
		&self,
		cluster_address: Address,
		block_number: BlockNumber,
	) -> Result<Option<BlockProposer>, Error> {
		let key = format!(
			"block_proposer_cluster_address:{:?}_block_number{:?}",
			cluster_address, block_number
		);

		match self.db.get(&key)? {
			Some(value) => {
				let value_str = String::from_utf8_lossy(&value);
				Ok(serde_json::from_str(&value_str)?)
			},
			None => Err(Error::msg("Block Proposer not found")),
		}
	}

	async fn load_block_proposers(
		&self,
		cluster_address: &Address,
	) -> Result<HashMap<BlockNumber, BlockProposer>, Error> {
		let key = format!("block_proposer_cluster_address:{:?}", cluster_address);
		let mut block_proposers = HashMap::new();

		let serialized_data = self.db.get(&key);

		if let Ok(block_proposer_data) = serialized_data {
			// Check if the Option contains Some variant
			if let Some(block_proposer_vector) = block_proposer_data {
				let block_proposer_list: Vec<BlockProposer> =
					serde_json::from_slice(&block_proposer_vector)?;
				for block_proposer in block_proposer_list {
					let _block_number =
						block_proposer.block_number.try_into().unwrap_or(BlockNumber::MIN);
					block_proposers.insert(block_proposer.block_number, block_proposer);
				}
			}
		}
		Ok(block_proposers)
	}

	async fn find_cluster_address(&self, _address: Address) -> Result<Address, Error> {
		// Implementation for set_schema_version method
		todo!()
	}

	async fn is_selected_next(&self, _address: &Address) -> Result<bool, Error> {
		todo!()
	}
}
