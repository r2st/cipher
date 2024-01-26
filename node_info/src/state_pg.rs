use anyhow::Error;
use async_trait::async_trait;
use db::postgres::{
	pg_models::{NewNodeInfo, QueryNodeInfo},
	postgres::{PgConnectionType, PostgresDBConn},
	schema,
};
use db_traits::{base::BaseState, node_info::NodeInfoState};
use diesel::{self, prelude::*, QueryResult};
use primitives::Address;
use std::collections::HashMap;
use system::node_info::{NodeInfo, NodeInfoSignPayload};

pub struct StatePg<'a> {
	pub(crate) pg: &'a PostgresDBConn<'a>,
}
#[async_trait]
impl<'a> BaseState<NodeInfo> for StatePg<'a> {
	async fn create_table(&self) -> Result<(), Error> {
		// Implementation for create_table method
		Ok(())
	}

	async fn create(&self, node_info: &NodeInfo) -> Result<(), Error> {
		let is_exisit = self.has_address_exists(&node_info.address).await?;
		if !is_exisit {
			let new_node_info = NewNodeInfo {
				address: hex::encode(node_info.address),
				ip_address: Some(hex::encode(node_info.data.ip_address.clone())),
				metadata: Some(hex::encode(node_info.data.metadata.clone())),
				cluster_address: Some(hex::encode(node_info.data.cluster_address.clone())),
				signature: Some(hex::encode(node_info.signature.clone())),
				verifying_key: Some(hex::encode(node_info.verifying_key.clone())),
			};

			match &self.pg.conn {
				PgConnectionType::TxConn(conn) => diesel::insert_into(schema::node_info::table)
					.values(new_node_info)
					.execute(*conn.lock().await),
				PgConnectionType::PgConn(conn) => diesel::insert_into(schema::node_info::table)
					.values(new_node_info)
					.execute(&mut *conn.lock().await),
			}?;
			Ok(())
		} else {
			Ok(()) // Explicitly return Ok(()) if the address exists
		}
	}

	async fn update(&self, node_inf: &NodeInfo) -> Result<(), Error> {
		use db::postgres::schema::node_info::dsl::*;

		let addr = hex::encode(node_inf.address);
		match &self.pg.conn {
			PgConnectionType::TxConn(conn) => diesel::update(node_info.filter(address.eq(addr)))
				.set((
					ip_address.eq(hex::encode(node_inf.data.ip_address.clone())),
					metadata.eq(hex::encode(node_inf.data.metadata.clone())),
					cluster_address.eq(hex::encode(node_inf.data.cluster_address.clone())),
					signature.eq(hex::encode(node_inf.signature.clone())),
					verifying_key.eq(hex::encode(node_inf.verifying_key.clone())),
				)) // set new values for balance and nonce
				.execute(*conn.lock().await),
			PgConnectionType::PgConn(conn) => diesel::update(node_info.filter(address.eq(addr)))
				.set((
					ip_address.eq(hex::encode(node_inf.data.ip_address.clone())),
					metadata.eq(hex::encode(node_inf.data.metadata.clone())),
					cluster_address.eq(hex::encode(node_inf.data.cluster_address.clone())),
					signature.eq(hex::encode(node_inf.signature.clone())),
					verifying_key.eq(hex::encode(node_inf.verifying_key.clone())),
				)) // set new values for balance and nonce
				.execute(&mut *conn.lock().await),
		}?;
		Ok(())
	}

	async fn raw_query(&self, query: &str) -> Result<(), Error> {
		match &self.pg.conn {
			PgConnectionType::TxConn(conn) => diesel::sql_query(query).execute(*conn.lock().await),
			PgConnectionType::PgConn(conn) =>
				diesel::sql_query(query).execute(&mut *conn.lock().await),
		}?;
		Ok(())
	}

	async fn set_schema_version(&self, _version: u32) -> Result<(), Error> {
		// Implementation for set_schema_version method
		todo!()
	}
}

#[async_trait]
impl<'a> NodeInfoState for StatePg<'a> {
	async fn store_nodes(
		&self,
		clusters: &HashMap<Address, HashMap<Address, NodeInfo>>,
	) -> Result<(), Error> {
		for (_cluster_address, full_node_infos) in clusters {
			for (_address, node_info) in full_node_infos {
				self.create(node_info).await?;
			}
		}
		Ok(())
	}

	async fn find_node_info(
		&self,
		address: &Address,
	) -> Result<(Option<Address>, Option<NodeInfo>), Error> {
		let encode_addr = hex::encode(address);

		let res: QueryResult<QueryNodeInfo> = match &self.pg.conn {
			PgConnectionType::TxConn(conn) => schema::node_info::table
				.filter(schema::node_info::dsl::address.eq(encode_addr))
				.first(*conn.lock().await),
			PgConnectionType::PgConn(conn) => schema::node_info::table
				.filter(schema::node_info::dsl::address.eq(encode_addr))
				.first(&mut *conn.lock().await),
		};

		match res {
			Ok(query_results) => {
				let mut address: Address = [0; 20];
				let mut cluster_address: Address = [0; 20];
				let from = hex::decode(query_results.address.clone())?;

				if from.len() == 20 {
					let mut array = [0u8; 20];
					array.copy_from_slice(&from);
					address = array;
				}

				let ch = hex::decode(query_results.cluster_address.clone().unwrap())?;
				if ch.len() == 20 {
					let mut array = [0u8; 20];
					array.copy_from_slice(&ch);
					cluster_address = array;
				}

				let metadata =
					hex::decode(query_results.metadata.clone().unwrap_or_else(|| "".to_string()))?;
				let ip_address = hex::decode(
					query_results.ip_address.clone().unwrap_or_else(|| "".to_string()),
				)?;
				let signature =
					hex::decode(query_results.signature.clone().unwrap_or_else(|| "".to_string()))?;

				let verifying_key = hex::decode(
					query_results.verifying_key.clone().unwrap_or_else(|| "".to_string()),
				)?;

				let data = NodeInfoSignPayload { ip_address, metadata, cluster_address };
				let node_info = NodeInfo { data, address, signature, verifying_key };

				Ok((Some(cluster_address), Some(node_info)))
			},
			Err(e) => return Err(anyhow::anyhow!("Diesel query failed: {}", e)),
		}
	}

	async fn load_node_info(&self, address: &Address) -> Result<NodeInfo, Error> {
		let encode_addr = hex::encode(address);

		let res: QueryResult<QueryNodeInfo> = match &self.pg.conn {
			PgConnectionType::TxConn(conn) => schema::node_info::table
				.filter(schema::node_info::dsl::address.eq(encode_addr))
				.get_result(*conn.lock().await),
			PgConnectionType::PgConn(conn) => schema::node_info::table
				.filter(schema::node_info::dsl::address.eq(encode_addr))
				.get_result(&mut *conn.lock().await),
		};

		match res {
			Ok(query_results) => {
				let mut address: Address = [0; 20];
				let mut cluster_address: Address = [0; 20];
				let from = hex::decode(query_results.address.clone())?;

				if from.len() == 20 {
					let mut array = [0u8; 20];
					array.copy_from_slice(&from);
					address = array;
				}

				let ch = hex::decode(query_results.cluster_address.clone().unwrap())?;
				if ch.len() == 20 {
					let mut array = [0u8; 20];
					array.copy_from_slice(&ch);
					cluster_address = array;
				}

				let metadata =
					hex::decode(query_results.metadata.clone().unwrap_or_else(|| "".to_string()))?;
				let ip_address = hex::decode(
					query_results.ip_address.clone().unwrap_or_else(|| "".to_string()),
				)?;
				let signature =
					hex::decode(query_results.signature.clone().unwrap_or_else(|| "".to_string()))?;

				let verifying_key = hex::decode(
					query_results.verifying_key.clone().unwrap_or_else(|| "".to_string()),
				)?;
				print!("the value in ip {:?}", ip_address);
				let node_info = NodeInfo::new(
					address,
					ip_address,
					metadata,
					cluster_address,
					signature,
					verifying_key,
				);
				Ok(node_info)
			},
			Err(e) => return Err(anyhow::anyhow!("Diesel query failed: {}", e)),
		}
	}

	async fn load_nodes(
		&self,
		cluster_address: &Address,
	) -> Result<HashMap<Address, NodeInfo>, Error> {
		let mut cluster: HashMap<_, _> = HashMap::new();
		let encode_address = hex::encode(cluster_address);

		let res: QueryResult<Vec<QueryNodeInfo>> = match &self.pg.conn {
			PgConnectionType::TxConn(conn) => schema::node_info::table
				.filter(schema::node_info::dsl::cluster_address.eq(encode_address))
				.load(*conn.lock().await),
			PgConnectionType::PgConn(conn) => schema::node_info::table
				.filter(schema::node_info::dsl::cluster_address.eq(encode_address))
				.load(&mut *conn.lock().await),
		};

		match res {
			Ok(results) => {
				for query_results in results {
					let mut address: Address = [0; 20];
					let mut cluster_address: Address = [0; 20];
					let from = hex::decode(query_results.address.clone())?;

					if from.len() == 20 {
						let mut array = [0u8; 20];
						array.copy_from_slice(&from);
						address = array;
					}

					let ch = hex::decode(query_results.cluster_address.clone().unwrap())?;
					if ch.len() == 20 {
						let mut array = [0u8; 20];
						array.copy_from_slice(&ch);
						cluster_address = array;
					}

					let metadata = hex::decode(
						query_results.metadata.clone().unwrap_or_else(|| "".to_string()),
					)?;
					let ip_address = hex::decode(
						query_results.ip_address.clone().unwrap_or_else(|| "".to_string()),
					)?;
					let signature = hex::decode(
						query_results.signature.clone().unwrap_or_else(|| "".to_string()),
					)?;

					let verifying_key = hex::decode(
						query_results.verifying_key.clone().unwrap_or_else(|| "".to_string()),
					)?;

					let data = NodeInfoSignPayload { ip_address, metadata, cluster_address };
					let node_info = NodeInfo { data, address, signature, verifying_key };
					cluster.insert(address, node_info);
				}
				Ok(cluster)
			},
			Err(e) => return Err(anyhow::anyhow!("Diesel query failed: {}", e)),
		}
	}

	async fn remove_node_info(&self, address: &Address) -> Result<(), Error> {
		let encode_address = hex::encode(address);
		match &self.pg.conn {
			PgConnectionType::TxConn(conn) => diesel::delete(
				schema::node_info::table.filter(schema::node_info::address.eq(encode_address)),
			)
			.execute(*conn.lock().await),
			PgConnectionType::PgConn(conn) => diesel::delete(
				schema::node_info::table.filter(schema::node_info::address.eq(encode_address)),
			)
			.execute(&mut *conn.lock().await),
		}?;
		Ok(())
	}

	async fn has_address_exists(&self, address: &Address) -> Result<bool, Error> {
		let encoded_address = hex::encode(address);

		let res = match &self.pg.conn {
			PgConnectionType::TxConn(conn) => schema::node_info::table
				.filter(schema::node_info::dsl::address.eq(encoded_address))
				.load::<QueryNodeInfo>(*conn.lock().await),
			PgConnectionType::PgConn(conn) => schema::node_info::table
				.filter(schema::node_info::dsl::address.eq(encoded_address))
				.load::<QueryNodeInfo>(&mut *conn.lock().await),
		}?;
		Ok(!res.is_empty())
	}
}
