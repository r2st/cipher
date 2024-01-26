use crate::{state_cas::StateCas, state_pg::StatePg, state_rock::StateRock};
use anyhow::Error;
use db::db::DbTxConn;
use db_traits::{base::BaseState, node_info::NodeInfoState as NodeInfoStateInternal};

use primitives::*;
use rocksdb::{DBWithThreadMode, MultiThreaded, Options};
use std::{collections::HashMap, sync::Arc};
use system::node_info::NodeInfo;

pub enum StateInternalImpl<'a> {
	StateRock(StateRock),
	StatePg(StatePg<'a>),
	StateCas(StateCas),
}
pub struct NodeInfoState<'a> {
	pub state: Arc<StateInternalImpl<'a>>,
}

impl<'a> NodeInfoState<'a> {
	pub async fn new(db_pool_conn: &'a DbTxConn<'a>) -> Result<Self, Error> {
		let state: StateInternalImpl<'a>;

		match &db_pool_conn {
			DbTxConn::POSTGRES(pg) => {
				state = StateInternalImpl::StatePg(StatePg { pg });
			},
			DbTxConn::CASSANDRA(session) => {
				state = StateInternalImpl::StateCas(StateCas { session: session.clone() });
			},
			DbTxConn::ROCKSDB(db_path) => {
				let db_path = format!("{}/node_info", db_path);
				let mut options = Options::default();
				options.create_if_missing(true);

				state = StateInternalImpl::StateRock(StateRock {
					db_path: db_path.clone(),
					db: DBWithThreadMode::<MultiThreaded>::open(&options, db_path.clone())?,
				});
			},
		}

		let state = NodeInfoState { state: Arc::new(state) };

		state.create_table().await?;
		Ok(state)
	}

	pub async fn create_table(&self) -> Result<(), Error> {
		match &*self.state {
			StateInternalImpl::StateRock(s) => s.create_table().await,
			StateInternalImpl::StatePg(s) => s.create_table().await,
			StateInternalImpl::StateCas(s) => s.create_table().await,
		}
	}

	pub async fn raw_query(&self, query: &str) -> Result<(), Error> {
		match &*self.state {
			StateInternalImpl::StateRock(s) => s.raw_query(query).await,
			StateInternalImpl::StatePg(s) => s.raw_query(query).await,
			StateInternalImpl::StateCas(s) => s.raw_query(query).await,
		}
	}

	pub async fn store_node_info(&self, node_info: &NodeInfo) -> Result<(), Error> {
		match &*self.state {
			StateInternalImpl::StateRock(s) => s.create(node_info).await,
			StateInternalImpl::StatePg(s) => s.create(node_info).await,
			StateInternalImpl::StateCas(s) => s.create(node_info).await,
		}
	}

	pub async fn update_node_info(&self, node_info: &NodeInfo) -> Result<(), Error> {
		match &*self.state {
			StateInternalImpl::StateRock(s) => s.update(node_info).await,
			StateInternalImpl::StatePg(s) => s.update(node_info).await,
			StateInternalImpl::StateCas(s) => s.update(node_info).await,
		}
	}

	pub async fn store_nodes(
		&self,
		clusters: &HashMap<Address, HashMap<Address, NodeInfo>>,
	) -> Result<(), Error> {
		match &*self.state {
			StateInternalImpl::StateRock(s) => s.store_nodes(clusters).await,
			StateInternalImpl::StatePg(s) => s.store_nodes(clusters).await,
			StateInternalImpl::StateCas(s) => s.store_nodes(clusters).await,
		}
	}

	pub async fn find_node_info(
		&self,
		address: &Address,
	) -> Result<(Option<Address>, Option<NodeInfo>), Error> {
		match &*self.state {
			StateInternalImpl::StateRock(s) => s.find_node_info(address).await,
			StateInternalImpl::StatePg(s) => s.find_node_info(address).await,
			StateInternalImpl::StateCas(s) => s.find_node_info(address).await,
		}
	}

	pub async fn load_node_info(&self, address: &Address) -> Result<NodeInfo, Error> {
		match &*self.state {
			StateInternalImpl::StateRock(s) => s.load_node_info(address).await,
			StateInternalImpl::StatePg(s) => s.load_node_info(address).await,
			StateInternalImpl::StateCas(s) => s.load_node_info(address).await,
		}
	}

	pub async fn load_nodes(
		&self,
		cluster_address: &Address,
	) -> Result<HashMap<Address, NodeInfo>, Error> {
		match &*self.state {
			StateInternalImpl::StateRock(s) => s.load_nodes(cluster_address).await,
			StateInternalImpl::StatePg(s) => s.load_nodes(cluster_address).await,
			StateInternalImpl::StateCas(s) => s.load_nodes(cluster_address).await,
		}
	}

	pub async fn remove_node_info(&self, address: &Address) -> Result<(), Error> {
		match &*self.state {
			StateInternalImpl::StateRock(s) => s.remove_node_info(address).await,
			StateInternalImpl::StatePg(s) => s.remove_node_info(address).await,
			StateInternalImpl::StateCas(s) => s.remove_node_info(address).await,
		}
	}

	pub async fn has_address_exists(&self, address: &Address) -> Result<bool, Error> {
		match &*self.state {
			StateInternalImpl::StateRock(s) => s.has_address_exists(address).await,
			StateInternalImpl::StatePg(s) => s.has_address_exists(address).await,
			StateInternalImpl::StateCas(s) => s.has_address_exists(address).await,
		}
	}
}
