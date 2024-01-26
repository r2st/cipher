#[cfg(test)]
mod tests {
	use crate::{
		node_info_manager::*,
		node_info_state::{NodeInfoState, StateInternalImpl},
	};
	use anyhow::{anyhow, Error};
	use cluster::{cluster_manager::ClusterManager, cluster_state::ClusterState};
	use db::db::{Database, DbTxConn};
	use diesel::{
		dsl::date,
		pg,
		pg::PgConnection,
		prelude::*,
		r2d2::{ConnectionManager, Pool, PooledConnection},
		RunQueryDsl,
	};
	use primitives::*;
	use serde::{Deserialize, Serialize};
	use sha2::Digest;
	use std::collections::HashMap;
	use system::{
		account::Account,
		config::Config,
		node_info::{NodeInfo, NodeInfoSignPayload},
	};
	use vrf_helper::{common::SecpVRF, secp_vrf::KeySpace};

	#[derive(Debug, Serialize, Deserialize)]
	struct TXPayload {
		pub address: Address,
		pub ip_address: IpAddress,
		pub metadata: Metadata,
	}

	// Helper function to create a new State for testing
	async fn database_conn<'a>() -> Result<(DbTxConn<'a>, Config), Error> {
		let config_data = Config::default();
		Database::new_test(&config_data).await;
		let db_pool_conn = Database::get_test_connection().await.unwrap();
		Ok((db_pool_conn, config_data))
	}

	async fn perform_table_cleanup() {
		let (db_pool_conn, config) = database_conn().await.unwrap();
		let cluster_state = ClusterState::new(&db_pool_conn).await.unwrap();
		let node_info_state = NodeInfoState::new(&db_pool_conn).await.unwrap();

		truncate_cluster_table(&cluster_state).await;
		truncate_node_info_table(&node_info_state).await;
		// Create a ClusterManager instance
		let mut cluster_manager = ClusterManager {};
		let cluster_address = [10u8; 20];
		cluster_manager.create_cluster(&cluster_address, &cluster_state).await.unwrap();
	}

	pub async fn truncate_cluster_table<'a>(cluster_state: &ClusterState<'a>) {
		cluster_state.raw_query("TRUNCATE cluster_address;").await;
		cluster_state.raw_query("TRUNCATE cluster;").await;
	}
	pub async fn truncate_node_info_table<'a>(node_info_state: &NodeInfoState<'a>) {
		node_info_state.raw_query("TRUNCATE node_info;").await;
	}

	#[tokio::test]
	#[serial_test::serial]
	async fn test_store_and_load_node_info() {
		let (db_pool_conn, config) = database_conn().await.unwrap();
		let node_info_state = NodeInfoState::new(&db_pool_conn).await.unwrap();
		perform_table_cleanup().await;

		let key_space = KeySpace::new();

		let public_key = key_space.public_key;
		let address = Account::address(&public_key.serialize().to_vec().clone()).unwrap();

		let payload = TXPayload {
			address: address.clone(),
			ip_address: "127.0.0.1".as_bytes().to_vec(),
			metadata: "metadata".as_bytes().to_vec(),
		};
		let signature = payload.sign_with_ecdsa(key_space.secret_key).unwrap();

		let cluster_address = [11u8; 20];
		let node_info = NodeInfo::new(
			address,
			"127.0.0.1".as_bytes().to_vec(),
			"metadata".as_bytes().to_vec(),
			cluster_address,
			signature.serialize_compact().to_vec(),
			public_key.serialize().to_vec(),
		);
		node_info_state.store_node_info(&node_info.clone()).await.unwrap();
		let loaded_node_info = node_info_state.load_node_info(&address).await.unwrap();
		assert_eq!(node_info, loaded_node_info);
	}

	#[tokio::test]
	#[serial_test::serial]
	async fn test_store_and_load_nodes() {
		let (db_pool_conn, config) = database_conn().await.unwrap();
		let node_info_state = NodeInfoState::new(&db_pool_conn).await.unwrap();
		perform_table_cleanup().await;

		let cluster_address = [11u8; 20];

		let key_space1 = KeySpace::new();
		let public_key_1 = key_space1.public_key;
		let account1 = Account::address(&public_key_1.serialize().to_vec().clone()).unwrap();

		let payload1 = TXPayload {
			address: account1.clone(),
			ip_address: "127.0.0.1".as_bytes().to_vec(),
			metadata: "metadata1".as_bytes().to_vec(),
		};

		let signature1 = payload1.sign_with_ecdsa(key_space1.secret_key).unwrap();

		let key_space2 = KeySpace::new();
		let public_key_2 = key_space2.public_key;
		let account2 = Account::address(&public_key_2.serialize().to_vec().clone()).unwrap();

		let payload2 = TXPayload {
			address: account2.clone(),
			ip_address: "127.0.0.1".as_bytes().to_vec(),
			metadata: "metadata2".as_bytes().to_vec(),
		};

		let signature2 = payload2.sign_with_ecdsa(key_space2.secret_key).unwrap();

		let full_node_info1 = NodeInfo::new(
			account1,
			"127.0.0.1".as_bytes().to_vec(),
			"metadata1".as_bytes().to_vec(),
			cluster_address,
			signature1.serialize_compact().to_vec(),
			public_key_1.serialize().to_vec(),
		);
		let full_node_info2 = NodeInfo::new(
			account2,
			"127.0.0.1".as_bytes().to_vec(),
			"metadata2".as_bytes().to_vec(),
			cluster_address,
			signature2.serialize_compact().to_vec(),
			public_key_2.serialize().to_vec(),
		);

		let mut cluster = HashMap::new();
		cluster.insert(account1, full_node_info1.clone());
		cluster.insert(account2, full_node_info2.clone());

		node_info_state
			.store_nodes(&[(cluster_address, cluster.clone())].iter().cloned().collect())
			.await
			.unwrap();

		let loaded_cluster = node_info_state.load_nodes(&cluster_address).await.unwrap();
		assert_eq!(cluster, loaded_cluster);
	}

	//
	#[tokio::test]
	#[serial_test::serial]
	async fn test_remove_node_info() {
		// Create a new NodeInfoState
		let (db_pool_conn, config) = database_conn().await.unwrap();
		let node_info_state = NodeInfoState::new(&db_pool_conn).await.unwrap();
		perform_table_cleanup().await;
		// Define sample cluster nonce and address
		let cluster_address = [11u8; 20];

		let key_space = KeySpace::new();
		let public_key = key_space.public_key;
		let address = Account::address(&public_key.serialize().to_vec().clone()).unwrap();

		let payload = TXPayload {
			address: address.clone(),
			ip_address: "127.0.0.1".as_bytes().to_vec(),
			metadata: "metadata".as_bytes().to_vec(),
		};

		let signature = payload.sign_with_ecdsa(key_space.secret_key).unwrap();

		// Define sample full node info
		let node_info = NodeInfo::new(
			address,
			"127.0.0.1".as_bytes().to_vec(),
			"metadata".as_bytes().to_vec(),
			cluster_address,
			signature.serialize_compact().to_vec(),
			public_key.serialize().to_vec(),
		);

		// Store the full node info
		node_info_state.store_node_info(&node_info.clone()).await.unwrap();

		// Remove the full node info
		node_info_state.remove_node_info(&address).await.unwrap();

		// Attempt to load the removed full node info
		let loaded_info = node_info_state.load_node_info(&address).await;

		// Assert that the loaded info is None (indicating it was successfully removed)
		// assert_eq!(loaded_info.unwrap_err().to_string(), "Expected a single row, found 0 rows");
		assert!(loaded_info.is_err());
	}

	#[tokio::test]
	#[serial_test::serial]
	async fn test_find_node_info() {
		let cluster_address = [11u8; 20];

		let key_space = KeySpace::new();
		let public_key = key_space.public_key;
		let address = Account::address(&public_key.serialize().to_vec().clone()).unwrap();

		let payload = TXPayload {
			address: address.clone(),
			ip_address: "127.0.0.1".as_bytes().to_vec(),
			metadata: "metadata".as_bytes().to_vec(),
		};

		let signature = payload.sign_with_ecdsa(key_space.secret_key).unwrap();

		let (db_pool_conn, config) = database_conn().await.unwrap();
		let node_info_state = NodeInfoState::new(&db_pool_conn).await.unwrap();
		perform_table_cleanup().await;

		// Store full node info
		let node_info = NodeInfo::new(
			address,
			"127.0.0.1".as_bytes().to_vec(),
			"metadata".as_bytes().to_vec(),
			cluster_address,
			signature.serialize_compact().to_vec(),
			public_key.serialize().to_vec(),
		);
		node_info_state.store_node_info(&node_info.clone()).await.unwrap();

		// Find node info
		let (found_cluster_address, found_node_info) =
			node_info_state.find_node_info(&address).await.unwrap();

		assert_eq!(found_cluster_address, Some(cluster_address));
		assert_eq!(found_node_info, Some(node_info));
	}

	#[tokio::test]
	#[serial_test::serial]
	async fn test_join_cluster() {
		// Create a new NodeInfoState instance
		let (db_pool_conn, config) = database_conn().await.unwrap();
		let node_info_state = NodeInfoState::new(&db_pool_conn).await.unwrap();
		perform_table_cleanup().await;
		let key_space = KeySpace::new();
		let public_key = key_space.public_key;
		let ip_address = "127.0.0.1".as_bytes().to_vec();
		let metadata = "metadata".as_bytes().to_vec();
		let address = Account::address(&public_key.serialize().to_vec().clone()).unwrap();
		let cluster_address = [11u8; 20];

		let node_info_payload = NodeInfoSignPayload {
			ip_address: ip_address.clone(),
			metadata: metadata.clone(),
			cluster_address,
		};

		let signature = node_info_payload.sign_with_ecdsa(key_space.secret_key).unwrap();

		let node_info = NodeInfo::new(
			address,
			ip_address,
			metadata,
			cluster_address,
			signature.serialize_compact().to_vec(),
			public_key.serialize().to_vec(),
		);

		let mut cluster_register_manager = NodeInfoManager {};
		// Invoke the join_cluster function
		let result = cluster_register_manager
			.join_cluster(&node_info.clone(), &cluster_address, &node_info_state)
			.await
			.unwrap();

		// Assert the result
		//assert!(result.is_ok());

		// Check if the node_info is stored in the cluster
		let (found_cluster_address, loaded_node_info) =
			node_info_state.find_node_info(&node_info.address).await.unwrap();
		assert_eq!(found_cluster_address, Some(cluster_address));
		assert_eq!(loaded_node_info, Some(node_info));
	}

	#[tokio::test]
	#[serial_test::serial]
	async fn test_leave_cluster() {
		// Create a new NodeInfoState
		let (db_pool_conn, config) = database_conn().await.unwrap();
		let node_info_state = NodeInfoState::new(&db_pool_conn).await.unwrap();
		perform_table_cleanup().await;
		// Define sample cluster nonce and address
		let cluster_address = [11u8; 20];
		let key_space = KeySpace::new();
		let public_key = key_space.public_key;

		let ip_address = "127.0.0.1".as_bytes().to_vec();
		let metadata = "metadata".as_bytes().to_vec();
		let address = Account::address(&public_key.serialize().to_vec().clone()).unwrap();

		let node_info_payload = NodeInfoSignPayload {
			ip_address: ip_address.clone(),
			metadata: metadata.clone(),
			cluster_address,
		};

		let signature = node_info_payload.sign_with_ecdsa(key_space.secret_key).unwrap();

		// Define sample full node info
		let node_info = NodeInfo::new(
			address,
			ip_address,
			metadata,
			cluster_address,
			signature.serialize_compact().to_vec(),
			public_key.serialize().to_vec(),
		);

		// Store the full node info
		node_info_state.store_node_info(&node_info.clone()).await.unwrap();

		// Create a new NodeInfoManager
		let mut cluster_register_manager = NodeInfoManager {};

		// Call the leave_cluster function
		cluster_register_manager
			.leave_cluster(&node_info.clone(), &node_info_state)
			.await
			.unwrap();

		// Attempt to load the removed full node info
		let loaded_info = node_info_state.load_node_info(&address).await;

		// Assert that the loaded info is None (indicating it was successfully removed)
		// assert_eq!(loaded_info.unwrap_err().to_string(), "Expected a single row, found 0 rows");
		assert!(loaded_info.is_err());
	}

	#[tokio::test]
	#[serial_test::serial]
	async fn test_update_cluster() {
		// Create a new NodeInfoState
		let (db_pool_conn, config) = database_conn().await.unwrap();
		let node_info_state = NodeInfoState::new(&db_pool_conn).await.unwrap();
		perform_table_cleanup().await;
		// Define sample cluster nonce and address
		let cluster_address = [11u8; 20];

		let key_space = KeySpace::new();
		let public_key = key_space.public_key;
		let ip_address = "127.0.0.1".as_bytes().to_vec();
		let metadata = "metadata".as_bytes().to_vec();
		let address = Account::address(&public_key.serialize().to_vec().clone()).unwrap();
		let cluster_address = [11u8; 20];

		let node_info_payload = NodeInfoSignPayload {
			ip_address: ip_address.clone(),
			metadata: metadata.clone(),
			cluster_address,
		};

		let signature = node_info_payload.sign_with_ecdsa(key_space.secret_key).unwrap();

		let node_info = NodeInfo::new(
			address,
			ip_address.clone(),
			metadata,
			cluster_address,
			signature.serialize_compact().to_vec(),
			public_key.serialize().to_vec(),
		);

		// Store the full node info
		node_info_state.store_node_info(&node_info.clone()).await.unwrap();

		// Create a new NodeInfoManager
		let mut cluster_register_manager = NodeInfoManager {};

		// Update metadata
		let metadata = "Updated metadata".as_bytes().to_vec();

		let node_info_payload = NodeInfoSignPayload {
			ip_address: ip_address.clone(),
			metadata: metadata.clone(),
			cluster_address,
		};

		let signature = node_info_payload.sign_with_ecdsa(key_space.secret_key).unwrap();

		let updated_node_info = NodeInfo::new(
			address,
			ip_address,
			metadata,
			cluster_address,
			signature.serialize_compact().to_vec(),
			public_key.serialize().to_vec(),
		);

		// Call the update_cluster function
		cluster_register_manager
			.update_cluster(&updated_node_info.clone(), &node_info_state)
			.await
			.unwrap();

		// Load the updated full node info
		let loaded_info = node_info_state.load_node_info(&address).await.unwrap();

		// Assert that the loaded info matches the updated info
		assert_eq!(loaded_info, updated_node_info);
	}
}
