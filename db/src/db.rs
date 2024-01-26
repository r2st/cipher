use crate::{
	cassandra::DatabaseManager as DatabaseManagerCas,
	postgres::postgres::{PgConnectionType, PostgresDB, PostgresDBConn, PostgresDBPool},
};
use anyhow::{anyhow, Error, Result};

use scylla::Session;
use std::sync::Arc;
use system::{config::Config as SystemConfig, db_connection_info::DbType};
use tokio::sync::Mutex;

lazy_static::lazy_static! {
	pub static ref DB_CONFIG: Arc<Mutex<Option<Arc<SystemConfig>>>> = Arc::new(Mutex::new(None));
}

pub struct Database {
	pub db: DbConnType,
	pub config: SystemConfig,
}
#[derive(Clone)]
pub enum DbTxConn<'a> {
	POSTGRES(PostgresDBConn<'a>),
	CASSANDRA(Arc<Session>),
	ROCKSDB(String),
}

pub enum DbConnType {
	POSTGRES(PostgresDB),
	CASSANDRA(Arc<Session>),
	ROCKSDB(String),
}

impl Database {
	pub async fn new(config: &SystemConfig) {
		let mut lock = DB_CONFIG.lock().await;
		*lock = Some(Arc::new(config.clone()));
		match <i8 as TryInto<DbType>>::try_into(config.db_type).unwrap() {
			DbType::CASSANDRA => {
				let db = crate::cassandra::DatabaseManager::initialize(Some(config))
					.await
					.expect("error cassandra initialize");
				if !DatabaseManagerCas::is_session_healthy(db.clone()).await {
					DatabaseManagerCas::replace_session().await.unwrap();
				}
			},
			DbType::POSTGRES =>
				PostgresDBPool::initialize_from_config(config, config.postgres_db_name.clone())
					.await
					.expect("PG error: initialize_from_config"),
			DbType::ROCKSDB => {},
		}
	}

	pub async fn re_initialize(config: &SystemConfig) {
		let mut lock = DB_CONFIG.lock().await;
		*lock = Some(Arc::new(config.clone()));
		match <i8 as TryInto<DbType>>::try_into(config.db_type).unwrap() {
			DbType::CASSANDRA => {
				let db = crate::cassandra::DatabaseManager::initialize(Some(config))
					.await
					.expect("error cassandra initialize");
				if !DatabaseManagerCas::is_session_healthy(db.clone()).await {
					DatabaseManagerCas::replace_session().await.unwrap();
				}
			},
			DbType::POSTGRES =>
				PostgresDBPool::re_initialize_from_config(config, config.postgres_db_name.clone())
					.await
					.expect("PG error: re_initialize_from_config"),
			DbType::ROCKSDB => {},
		}
	}

	pub async fn new_test(config: &SystemConfig) {
		let mut lock = DB_CONFIG.lock().await;
		*lock = Some(Arc::new(config.clone()));
		match <i8 as TryInto<DbType>>::try_into(config.db_type).unwrap() {
			DbType::CASSANDRA => {
				let db = crate::cassandra::DatabaseManager::initialize(Some(config))
					.await
					.expect("error cassandra initialize");
				if !DatabaseManagerCas::is_session_healthy(db.clone()).await {
					DatabaseManagerCas::replace_session().await.unwrap();
				}
			},
			DbType::POSTGRES => {
				PostgresDBPool::initialize_from_config(
					config,
					config.postgres_test_db_name.clone(),
				)
				.await
				.unwrap();
			},
			DbType::ROCKSDB => {},
		}
	}

	pub async fn get_connection() -> Result<Database, Error> {
		let lock = DB_CONFIG.lock().await;
		if let Some(config) = &*lock {
			let db = match config.db_type.try_into()? {
				DbType::POSTGRES => DbConnType::POSTGRES(PostgresDBPool::new_pg_conn_from_config(
					&config.clone(),
					config.postgres_db_name.clone(),
				)?),
				DbType::CASSANDRA => DbConnType::CASSANDRA({
					let db = crate::cassandra::DatabaseManager::initialize(Some(&config)).await?;
					if !DatabaseManagerCas::is_session_healthy(db.clone()).await {
						DatabaseManagerCas::replace_session().await.unwrap();
					}
					db
				}),
				DbType::ROCKSDB =>
					DbConnType::ROCKSDB(crate::rocksdb::DatabaseManager::new(&config)),
			};
			Ok(Database {
				db,
				config: <system::config::Config as Clone>::clone(&(*Arc::clone(&config))).into(),
			})
		} else {
			Err(anyhow!("get_connection: DB is not initialized!"))
		}
	}

	pub async fn get_postgres_connection() -> Result<PostgresDB, Error> {
		let lock = DB_CONFIG.lock().await;
		if let Some(config) = &*lock {
			PostgresDBPool::new_pg_conn_from_config(
				&config.clone(),
				config.postgres_db_name.clone(),
			)
		} else {
			Err(anyhow!("get_connection: DB is not initialized!"))
		}
	}

	pub async fn get_pool_connection<'a>() -> Result<DbTxConn<'a>, Error> {
		let lock = DB_CONFIG.lock().await;
		if let Some(config) = &*lock {
			let conn: DbTxConn<'a> = match config.db_type.try_into()? {
				DbType::POSTGRES => {
					let pg = PostgresDBPool::new_pool_conn_from_config(
						&config.clone(),
						config.postgres_db_name.clone(),
					)
					.await?;
					let conn = PgConnectionType::PgConn(Arc::new(Mutex::new(pg.conn)));
					let p_conn = PostgresDBConn { conn, config: pg.config };
					DbTxConn::POSTGRES(p_conn)
				},
				DbType::CASSANDRA => DbTxConn::CASSANDRA({
					let db = crate::cassandra::DatabaseManager::initialize(Some(&config)).await?;
					if !DatabaseManagerCas::is_session_healthy(db.clone()).await {
						DatabaseManagerCas::replace_session().await.unwrap();
					}
					db
				}),
				DbType::ROCKSDB => DbTxConn::ROCKSDB(crate::rocksdb::DatabaseManager::new(&config)),
			};
			Ok(conn)
		} else {
			Err(anyhow!("get_pool_connection: DB is not initialized!"))
		}
	}

	pub async fn get_test_connection<'a>() -> Result<DbTxConn<'a>, Error> {
		let lock = DB_CONFIG.lock().await;
		if let Some(config) = &*lock {
			let conn: DbTxConn<'a> = match config.db_type.try_into()? {
				DbType::POSTGRES => {
					let pg = PostgresDBPool::new_pool_conn_from_config(
						&config.clone(),
						config.postgres_test_db_name.clone(),
					)
					.await?;
					let conn = PgConnectionType::PgConn(Arc::new(Mutex::new(pg.conn)));
					let p_conn = PostgresDBConn { conn, config: pg.config };
					DbTxConn::POSTGRES(p_conn)
				},
				DbType::CASSANDRA => DbTxConn::CASSANDRA({
					let db = crate::cassandra::DatabaseManager::initialize(Some(&config)).await?;
					if !DatabaseManagerCas::is_session_healthy(db.clone()).await {
						DatabaseManagerCas::replace_session().await.unwrap();
					}
					db
				}),
				DbType::ROCKSDB => DbTxConn::ROCKSDB(crate::rocksdb::DatabaseManager::new(&config)),
			};
			Ok(conn)
		} else {
			Err(anyhow!("get_test_connection: DB is not initialized!"))
		}
	}
}
