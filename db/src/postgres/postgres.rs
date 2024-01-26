use crate::postgres::config::Config as PgConfig;
use diesel::{
	deserialize::QueryableByName,
	dsl::sql_query,
	pg::PgConnection,
	prelude::*,
	r2d2::{ConnectionManager, Pool, PooledConnection},
	sql_types::Bool,
};
use diesel_migrations::{embed_migrations, EmbeddedMigrations, MigrationHarness};
use once_cell::sync::Lazy;
use std::sync::Arc;
use system::config::Config as SystemConfig;
use tokio::sync::Mutex;

pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("./src/postgres/migrations/");

static DB_POOL: Lazy<Arc<Mutex<Option<Pool<ConnectionManager<PgConnection>>>>>> =
	Lazy::new(|| Arc::new(Mutex::new(None)));

#[derive(QueryableByName, Debug)]
struct Exists {
	#[sql_type = "Bool"]
	exists: bool,
}
pub struct PostgresDBPool {
	pub conn: PooledConnection<ConnectionManager<PgConnection>>,
	pub config: PgConfig,
}

pub struct PostgresDB {
	pub conn: PgConnection,
	pub config: PgConfig,
}

#[derive(Clone)]
pub enum PgConnectionType<'a> {
	TxConn(Arc<Mutex<&'a mut PgConnection>>),
	PgConn(Arc<Mutex<PooledConnection<ConnectionManager<PgConnection>>>>),
}
#[derive(Clone)]
pub struct PostgresDBConn<'a> {
	pub conn: PgConnectionType<'a>,
	pub config: PgConfig,
}

impl PostgresDBPool {
	pub async fn new(database_url: &str, cfg: PgConfig) -> anyhow::Result<PostgresDBPool> {
		let new_conn: PooledConnection<ConnectionManager<PgConnection>> =
			Self::get_pool_conn_arc(database_url, cfg.clone()).await?;
		Ok(PostgresDBPool { conn: new_conn, config: cfg })
	}

	pub async fn initialize(database_url: &str, cfg: PgConfig) -> anyhow::Result<()> {
		let manager = ConnectionManager::new(database_url);
		let pool = Pool::builder().max_size(cfg.pool_size.clone()).build(manager)?;
		let mut conn: PooledConnection<ConnectionManager<PgConnection>> = pool.get()?;
		let database_exists = sql_query(format!(
			"SELECT EXISTS(SELECT datname FROM pg_catalog.pg_database WHERE datname = '{}');",
			cfg.postgres_db_name.clone()
		))
		.get_result::<Exists>(&mut conn)
		.optional()?
		.map(|res| res.exists)
		.unwrap_or(false);

		// Create the database if it does not exist
		if !database_exists {
			sql_query(format!("CREATE DATABASE {};", cfg.postgres_db_name.clone()))
				.execute(&mut conn)
				.unwrap();
		}
		drop(conn);
		let mut pg = Self::new(database_url, cfg).await?;
		pg.conn.run_pending_migrations(MIGRATIONS).map_err(|e| anyhow::anyhow!(e))?;
		Ok(())
	}

	pub async fn re_initialize(database_url: &str, cfg: PgConfig) -> anyhow::Result<()> {
		let manager = ConnectionManager::new(database_url);
		let pool = Pool::builder().max_size(cfg.pool_size.clone()).build(manager)?;
		let mut conn: PooledConnection<ConnectionManager<PgConnection>> = pool.get()?;
		let database_exists = sql_query(format!(
			"SELECT EXISTS(SELECT datname FROM pg_catalog.pg_database WHERE datname = '{}');",
			cfg.postgres_db_name.clone()
		))
		.get_result::<Exists>(&mut conn)
		.optional()?
		.map(|res| res.exists)
		.unwrap_or(false);

		// Create the database if it does not exist
		if !database_exists {
			sql_query(format!("CREATE DATABASE {};", cfg.postgres_db_name.clone()))
				.execute(&mut conn)
				.unwrap();
		} else {
			sql_query(format!(
				"SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity WHERE pg_stat_activity.datname = '{}'",
				cfg.postgres_db_name.clone()))
				.execute(&mut conn)
				.unwrap();
			sql_query(format!("DROP DATABASE IF EXISTS {};", cfg.postgres_db_name.clone()))
				.execute(&mut conn)
				.unwrap();
			sql_query(format!("CREATE DATABASE {};", cfg.postgres_db_name.clone()))
				.execute(&mut conn)
				.unwrap();
		}
		drop(conn);
		let mut pg = Self::new(database_url, cfg).await?;
		pg.conn.run_pending_migrations(MIGRATIONS).map_err(|e| anyhow::anyhow!(e))?;
		Ok(())
	}

	/*fn get_pool_conn(
		database_url: &str,
		cfg: PgConfig,
	) -> anyhow::Result<PooledConnection<ConnectionManager<PgConnection>>> {
		let new_database_url =
			format!("{}/{}", database_url.trim_end_matches('/'), cfg.postgres_db_name.clone());
		println!("new_database_url: {:?}", new_database_url);
		let new_manager = ConnectionManager::new(new_database_url.clone());
		let new_pool = Pool::builder().max_size(cfg.pool_size).build(new_manager)?;
		let mut new_conn: PooledConnection<ConnectionManager<PgConnection>> = new_pool.get()?;
		new_conn.run_pending_migrations(MIGRATIONS).map_err(|e| anyhow::anyhow!(e))?;
		Ok(new_conn)
	}*/

	async fn get_pool_conn_arc(
		database_url: &str,
		cfg: PgConfig,
	) -> anyhow::Result<PooledConnection<ConnectionManager<PgConnection>>> {
		let mut lock = DB_POOL.lock().await;
		if let Some(new_pool) = &*lock {
			let mut new_conn: PooledConnection<ConnectionManager<PgConnection>> = new_pool.get()?;
			new_conn.run_pending_migrations(MIGRATIONS).map_err(|e| anyhow::anyhow!(e))?;
			Ok(new_conn)
		} else {
			let new_database_url =
				format!("{}/{}", database_url.trim_end_matches('/'), cfg.postgres_db_name.clone());
			println!("new_database_url: {:?}", new_database_url);
			let new_manager = ConnectionManager::new(new_database_url.clone());
			let new_pool = Pool::builder().max_size(cfg.pool_size).build(new_manager)?;
			let new_conn: PooledConnection<ConnectionManager<PgConnection>> = new_pool.get()?;
			*lock = Some(new_pool.clone());
			Ok(new_conn)
		}
	}

	pub fn new_pg_connection(database_url: &str, cfg: PgConfig) -> anyhow::Result<PostgresDB> {
		let new_database_url =
			format!("{}/{}", database_url.trim_end_matches('/'), cfg.postgres_db_name.clone());
		println!("new_database_url: {:?}", new_database_url);
		let mut new_conn =
			PgConnection::establish(&new_database_url).expect("Error connecting to database");
		new_conn.run_pending_migrations(MIGRATIONS).map_err(|e| anyhow::anyhow!(e))?;
		Ok(PostgresDB { conn: new_conn, config: cfg })
	}

	pub async fn new_pool_conn_from_config(
		config: &SystemConfig,
		postgres_db_name: String,
	) -> anyhow::Result<PostgresDBPool> {
		let cfg = PgConfig {
			db_url: config.postgres_host.clone(),
			pool_size: config.postgres_pool_size.clone(),
			postgres_username: config.postgres_username.clone(),
			postgres_password: config.postgres_password.clone(),
			postgres_db_name,
			dev_mode: config.dev_mode,
		};
		let url = format!(
			"postgres://{}:{}@{}",
			cfg.postgres_username.clone(),
			cfg.postgres_password.clone(),
			cfg.db_url.clone()
		);
		Self::new(&url, cfg).await
	}

	pub fn new_pg_conn_from_config(
		config: &SystemConfig,
		postgres_db_name: String,
	) -> anyhow::Result<PostgresDB> {
		let cfg = PgConfig {
			db_url: config.postgres_host.clone(),
			pool_size: config.postgres_pool_size.clone(),
			postgres_username: config.postgres_username.clone(),
			postgres_password: config.postgres_password.clone(),
			postgres_db_name,
			dev_mode: config.dev_mode,
		};
		let url = format!(
			"postgres://{}:{}@{}",
			cfg.postgres_username.clone(),
			cfg.postgres_password.clone(),
			cfg.db_url.clone()
		);
		Self::new_pg_connection(&url, cfg)
	}

	pub async fn initialize_from_config(
		config: &SystemConfig,
		postgres_db_name: String,
	) -> anyhow::Result<()> {
		let cfg = PgConfig {
			db_url: config.postgres_host.clone(),
			pool_size: config.postgres_pool_size.clone(),
			postgres_username: config.postgres_username.clone(),
			postgres_password: config.postgres_password.clone(),
			postgres_db_name,
			dev_mode: config.dev_mode,
		};
		let url = format!(
			"postgres://{}:{}@{}",
			cfg.postgres_username.clone(),
			cfg.postgres_password.clone(),
			cfg.db_url.clone()
		);
		Self::initialize(&url, cfg).await
	}

	pub async fn re_initialize_from_config(
		config: &SystemConfig,
		postgres_db_name: String,
	) -> anyhow::Result<()> {
		let cfg = PgConfig {
			db_url: config.postgres_host.clone(),
			pool_size: config.postgres_pool_size.clone(),
			postgres_username: config.postgres_username.clone(),
			postgres_password: config.postgres_password.clone(),
			postgres_db_name,
			dev_mode: config.dev_mode,
		};
		let url = format!(
			"postgres://{}:{}@{}",
			cfg.postgres_username.clone(),
			cfg.postgres_password.clone(),
			cfg.db_url.clone()
		);
		Self::re_initialize(&url, cfg).await
	}
}
