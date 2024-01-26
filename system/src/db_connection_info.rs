use anyhow::{anyhow, Error, Result};
use serde::{Deserialize, Serialize};

/// Specifies node database connection info
pub struct DbConnectionInfo {
	pub host: String,
	pub username: String,
	pub password: String,
	pub db_name: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[repr(i8)]
pub enum DbType {
	CASSANDRA = 0,
	POSTGRES = 1,
	ROCKSDB = 2,
}

impl TryInto<DbType> for i8 {
	type Error = Error;

	fn try_into(self) -> Result<DbType, Self::Error> {
		match self {
			0 => Ok(DbType::CASSANDRA),
			1 => Ok(DbType::POSTGRES),
			2 => Ok(DbType::ROCKSDB),
			_ => Err(anyhow!("Invalid Db type {}", self)),
		}
	}
}
