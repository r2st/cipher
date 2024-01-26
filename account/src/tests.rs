#[cfg(test)]
mod tests {
	use crate::{account_manager::*, account_state::*};
	use anyhow::Error;
	use db::db::{Database, DbTxConn};
	use primitives::Address;
	use serial_test::serial;
	use system::{
		account::{Account, AccountType},
		config::Config,
	};

	async fn database_conn<'a>() -> Result<(DbTxConn<'a>, Config), Error> {
		let config_data = Config::default();
		Database::new_test(&config_data).await;
		let db_pool_conn = Database::get_test_connection().await.unwrap();
		Ok((db_pool_conn, config_data))
	}

	async fn create_account<'a>(address: Address, account_state: &AccountState<'a>) -> Account {
		let account = Account::new(address);
		account_state.create_account(&account).await.unwrap();
		let loaded_account = account_state.get_account(&address).await.unwrap();
		assert_eq!(loaded_account.balance, account.balance);
		assert_eq!(loaded_account.nonce, account.nonce);
		assert_eq!(loaded_account.account_type, AccountType::User);
		account
	}

	pub async fn truncate_table<'a>(account_state: &AccountState<'a>) -> Result<(), Error> {
		let delete_query = "TRUNCATE account;";
		match account_state.raw_query(delete_query).await {
			Ok(_) => Ok(()),
			Err(e) => Err(e.into()), // Convert the error to the appropriate type
		}
	}

	#[tokio::test]
	#[serial]
	async fn test_create_account() {
		let (db_pool_conn, config) = database_conn().await.unwrap();
		let account_state = AccountState::new(&db_pool_conn).await.unwrap();
		truncate_table(&account_state).await.unwrap();

		let address: Address = [1; 20];
		create_account(address, &account_state).await;
	}

	#[tokio::test]
	#[serial]
	async fn test_update_account() {
		let (db_pool_conn, config) = database_conn().await.unwrap();
		let account_state = AccountState::new(&db_pool_conn).await.unwrap();
		truncate_table(&account_state).await.unwrap();

		let address: Address = [2; 20];
		let mut account = create_account(address, &account_state).await;
		account.balance = 200;
		account.nonce = 10;

		account_state.update_account(&account).await.unwrap();
		let loaded_account = account_state.get_account(&address).await.unwrap();
		assert_eq!(loaded_account.balance, account.balance);
		assert_eq!(loaded_account.nonce, account.nonce);
		assert_eq!(loaded_account.account_type, AccountType::User);
	}

	#[tokio::test]
	#[serial]
	async fn test_update_account_creates_new_account() {
		let (db_pool_conn, config) = database_conn().await.unwrap();
		let account_state = AccountState::new(&db_pool_conn).await.unwrap();
		truncate_table(&account_state).await.unwrap();

		let address: Address = [16; 20];
		let mut account = Account::new(address);
		account.balance = 200;
		account.nonce = 10;

		account_state.update_account(&account).await.unwrap();
		let loaded_account = account_state.get_account(&address).await.unwrap();
		assert_eq!(loaded_account.balance, account.balance);
		assert_eq!(loaded_account.nonce, account.nonce);
		assert_eq!(loaded_account.account_type, AccountType::User);
	}

	#[tokio::test]
	#[serial]
	async fn test_update_account_creates_new_system_account() {
		let (db_pool_conn, config) = database_conn().await.unwrap();
		let account_state = AccountState::new(&db_pool_conn).await.unwrap();
		truncate_table(&account_state).await.unwrap();

		let address: Address = [17; 20];
		let mut account = Account::new_system(address);
		account.balance = 200;
		account.nonce = 10;

		account_state.update_account(&account).await.unwrap();
		let loaded_account = account_state.get_account(&address).await.unwrap();
		assert_eq!(loaded_account.balance, account.balance);
		assert_eq!(loaded_account.nonce, account.nonce);
		assert_eq!(loaded_account.account_type, AccountType::System);
	}

	#[tokio::test]
	#[serial]
	async fn test_update_balance() {
		let (db_pool_conn, config) = database_conn().await.unwrap();
		let account_state = AccountState::new(&db_pool_conn).await.unwrap();
		truncate_table(&account_state).await.unwrap();

		let address: Address = [3; 20];
		let mut account = create_account(address, &account_state).await;
		account.balance = 100;
		account.nonce = 10;

		account_state.update_balance(&account).await.unwrap();

		let loaded_account = account_state.get_account(&address).await.unwrap();
		assert_eq!(loaded_account.balance, account.balance);
		assert_eq!(loaded_account.nonce, 0);
	}

	#[tokio::test]
	#[serial]
	async fn test_increment_nonce() {
		let (db_pool_conn, config) = database_conn().await.unwrap();
		let account_state = AccountState::new(&db_pool_conn).await.unwrap();
		truncate_table(&account_state).await.unwrap();

		let address: Address = [4; 20];
		let mut account = create_account(address, &account_state).await;
		account.balance = 100;

		account_state.increment_nonce(&address).await.unwrap();

		let loaded_account = account_state.get_account(&address).await.unwrap();
		assert_eq!(loaded_account.balance, 0);
		assert_eq!(loaded_account.nonce, account.nonce + 1);
	}

	#[tokio::test]
	#[serial]
	async fn test_get_account() {
		let (db_pool_conn, config) = database_conn().await.unwrap();
		let account_state = AccountState::new(&db_pool_conn).await.unwrap();
		truncate_table(&account_state).await.unwrap();

		let address: Address = [5; 20];
		let mut account = create_account(address, &account_state).await;
		account.balance = 100;
		account.nonce = 5;

		account_state.update_account(&account).await.unwrap();

		let loaded_account = account_state.get_account(&address).await.unwrap();
		assert_eq!(loaded_account.balance, account.balance);
		assert_eq!(loaded_account.nonce, account.nonce);
	}

	#[tokio::test]
	#[serial]
	async fn test_get_nonce() {
		let (db_pool_conn, config) = database_conn().await.unwrap();
		let account_state = AccountState::new(&db_pool_conn).await.unwrap();
		truncate_table(&account_state).await.unwrap();

		let address: Address = [6; 20];
		let mut account = create_account(address, &account_state).await;
		account.balance = 0;
		account.nonce = 5;

		account_state.update_account(&account).await.unwrap();

		let nonce = account_state.get_nonce(&address).await.unwrap();
		assert_eq!(nonce, account.nonce);
	}

	#[tokio::test]
	#[serial]
	async fn test_is_valid_account() {
		let (db_pool_conn, config) = database_conn().await.unwrap();
		let account_state = AccountState::new(&db_pool_conn).await.unwrap();
		truncate_table(&account_state).await.unwrap();

		let address: Address = [7; 20];
		let _account = create_account(address, &account_state).await;
		let is_valid_account = account_state.is_valid_account(&address).await.unwrap();
		assert!(account_state.is_valid_account(&address).await.is_ok());
		assert!(is_valid_account);
	}

	#[tokio::test]
	#[serial]
	async fn test_new_account() {
		let (db_pool_conn, config) = database_conn().await.unwrap();
		let account_state = AccountState::new(&db_pool_conn).await.unwrap();
		truncate_table(&account_state).await.unwrap();

		let address: Address = [8; 20];
		let account_manager = AccountManager::new(&address, &account_state).await.unwrap();

		let loaded_account = account_state.get_account(&address).await.unwrap();
		assert_eq!(loaded_account.balance, account_manager.account.balance);
		assert_eq!(loaded_account.nonce, account_manager.account.nonce);
	}

	#[tokio::test]
	#[serial]
	async fn test_get_balance() {
		let (db_pool_conn, config) = database_conn().await.unwrap();
		let account_state = AccountState::new(&db_pool_conn).await.unwrap();
		truncate_table(&account_state).await.unwrap();

		let address: Address = [9; 20];
		let account_manager = AccountManager::new(&address, &account_state).await.unwrap();

		let balance = account_manager.get_balance();
		assert_eq!(balance, account_manager.account.balance);
	}

	#[tokio::test]
	#[serial]
	async fn test_get_current_nonce() {
		let (db_pool_conn, config) = database_conn().await.unwrap();
		let account_state = AccountState::new(&db_pool_conn).await.unwrap();
		truncate_table(&account_state).await.unwrap();

		let address: Address = [10; 20];
		let mut account_manager = AccountManager::new(&address, &account_state).await.unwrap();

		let initial_nonce = account_manager.account.nonce;
		let next_nonce = account_manager.get_current_nonce(&account_state).await.unwrap();

		assert_eq!(next_nonce, initial_nonce);

		let loaded_account = account_state.get_account(&address).await.unwrap();
		assert_eq!(loaded_account.nonce, next_nonce);
	}

	#[tokio::test]
	#[serial]
	async fn test_transfer_sufficient_balance() {
		let (db_pool_conn, config) = database_conn().await.unwrap();
		let account_state = AccountState::new(&db_pool_conn).await.unwrap();
		truncate_table(&account_state).await.unwrap();

		let address1: Address = [11; 20];
		let mut account_manager1 = AccountManager::new(&address1, &account_state).await.unwrap();

		let address2: Address = [12; 20];
		let account_manager2 = AccountManager::new(&address2, &account_state).await.unwrap();

		let transfer_amount = 50;
		account_manager1.account.balance = 500;
		account_state.update_balance(&account_manager1.account).await.unwrap();
		account_manager1
			.transfer(&address2, &transfer_amount, &account_state)
			.await
			.unwrap();

		let loaded_account1 = account_state.get_account(&address1).await.unwrap();
		let loaded_account2 = account_state.get_account(&address2).await.unwrap();

		assert_eq!(loaded_account1.balance, account_manager1.account.balance);
		assert_eq!(loaded_account2.balance, account_manager2.account.balance + transfer_amount);
	}

	#[tokio::test]
	#[serial]
	async fn test_transfer_insufficient_balance() {
		let (db_pool_conn, config) = database_conn().await.unwrap();
		let account_state = AccountState::new(&db_pool_conn).await.unwrap();
		truncate_table(&account_state).await.unwrap();

		let address1: Address = [13; 20];
		let mut account_manager1 = AccountManager::new(&address1, &account_state).await.unwrap();

		let address2: Address = [14; 20];
		let account_manager2 = AccountManager::new(&address2, &account_state).await.unwrap();

		let transfer_amount = 150;

		let result = account_manager1.transfer(&address2, &transfer_amount, &account_state).await;

		assert!(result.is_err());
		assert_eq!(result.unwrap_err().to_string(), "Insufficient balance");

		let loaded_account1 = account_state.get_account(&address1).await.unwrap();
		let loaded_account2 = account_state.get_account(&address2).await.unwrap();

		assert_eq!(loaded_account1.balance, account_manager1.account.balance);
		assert_eq!(loaded_account2.balance, account_manager2.account.balance);
	}

	#[tokio::test]
	#[serial]
	async fn test_has_sufficient_balance() {
		let (db_pool_conn, config) = database_conn().await.unwrap();
		let account_state = AccountState::new(&db_pool_conn).await.unwrap();
		truncate_table(&account_state).await.unwrap();

		let address: Address = [15; 20];
		let account_manager = AccountManager::new(&address, &account_state).await.unwrap();

		assert!(account_manager.has_sufficient_balance(&0));
		assert!(!account_manager.has_sufficient_balance(&50));
		assert!(!account_manager.has_sufficient_balance(&150));
	}
}
