#[cfg(test)]
mod tests {
	use crate::account_state::AccountState;
	use crate::{account_manager::*, state_pg::*};
	use anyhow::{anyhow, Error};
	use db::db::Database;
	use db::postgres::postgres::PostgresDB;
	use db::postgres::postgres_test::PostgresTestDB;
	use primitives::*;
	use serial_test::serial;
	use std::sync::Arc;
	use std::sync::Mutex;
	use system::account::{Account, AccountType};
	use system::config::Config;

	// Helper function to create a new AccountState for testing
	async fn create_test_account_state() -> Result<AccountState, Error> {
		let mut config_data = Config::default();
		let db: Database = Database::new(&config_data).await.expect("Unable to connect");
		let acc_state = AccountState::new().await?;
		Ok(acc_state)
	}

	async fn create_account(address: Address, account_state: &AccountState) -> Account {
		let account = Account::new(address);
		account_state.create_account(&account).await.unwrap();
		let loaded_account = account_state.get_account(&address).await.unwrap();
		assert_eq!(loaded_account.balance, account.balance);
		assert_eq!(loaded_account.nonce, account.nonce);
		assert_eq!(loaded_account.account_type, AccountType::User);
		account
	}
	#[tokio::test]
	#[serial]
	async fn test_create_account() {
		let account_state = match create_test_account_state().await {
			Ok(state) => state,
			Err(e) => {
				println!("{:?}", e);
				return;
			},
		};
		// Validate DB connection
		let address: Address = [1; 20];
		let _ = create_account(address, &account_state).await;
		//drop_table(&account_state).await.unwrap();
	}

	#[tokio::test]
	#[serial]
	async fn test_update_account() {
		let account_state = match create_test_account_state().await {
			Ok(state) => state,
			Err(e) => {
				println!("Connection error {:?}", e);
				return;
			},
		};

		let address: Address = [2; 20];
		let mut account = create_account(address, &account_state).await;
		account.balance = 200;
		account.nonce = 10;

		account_state.update_account(&account).await.unwrap();
		let loaded_account = account_state.get_account(&address).await.unwrap();
		assert_eq!(loaded_account.balance, account.balance);
		assert_eq!(loaded_account.nonce, account.nonce);
		assert_eq!(loaded_account.account_type, AccountType::User);
		// drop_table(&account_state).await.unwrap();
	}

	#[tokio::test]
	#[serial]
	async fn test_is_valid_account() {
		let account_state = match create_test_account_state().await {
			Ok(state) => state,
			Err(e) => return println!("{:?}", e),
		};
		let address: Address = [7; 20];
		let _account = create_account(address, &account_state).await;
		let is_valid_account = account_state.is_valid_account(&address).await.unwrap();
		assert!(account_state.is_valid_account(&address).await.is_ok());
		assert!(is_valid_account);
		// drop_table(&account_state).await.unwrap();
	}

	#[tokio::test]
	#[serial]
	async fn test_new_account() {
		let account_state = match create_test_account_state().await {
			Ok(state) => state,
			Err(e) => return println!("{:?}", e),
		};

		let address: Address = [8; 20];
		let account_manager = AccountManager::new(&address, &account_state).await.unwrap();

		let loaded_account = account_state.get_account(&address).await.unwrap();
		assert_eq!(loaded_account.balance, account_manager.account.balance);
		assert_eq!(loaded_account.nonce, account_manager.account.nonce);
		// drop_table(&account_state).await.unwrap();
	}

	#[tokio::test]
	#[serial]
	async fn test_get_balance() {
		let account_state = match create_test_account_state().await {
			Ok(state) => state,
			Err(e) => return println!("{:?}", e),
		};
		let address: Address = [9; 20];
		let account_manager = AccountManager::new(&address, &account_state).await.unwrap();

		let balance = account_manager.get_balance();
		assert_eq!(balance, account_manager.account.balance);
		// drop_table(&account_state).await.unwrap();
	}

	#[tokio::test]
	#[serial]
	async fn test_get_current_nonce() {
		let account_state = match create_test_account_state().await {
			Ok(state) => state,
			Err(e) => return println!("{:?}", e),
		};
		let address: Address = [10; 20];
		let mut account_manager = AccountManager::new(&address, &account_state).await.unwrap();

		let initial_nonce = account_manager.account.nonce;
		let next_nonce = account_manager.get_current_nonce(&account_state).await.unwrap();

		assert_eq!(next_nonce, initial_nonce);

		let loaded_account = account_state.get_account(&address).await.unwrap();
		assert_eq!(loaded_account.nonce, next_nonce);
		// drop_table(&account_state).await.unwrap();
	}

	#[tokio::test]
	#[serial]
	async fn test_transfer_sufficient_balance() {
		let account_state = match create_test_account_state().await {
			Ok(state) => state,
			Err(e) => return println!("{:?}", e),
		};
		let address1: Address = [11; 20];
		let mut account_manager1 = AccountManager::new(&address1, &account_state).await.unwrap();

		let address2: Address = [12; 20];
		let account_manager2 = AccountManager::new(&address2, &account_state).await.unwrap();

		let transfer_amount = 50;
		account_manager1.account.balance = 500;
		account_state.update_account(&account_manager1.account).await.unwrap();
		account_manager1
			.transfer(&address2, &transfer_amount, &account_state)
			.await
			.unwrap();

		let loaded_account1 = account_state.get_account(&address1).await.unwrap();
		let loaded_account2 = account_state.get_account(&address2).await.unwrap();

		assert_eq!(loaded_account1.balance, account_manager1.account.balance);
		assert_eq!(loaded_account2.balance, account_manager2.account.balance + transfer_amount);
		// drop_table(&account_state).await.unwrap();
	}

	#[tokio::test]
	#[serial]
	async fn test_transfer_insufficient_balance() {
		let account_state = match create_test_account_state().await {
			Ok(state) => state,
			Err(e) => return println!("{:?}", e),
		};
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
		// drop_table(&account_state).await.unwrap();
	}

	#[tokio::test]
	#[serial]
	async fn test_has_sufficient_balance() {
		let account_state = match create_test_account_state().await {
			Ok(state) => state,
			Err(e) => return println!("{:?}", e),
		};
		let address: Address = [15; 20];
		let account_manager = AccountManager::new(&address, &account_state).await.unwrap();

		assert!(account_manager.has_sufficient_balance(&0));
		assert!(!account_manager.has_sufficient_balance(&50));
		assert!(!account_manager.has_sufficient_balance(&150));
		// drop_table(&account_state).await.unwrap();
	}
}