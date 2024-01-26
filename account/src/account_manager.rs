use crate::account_state::AccountState;
use anyhow::{anyhow, Error};
use db::utils::big_int::{FromBigInt, ToBigInt};
use primitives::{Address, Balance, Nonce};
use system::account::Account;

pub struct AccountManager {
	pub account: Account,
}

impl<'a> AccountManager {
	pub async fn new(
		address: &Address,
		account_state: &AccountState<'a>,
	) -> Result<AccountManager, Error> {
		let account = Account::new(*address);
		account_state.create_account(&account).await?;
		let account_manager = AccountManager { account };
		Ok(account_manager)
	}
	pub async fn new_system(
		address: &Address,
		account_state: &AccountState<'a>,
	) -> Result<AccountManager, Error> {
		let account = Account::new_system(*address);
		account_state.create_account(&account).await?;
		let account_manager = AccountManager { account };
		Ok(account_manager)
	}
	pub fn get_balance(&self) -> Balance {
		self.account.balance
	}
	pub async fn get_current_nonce(
		&mut self,
		_account_state: &AccountState<'a>,
	) -> Result<Nonce, Error> {
		Ok(self.account.nonce)
	}
	pub async fn transfer(
		&mut self,
		to: &Address,
		amount: &Balance,
		account_state: &AccountState<'a>,
	) -> Result<(), Error> {
		if !self.has_sufficient_balance(amount) {
			return Err(anyhow!("Insufficient balance"))
		}

		let account_balance = self
			.account
			.balance
			.get_big_int()
			.checked_sub(&amount.get_big_int())
			.ok_or(anyhow!("Error Subtracting balance"))?;

		self.account.balance = u128::from_big_int(&account_balance);

		account_state.update_balance(&self.account).await?;

		let mut to_account = if !account_state.is_valid_account(&to).await? {
			Account::new(to.clone())
		} else {
			account_state.get_account(&to).await?
		};

		let to_account_balance = to_account
			.balance
			.get_big_int()
			.checked_add(&amount.get_big_int())
			.ok_or(anyhow!("Error Adding balance"))?;
		to_account.balance = u128::from_big_int(&to_account_balance);
		account_state.update_balance(&to_account).await?;

		Ok(())
	}
	pub fn has_sufficient_balance(&self, amount: &Balance) -> bool {
		self.account.balance >= *amount
	}
	pub async fn increment_nonce(
		address: &Address,
		account_state: &AccountState<'a>,
	) -> Result<(), Error> {
		account_state.increment_nonce(address).await
	}
}
