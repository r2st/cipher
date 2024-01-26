use crate::validate_common::ValidateCommon;
use anyhow::{anyhow, Error};
use db::{db::DbTxConn, utils::big_int::ToBigInt};
use primitives::*;
use system::transaction::Transaction;

pub struct ValidateToken {}

impl<'a> ValidateToken {
	pub async fn validate_native_token(
		transaction: &Transaction,
		amount: Balance,
		sender: &Address,
		db_pool_conn: &'a DbTxConn<'a>,
	) -> Result<(), Error> {
		let account =
			ValidateCommon::validate_common_native_token_tx(transaction, sender, db_pool_conn)
				.await?;

		// Check if the account has sufficient balance for the transaction amount and fee

		let amount = amount.get_big_int();
		let fee_limit = transaction.fee_limit.get_big_int();
		let total_cost = amount.checked_add(&fee_limit).ok_or(anyhow!("Overflow"))?;

		if account.balance.get_big_int().lt(&total_cost) {
			return Err(anyhow!("Insufficient balance to cover the transaction"))
		}

		Ok(())
	}
}
