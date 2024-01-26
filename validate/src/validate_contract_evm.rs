use crate::validate_common::ContractValidator;
use anyhow::Error;
use async_trait::async_trait;
use db::db::DbTxConn;
use primitives::*;
use system::transaction::Transaction;

pub struct ValidateContract;

#[async_trait]
impl<'a> ContractValidator<'a> for ValidateContract {
	async fn validate_contract_deployment(
		&self,
		_transaction: &Transaction,
		_contract_code: &ContractCode,
		_sender: &Address,
		_db_conn: &'a DbTxConn<'a>,
	) -> Result<(), Error> {
		// FIXME: Temporarily bypassing validation - needs implementation!
		Ok(())
	}

	async fn validate_contract_init(
		&self,
		_transaction: &Transaction,
		_contract_address: &Address,
		_arguments: ContractArgument,
		_sender: &Address,
		_db_conn: &'a DbTxConn<'a>,
	) -> Result<(), Error> {
		// FIXME: Temporarily bypassing validation - needs implementation!
		Ok(())
	}

	async fn validate_contract_function_call(
		&self,
		_transaction: &Transaction,
		_contract_instance_address: &Address,
		_function_name: &ContractFunction,
		_arguments: &ContractArgument,
		_sender: &Address,
		_db_conn: &'a DbTxConn<'a>,
	) -> Result<(), Error> {
		// FIXME: Temporarily bypassing validation - needs implementation!
		Ok(())
	}
}
