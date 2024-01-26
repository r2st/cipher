use crate::rpc_model::{GetAccountStateRequest, GetAccountStateResponse};
use anyhow::{anyhow, Context, Result};
use log::debug;
use reqwest::RequestBuilder;
use secp256k1::SecretKey;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Value};

/// JSON RPC specific helpers

#[derive(Debug, Serialize, Deserialize)]
pub struct EVMSmartContractDeployment {
	pub smart_contract_deployment: Vec<serde_json::Value>,
}

pub async fn post_json_rpc(
	client: RequestBuilder,
	method: &str,
	params: Value,
) -> Result<JsonRpcResponse> {
	let request = JsonRpcRequest { jsonrpc: "2.0", method, params, id: 1 };

	debug!("JSON RPC REQUEST PARAMS: {}", request.params);

	let response = client.json(&request).send().await?.json::<JsonRpcResponse>().await?;

	Ok(response)
}

pub async fn get_nonce(client: RequestBuilder, secret_key: &SecretKey) -> Result<u128> {
	let address = crate::get_address_from_private_key(secret_key)?;

	let response = post_json_rpc(
		client,
		"cipher_getAccountState",
		json!({"request": GetAccountStateRequest { address } }),
	)
	.await?;

	parse_response::<GetAccountStateResponse>(response).and_then(|x| {
		x.account_state.ok_or(anyhow!("no account state")).and_then(|x| {
			let nonce: Result<u128, _> = x.nonce.parse().context("failed to parse nonce");
			nonce
		})
	})
}

pub fn parse_response<T: DeserializeOwned>(x: JsonRpcResponse) -> Result<T> {
	x.result
		.and_then(|x| serde_json::from_value::<T>(x).ok())
		.ok_or(anyhow!("failed to deserialize response"))
}

#[derive(Debug, Serialize)]
pub struct JsonRpcRequest<'a> {
	pub jsonrpc: &'a str,
	pub method: &'a str,
	pub params: serde_json::Value,
	pub id: u64,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct JsonRpcResponse {
	pub jsonrpc: String,
	pub result: Option<serde_json::Value>,
	pub error: Option<JsonRpcError>,
	pub id: u64,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct JsonRpcError {
	pub code: i32,
	pub message: String,
	pub data: Option<serde_json::Value>,
}
