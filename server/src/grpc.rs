use crate::service::FullNodeService;
use cipher_rpc::rpc_model::{
	node_server::{Node, NodeServer},
	GetAccountStateRequest, GetAccountStateResponse, GetBlockByNumberRequest,
	GetBlockByNumberResponse, GetChainStateRequest, GetChainStateResponse, GetCurrentNonceRequest,
	GetCurrentNonceResponse, GetEventsRequest, GetEventsResponse, GetStakeRequest,
	GetStakeResponse, GetTransactionReceiptRequest, GetTransactionReceiptResponse,
	GetTransactionsByAccountRequest, GetTransactionsByAccountResponse,
	SmartContractReadOnlyCallRequest, SmartContractReadOnlyCallResponse, SubmitTransactionRequest,
	SubmitTransactionResponse,
};
use hyper::http;
use log::info;
use tokio::sync::mpsc;

use system::mempool::ResponseMempool;
use tonic::{codegen::http::Method, transport::Server, Request, Response, Status};
use tonic_web::GrpcWebLayer;
use tower_http::cors::{AllowHeaders, AllowOrigin, CorsLayer};

pub struct FullNodeGrpc {
	service: FullNodeService,
	mempool_res_rx: mpsc::Receiver<ResponseMempool>,
}

/// gRPC wrapper for the FullNodeService
#[tonic::async_trait]
impl Node for FullNodeGrpc {
	type GetEventsStream =
		tokio_stream::wrappers::ReceiverStream<Result<GetEventsResponse, Status>>;
	type SubmitTransactionStream =
		tokio_stream::wrappers::ReceiverStream<Result<SubmitTransactionResponse, Status>>;
	async fn get_account_state(
		&self,
		request: Request<GetAccountStateRequest>,
	) -> Result<Response<GetAccountStateResponse>, Status> {
		Ok(Response::new(self.service.get_account_state(request.into_inner()).await?))
	}

	async fn submit_transaction(
		&self,
		request: Request<SubmitTransactionRequest>,
	) -> Result<Response<Self::SubmitTransactionStream>, Status> {
		Ok(Response::new(self.service.submit_transaction(request.into_inner()).await?))
	}

	async fn get_transaction_receipt(
		&self,
		request: Request<GetTransactionReceiptRequest>,
	) -> Result<Response<GetTransactionReceiptResponse>, Status> {
		Ok(Response::new(self.service.get_transaction_receipt(request.into_inner()).await?))
	}

	async fn get_transactions_by_account(
		&self,
		request: Request<GetTransactionsByAccountRequest>,
	) -> Result<Response<GetTransactionsByAccountResponse>, Status> {
		Ok(Response::new(self.service.get_transactions_by_account(request.into_inner()).await?))
	}

	async fn smart_contract_read_only_call(
		&self,
		request: Request<SmartContractReadOnlyCallRequest>,
	) -> Result<Response<SmartContractReadOnlyCallResponse>, Status> {
		Ok(Response::new(self.service.smart_contract_read_only_call(request.into_inner()).await?))
	}

	async fn get_chain_state(
		&self,
		request: Request<GetChainStateRequest>,
	) -> Result<Response<GetChainStateResponse>, Status> {
		Ok(Response::new(self.service.get_chain_state(request.into_inner()).await?))
	}

	async fn get_block_by_number(
		&self,
		request: Request<GetBlockByNumberRequest>,
	) -> Result<Response<GetBlockByNumberResponse>, Status> {
		Ok(Response::new(self.service.get_block_by_number(request.into_inner()).await?))
	}
	/*
	async fn get_latest_block_headers(
		&self,
		request: Request<GetLatestBlockHeadersRequest>,
	) -> Result<Response<GetLatestBlockHeadersResponse>, Status> { Ok(Response::new(
	  self.service.get_latest_block_headers(request.into_inner()).await?, ))
	}

	async fn get_latest_transactions(
		&self,
		request: Request<GetLatestTransactionsRequest>,
	) -> Result<Response<GetLatestTransactionsResponse>, Status> { //
	  self.service.get_latest_transactions(request.into_inner()).await?;

	}
	*/
	async fn get_stake(
		&self,
		request: Request<GetStakeRequest>,
	) -> Result<Response<GetStakeResponse>, Status> {
		Ok(Response::new(self.service.get_stake(request.into_inner()).await?))
	}

	async fn get_current_nonce(
		&self,
		request: Request<GetCurrentNonceRequest>,
	) -> Result<Response<GetCurrentNonceResponse>, Status> {
		Ok(Response::new(self.service.get_current_nonce(request.into_inner()).await?))
	}

	async fn get_events(
		&self,
		request: Request<GetEventsRequest>,
	) -> Result<Response<Self::GetEventsStream>, Status> {
		Ok(Response::new(self.service.get_events(request.into_inner()).await?))
	}
}

pub async fn run_server(
	address: String,
	service: FullNodeService,
	mempool_res_rx: mpsc::Receiver<ResponseMempool>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
	let addr = address.parse().unwrap();
	let node_service = FullNodeGrpc { service, mempool_res_rx };
	let node_server = NodeServer::new(node_service);
	info!("GRPC server listening on {}", addr);

	/*let cors = CorsLayer::new()
	.allow_origin(AllowOrigin::any())
	.allow_methods(vec![Method::GET, http::Method::POST, http::Method::PUT, http::Method::HEAD])
	.allow_headers(AllowHeaders::any());*/

	let grpc_web_layer = GrpcWebLayer::new();

	Server::builder()
		/*.accept_http1(true)
		.layer(cors)*/
		.layer(grpc_web_layer)
		.add_service(node_server)
		.serve(addr)
		.await?;

	Ok(())
}
