pub mod pb {
    tonic::include_proto!("rekko");
}

use futures_core::Stream;
use std::error::Error;
use std::net::ToSocketAddrs;
use std::pin::Pin;
use tonic::{transport::Server, Request, Response, Status, Streaming};

use pb::EchoRequest;
use pb::EchoResponse;
use pb::rekko_server::Rekko;
use pb::rekko_server::RekkoServer;

type EchoResult<T> = Result<Response<T>, Status>;
type ResponseStream = Pin<Box<dyn Stream<Item = Result<EchoResponse, Status>> + Send>>;

#[derive(Debug)]
pub struct EchoServer {}

#[tonic::async_trait]
impl Rekko for EchoServer {

    async fn unary_echo(
        &self,
        _request: Request<EchoRequest>,
    ) -> EchoResult<EchoResponse> {
        Err(Status::unimplemented("not implemented"))
    }

    type ServerStreamingEchoStream = ResponseStream;
    async fn server_streaming_echo(
        &self,
        _request: Request<EchoRequest>,
    ) -> EchoResult<Self::ServerStreamingEchoStream> {
        Err(Status::unimplemented("not implemented"))
    }

    async fn client_streaming_echo(
        &self,
        _request: Request<Streaming<EchoRequest>>,
    ) -> EchoResult<EchoResponse> {
        Err(Status::unimplemented("not implemented"))
    }

    type BidirectionalStreamingEchoStream = ResponseStream;
    async fn bidirectional_streaming_echo(
        &self,
        _request: Request<Streaming<EchoRequest>>,
    ) -> EchoResult<Self::BidirectionalStreamingEchoStream> {
        Err(Status::unimplemented("not implemented"))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let server = EchoServer{};
    Server::builder()
        .add_service(RekkoServer::new(server))
        .serve("127.0.0.1:9090".to_socket_addrs().unwrap().next().unwrap())
        .await
        .unwrap();

    Ok(())
}