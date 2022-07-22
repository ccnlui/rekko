pub mod pb {
    tonic::include_proto!("rekko");
}

use futures_core::Stream;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{transport::Server, Request, Response, Status, Streaming};
use std::error::Error;
use std::net::ToSocketAddrs;
use std::pin::Pin;
use std::time::Duration;

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
        _req: Request<EchoRequest>,
    ) -> EchoResult<EchoResponse> {
        Err(Status::unimplemented("not implemented"))
    }

    type ServerStreamingEchoStream = ResponseStream;
    async fn server_streaming_echo(
        &self,
        req: Request<EchoRequest>,
    ) -> EchoResult<Self::ServerStreamingEchoStream> {

        println!("Server streaming echo");
        println!("client connected from: {:?}", req.remote_addr());

        let repeat = std::iter::repeat(EchoResponse{
            message: req.into_inner().message,
        });
        let mut stream = Box::pin(tokio_stream::iter(repeat).throttle(Duration::from_millis(100)));

        let (tx, rx) = mpsc::channel(128);
        tokio::spawn(async move {
            while let Some(item) = stream.next().await {
                match tx.send(Result::<_, Status>::Ok(item)).await {
                    Ok(_) => {
                        // item (server response) was queued to be send to client
                    }
                    Err(_item) => {
                        // output_stream was build from rx and both are dropped
                        break;
                    }
                }
            }
            println!("client disconnected");
        });

        let output_stream = ReceiverStream::new(rx);
        Ok(Response::new(
            Box::pin(output_stream) as Self::ServerStreamingEchoStream
        ))
    }

    async fn client_streaming_echo(
        &self,
        _req: Request<Streaming<EchoRequest>>,
    ) -> EchoResult<EchoResponse> {
        Err(Status::unimplemented("not implemented"))
    }

    type BidirectionalStreamingEchoStream = ResponseStream;
    async fn bidirectional_streaming_echo(
        &self,
        _req: Request<Streaming<EchoRequest>>,
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