pub mod pb {
    tonic::include_proto!("rekko");
}

use futures_core::Stream;
use tokio::sync::mpsc;
use tokio::time;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{transport::Server, Request, Response, Status, Streaming};
use std::error::Error;
use std::io::ErrorKind;
use std::pin::Pin;
use std::time::Duration;
use std::time::SystemTime;

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

        let mut interval = time::interval(Duration::from_millis(100));
        let (tx, rx) = mpsc::channel(128);

        tokio::spawn(async move {
            loop {
                interval.tick().await;
                let msg = EchoResponse{
                    timestamp: SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as u64,
                    payload: (0..100).collect(),
                };
                match tx.send(Result::<_, Status>::Ok(msg)).await {
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

        let outbound = ReceiverStream::new(rx);
        Ok(Response::new(
            Box::pin(outbound) as Self::ServerStreamingEchoStream
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
        req: Request<Streaming<EchoRequest>>,
    ) -> EchoResult<Self::BidirectionalStreamingEchoStream> {

        println!("Bidirectional streaming echo");
        println!("client connected from: {:?}", req.remote_addr());

        let mut inbound = req.into_inner();
        let (tx, rx) = mpsc::unbounded_channel();

        // this spawn here is required if you want to handle connection error.
        // If we just map `inbound` and write it back as `out_stream` the `out_stream`
        // will be drooped when connection error occurs and error will never be propagated
        // to mapped version of `inbound`.
        tokio::spawn(async move {
            while let Some(r) = inbound.next().await {
                match r {
                    Ok(req) => {
                        tx.send(Ok(EchoResponse{ 
                            timestamp: req.timestamp,
                            payload: req.payload,
                        }))
                        .expect("working rx");
                        // println!("got: {}", req.timestamp);
                    }
                    Err(status) => {
                        if let Some(io_err) = match_for_io_error(&status) {
                            if io_err.kind() == ErrorKind::BrokenPipe {
                                eprintln!("client disconnected: broken pipe");
                                break;
                            }
                        }

                        match tx.send(Err(status)) {
                            Ok(_) => (),
                            Err(_err) => break, // response was dropped
                        }
                    }
                }
            }
            println!("stream ended");
        });

        let outbound = UnboundedReceiverStream::new(rx);

        Ok(Response::new(
            Box::pin(outbound) as Self::BidirectionalStreamingEchoStream
        ))
    }
}

fn match_for_io_error(err_status: &Status) -> Option<&std::io::Error> {
    let mut err: &(dyn Error + 'static) = err_status;

    loop {
        if let Some(io_err) = err.downcast_ref::<std::io::Error>() {
            return Some(io_err);
        }

        // h2::Error do not expose std::io::Error with `source()`
        // https://github.com/hyperium/h2/pull/462
        if let Some(h2_err) = err.downcast_ref::<h2::Error>() {
            if let Some(io_err) = h2_err.get_io() {
                return Some(io_err);
            }
        }

        err = match err.source() {
            Some(err) => err,
            None => return None,
        };
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let server = EchoServer{};
    Server::builder()
        .add_service(RekkoServer::new(server))
        .serve("127.0.0.1:9090".parse().unwrap())
        .await
        .unwrap();

    Ok(())
}