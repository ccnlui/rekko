pub mod pb {
    tonic::include_proto!("rekko");
}

use futures_core::Stream;
use std::error::Error;
use std::time::{Duration, SystemTime};
use futures_util::StreamExt;
use tonic::transport::Channel;

use pb::rekko_client::RekkoClient;
use pb::EchoRequest;

async fn server_streaming_echo(client: &mut RekkoClient<Channel>, num: usize) {
    let inbound = client
        .server_streaming_echo(EchoRequest{
            timestamp: SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as u64,
            payload: vec![],
        })
        .await
        .unwrap()
        .into_inner();

    let mut inbound = inbound.take(num);
    while let Some(res) = inbound.next().await {
        let resp = res.unwrap();
        println!("received: {} - {} bytes", resp.timestamp, resp.payload.len());
    }
    // stream is droped here and the disconnect info is send to server
}

fn echo_requests_iter() -> impl Stream<Item = EchoRequest> {
    tokio_stream::iter(1..usize::MAX).map(|_| EchoRequest{
        timestamp: SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as u64,
        payload: (0..100).collect(),
    })
}

async fn bidirectional_streaming_echo(client: &mut RekkoClient<Channel>, num: usize) {
    let outbound = echo_requests_iter().take(num);

    let mut inbound = client
        .bidirectional_streaming_echo(outbound)
        .await
        .unwrap()
        .into_inner();

    while let Some(res) = inbound.next().await {
        let resp = res.unwrap();
        let latency = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as u64 - resp.timestamp;
        println!("received: {} - {} bytes {} latency", resp.timestamp, resp.payload.len(), latency);
    }
    
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut client = RekkoClient::connect("http://127.0.0.1:9090").await.unwrap();

    // println!("Server streaming echo:");
    // server_streaming_echo(&mut client, 10).await;
    // tokio::time::sleep(Duration::from_secs(1)).await; // so we don't miss println

    println!("Bidirectional streaming echo:");
    bidirectional_streaming_echo(&mut client, 42).await;
    tokio::time::sleep(Duration::from_secs(1)).await;

    Ok(())
}