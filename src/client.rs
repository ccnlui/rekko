pub mod pb {
    tonic::include_proto!("rekko");
}

use futures_core::Stream;
use std::error::Error;
use std::time::Duration;
use futures_util::StreamExt;
use tonic::transport::Channel;

use pb::rekko_client::RekkoClient;
use pb::EchoRequest;

async fn server_streaming_echo(client: &mut RekkoClient<Channel>, num: usize) {
    let inbound = client
        .server_streaming_echo(EchoRequest{
            message: "hello".to_string(),
        })
        .await
        .unwrap()
        .into_inner();

    let mut out_stream = inbound.take(num);
    while let Some(item) = out_stream.next().await {
        println!("received: {}", item.unwrap().message);
    }
    // stream is droped here and the disconnect info is send to server
}

fn echo_requests_iter() -> impl Stream<Item = EchoRequest> {
    tokio_stream::iter(1..usize::MAX).map(|i| EchoRequest{
        message: format!("msg {:02}", i),
    })
}

async fn bidirectional_streaming_echo(client: &mut RekkoClient<Channel>, num: usize) {
    let outbound = echo_requests_iter().take(num);

    let mut inbound = client
        .bidirectional_streaming_echo(outbound)
        .await
        .unwrap()
        .into_inner();

    while let Some(item) = inbound.next().await {
        println!("received: {}", item.unwrap().message);
    }
    
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut client = RekkoClient::connect("http://127.0.0.1:9090").await.unwrap();

    println!("Server streaming echo:");
    server_streaming_echo(&mut client, 10).await;
    tokio::time::sleep(Duration::from_secs(1)).await; // so we don't miss println

    println!("Bidirectional streaming echo:");
    bidirectional_streaming_echo(&mut client, 42).await;
    tokio::time::sleep(Duration::from_secs(1)).await;

    Ok(())
}