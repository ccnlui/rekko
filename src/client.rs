pub mod pb {
    tonic::include_proto!("rekko");
}

use std::error::Error;
use std::time::Duration;
use futures_util::StreamExt;
use tonic::transport::Channel;

use pb::rekko_client::RekkoClient;
use pb::EchoRequest;

async fn server_streaming_echo(client: &mut RekkoClient<Channel>, num: usize) {
    let stream = client
        .server_streaming_echo(EchoRequest{
            message: "hello".to_string(),
        })
        .await
        .unwrap()
        .into_inner();

    let mut stream = stream.take(num);
    while let Some(item) = stream.next().await {
        println!("\treceived: {}", item.unwrap().message);
    }
    // stream is droped here and the disconnect info is send to server
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut client = RekkoClient::connect("http://127.0.0.1:9090").await.unwrap();

    println!("Server streaming echo:");
    server_streaming_echo(&mut client, 10).await;
    tokio::time::sleep(Duration::from_secs(1)).await; // so we don't miss println

    Ok(())
}