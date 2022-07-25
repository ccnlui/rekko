pub mod pb {
    tonic::include_proto!("rekko");
}

use std::error::Error;
use std::time::SystemTime;
use futures_util::StreamExt;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tonic::transport::Channel;
use tokio_stream::wrappers::UnboundedReceiverStream;

use pb::rekko_client::RekkoClient;
use pb::EchoRequest;

async fn server_streaming_echo(client: &mut RekkoClient<Channel>) -> (mpsc::UnboundedSender<EchoRequest>, JoinHandle<()>) {

    let (tx, rx) = mpsc::unbounded_channel();
    let mut inbound = client
        .bidirectional_streaming_echo(UnboundedReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner();

    let handle = tokio::spawn(async move {
        while let Some(res) = inbound.next().await {
            let resp = res.unwrap();
            let latency = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as u64 - resp.timestamp;
            println!("received: {} - {} bytes {} latency", resp.timestamp, resp.payload.len(), latency);
        }
    });

    (tx, handle)
}

fn send(tx: &mut mpsc::UnboundedSender<EchoRequest>, number_of_messages: u32) {
    let timestamp_ns = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as u64;

    for _ in 0..number_of_messages {
        tx.send(EchoRequest{ timestamp: timestamp_ns, payload: (0..100).collect() }).unwrap();
    };
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut client = RekkoClient::connect("http://127.0.0.1:9090").await.unwrap();
    let (mut tx, handle) = server_streaming_echo(&mut client).await;

    let warmup_iterations = 1;
    let warmup_message_rate = 10;
    let message_length = 100;
    let batch_size = 1;

    println!("Running warmup for {} iterations of {} messages each, with {} bytes payload and a burst size of {}...",
        warmup_iterations,
        warmup_message_rate,
        message_length,
        batch_size);

    send(&mut tx, warmup_message_rate);
    handle.await.unwrap();

    Ok(())
}