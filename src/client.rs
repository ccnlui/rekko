pub mod pb {
    tonic::include_proto!("rekko");
}

use std::error::Error;
use std::time::SystemTime;
use tonic::transport::Channel;
use tokio::sync::{mpsc, broadcast};
use tokio::task::JoinHandle;
use tokio::signal;
use tokio_stream::wrappers::UnboundedReceiverStream;
use hdrhistogram::Histogram;

use pb::rekko_client::RekkoClient;
use pb::EchoRequest;

async fn server_streaming_echo(
    mut sig_shutdown: broadcast::Receiver<()>,
    client: &mut RekkoClient<Channel>,
) -> (mpsc::UnboundedSender<EchoRequest>, JoinHandle<Histogram<u64>>) {

    let (tx, rx) = mpsc::unbounded_channel();
    let mut inbound = client
        .bidirectional_streaming_echo(UnboundedReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner();

    let handle = tokio::spawn(async move {

        const NANOS_PER_HOUR: u64 = 60 * 60 * 1_000_000_000;
        let mut histogram: Histogram<u64> = Histogram::new_with_max(NANOS_PER_HOUR, 3).unwrap();

        loop {
            tokio::select! {
                Ok(res) = inbound.message() => {
                    let resp = res.unwrap();
                    let latency = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as u64 - resp.timestamp;
                    histogram.record(latency).unwrap();
                    // println!("received: {} - {} bytes {} latency", resp.timestamp, resp.payload.len(), latency);
                }
                _ = sig_shutdown.recv() => {
                    println!("shutting down client...");
                    break;
                }
            }
        }
        histogram
    });

    (tx, handle)
}

fn send(tx: &mut mpsc::UnboundedSender<EchoRequest>, number_of_messages: u32) {
    let timestamp_ns = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as u64;

    for _ in 0..number_of_messages {
        tx.send(EchoRequest{ timestamp: timestamp_ns, payload: (0..100).collect() }).unwrap();
    };
}

fn output_percentile_distribution(
    histogram: Histogram<u64>,
    quantile_precision: usize,
    ticks_per_half: u32,
) {
    println!(
        "{:>12} {:>quantile_precision$} {:>quantile_precision$} {:>10} {:>14}",
        "Value",
        "QuantileValue",
        "QuantileIteration",
        "TotalCount",
        "1/(1-Quantile)",
        quantile_precision = quantile_precision + 2 // + 2 from leading "0." for numbers
    );
    let mut sum = 0;
    for v in histogram.iter_quantiles(ticks_per_half) {
        sum += v.count_since_last_iteration();
        if v.quantile_iterated_to() < 1.0 {
            println!(
                "{:12} {:1.*} {:1.*} {:10} {:14.2}",
                v.value_iterated_to(),
                quantile_precision,
                v.quantile(),
                quantile_precision,
                v.quantile_iterated_to(),
                sum,
                1_f64 / (1_f64 - v.quantile_iterated_to()),
            );
        } else {
            println!(
                "{:12} {:1.*} {:1.*} {:10} {:>14}",
                v.value_iterated_to(),
                quantile_precision,
                v.quantile(),
                quantile_precision,
                v.quantile_iterated_to(),
                sum,
                "∞"
            )
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {

    let (notify_shutdown, _) = broadcast::channel(1);
    let mut client = RekkoClient::connect("http://127.0.0.1:9090").await.unwrap();
    let (mut tx, handle) = server_streaming_echo(notify_shutdown.subscribe(), &mut client).await;

    tokio::spawn(async move {
        signal::ctrl_c().await.unwrap();
        notify_shutdown.send(()).unwrap();
        println!("ctrl-c received!");
    });


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

    let histogram = handle.await.unwrap();
    output_percentile_distribution(histogram, 3, 5);

    Ok(())
}
