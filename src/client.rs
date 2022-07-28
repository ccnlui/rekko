pub mod pb {
    tonic::include_proto!("ekko");
}

use std::cmp::min;
use std::error::Error;
use std::fmt::Display;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{Ordering, AtomicU64};
use std::time::{SystemTime, Duration};
use tonic::transport::Channel;
use tokio::sync::{mpsc, broadcast};
use tokio::task::{JoinHandle, yield_now};
use tokio::signal;
use tokio_stream::wrappers::UnboundedReceiverStream;
use hdrhistogram::Histogram;

use rekko::constants::NANOS_PER_SECOND;
use rekko::constants::NANOS_PER_HOUR;
use pb::ekko_client::EkkoClient;
use pb::EchoRequest;

async fn server_streaming_echo(
    mut sig_shutdown: broadcast::Receiver<()>,
    client: &mut EkkoClient<Channel>,
    histogram: Arc<Mutex<Histogram<u64>>>,
    count: Arc<AtomicU64>,
) -> (mpsc::UnboundedSender<EchoRequest>, JoinHandle<()>) {

    let (tx, rx) = mpsc::unbounded_channel();
    let mut inbound = client
        .bidirectional_streaming_echo(UnboundedReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner();

    let handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                result = inbound.message() => {
                    match result {
                        Ok(opt) => {
                            let resp = opt.unwrap();
                            let latency = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as u64 - resp.timestamp;
                            histogram.lock().unwrap().record(latency).unwrap();
                            count.fetch_add(1, Ordering::SeqCst);
                            // println!("received: {} - {} bytes {} latency", resp.timestamp, resp.payload.len(), latency);
                        }
                        Err(status) => {
                            println!("disconnected error: {:?}", status);
                            break;
                        }
                    }
                }
                _ = sig_shutdown.recv() => {
                    println!("shutting down client...");
                    break;
                }
            }
        }
    });

    (tx, handle)
}

async fn send_and_receive(
    tx: &mut mpsc::UnboundedSender<EchoRequest>,
    msg: Vec<u8>,
    number_of_messages: u64,
    iterations: u64,
    received_messages: Arc<AtomicU64>,
) -> u64 {

    let total_number_of_messages = iterations * number_of_messages;
    let mut sent_messages: u64 = 0;

    let start_time = SystemTime::now();
    let end_time = start_time + Duration::from_secs(iterations);
    let send_interval = Duration::from_nanos(NANOS_PER_SECOND * 1 / number_of_messages);
    let mut timestamp = start_time;
    let mut now = start_time;
    let mut next_report_time = start_time + Duration::from_secs(1);

    // TODO: make this configurable.
    let mut batch_size: u64 = 1;
    loop {
        let sent = send(tx, batch_size, msg.clone(), timestamp.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as u64);

        sent_messages += sent;
        if total_number_of_messages == sent_messages {
            report_progress(start_time, now, sent_messages);
            break;
        }

        now = SystemTime::now();
        if sent == batch_size {
            // next batch
            batch_size = min(total_number_of_messages - sent_messages, 1);
            timestamp += send_interval;

            // spin until next batch
            let mut received = 0;
            while now < timestamp && now < end_time {
                if received < sent_messages {
                    yield_now().await;
                    received = received_messages.load(Ordering::SeqCst);
                }
                now = SystemTime::now();
            }
        } else {
            // next batch
            batch_size -= sent;
            yield_now().await;
        }

        if now >= end_time {
            break;
        }

        if now >= next_report_time {
            let elapsed_seconds = report_progress(start_time, now, sent_messages);
            next_report_time = start_time + Duration::from_secs(elapsed_seconds + 1);
        }
    }

    let deadline = SystemTime::now() + Duration::from_secs(30);
    while received_messages.load(Ordering::SeqCst) < sent_messages {
        if SystemTime::now() >= deadline {
            println!("*** WARNING: Not all messages were received after 30s deadline!");
            break;
        }
        yield_now().await;
    }

    sent_messages
}

fn send(
    tx: &mut mpsc::UnboundedSender<EchoRequest>,
    number_of_messages: u64,
    msg: Vec<u8>,
    timestamp: u64,
) -> u64 {
    for _ in 0..number_of_messages {
        tx.send(EchoRequest{
            timestamp,
            payload: msg.clone(),
        }).unwrap();
    };
    number_of_messages
}

fn report_progress(start_time: SystemTime, now: SystemTime, sent_messages: u64) -> u64 {
    let elapsed_seconds = now.duration_since(start_time).unwrap().as_secs();
    let send_rate = match elapsed_seconds == 0 {
        true => sent_messages,
        false => sent_messages / elapsed_seconds,
    };
    println!("Send rate {} msg/sec", send_rate);
    elapsed_seconds
}

fn output_percentile_distribution(
    histogram: &Histogram<u64>,
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

    fn write_extra_data<T1: Display, T2: Display>(
        label1: &str,
        data1: T1,
        label2: &str,
        data2: T2,
    ) {
        println!(
            "#[{:10} = {:12.2}, {:14} = {:12.2}]",
            label1, data1, label2, data2
        );
    }
    write_extra_data(
        "Mean",
        histogram.mean(),
        "StdDeviation",
        histogram.stdev(),
    );
    write_extra_data(
        "Max",
        histogram.max(),
        "Total count",
        histogram.len()
    );
    write_extra_data(
        "Buckets",
        histogram.buckets(),
        "SubBuckets",
        histogram.distinct_values(),
    );
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {

    let (notify_shutdown, _) = broadcast::channel(1);
    let mut client = EkkoClient::connect("http://127.0.0.1:9090").await.unwrap();    
    let histogram: Histogram<u64> = Histogram::new_with_max(NANOS_PER_HOUR, 3).unwrap();
    let histogram = Arc::new(Mutex::new(histogram));
    let count = Arc::new(AtomicU64::new(0));

    let (mut tx, _) = server_streaming_echo(
        notify_shutdown.subscribe(),
        &mut client,
        Arc::clone(&histogram),
        Arc::clone(&count),
    ).await;

    tokio::spawn(async move {
        signal::ctrl_c().await.unwrap();
        notify_shutdown.send(()).unwrap();
        println!("ctrl-c received!");
    });

    
    let warmup_iterations = 10;
    let warmup_message_rate = 20_000;
    let message_length = 128;
    let batch_size = 1;

    let msg: Vec<u8> = (0..message_length).collect();

    println!("Running warmup for {} iterations of {} messages each, with {} bytes payload and a burst size of {}...",
        warmup_iterations,
        warmup_message_rate,
        message_length,
        batch_size);
    send_and_receive(&mut tx, msg.clone(), warmup_message_rate, warmup_iterations, Arc::clone(&count)).await;
    count.store(0, Ordering::SeqCst);
    histogram.lock().unwrap().reset();

    let iterations = 10;
    let message_rate = 500_000;

    println!("Running measurement for {} iterations of {} messages each, with {} bytes payload and a burst size of {}...",
        iterations,
        message_rate,
        message_length,
        batch_size);
    let sent_messages = send_and_receive(&mut tx, msg.clone(), message_rate, iterations, Arc::clone(&count)).await;
    println!("sent messages: {}", sent_messages);

    output_percentile_distribution(histogram.lock().as_deref().unwrap(), 12, 5);

    Ok(())
}
