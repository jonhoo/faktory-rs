use faktory::{channel, Message, WorkerBuilder};
use std::io::Error as IOError;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::fmt()
        .with_max_level(tracing::Level::TRACE)
        .init();

    // create a worker
    let mut w = WorkerBuilder::default();

    // customize graceful shutdown time-out
    w.graceful_shutdown_period(2_000);

    // register a handler
    w.register("job_type", |j| async move {
        println!("{:?}", j);
        Ok::<(), IOError>(())
    });

    // connect to the Faktrory server
    let mut w = w.connect(None).await.expect("Connected to server");

    // create a channel to send a signal to the worker
    let (tx, rx) = channel();

    let handle = tokio::spawn(async move { w.run(&["default"], Some(rx)).await });

    tx.send(Message::Exit(100)).await.expect("sent ok");

    let nrunning = handle.await.expect("joined ok").expect("no worker errors");

    // nrunng will be 0, since our workers are idle in this example:
    // we are not pushing jobs to the Faktory server
    tracing::info!(
        "Number of workers that were running when the signal was sent: {}",
        nrunning
    );
}
