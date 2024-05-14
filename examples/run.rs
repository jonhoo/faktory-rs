use faktory::Worker;
use std::io::Error as IOError;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::fmt()
        .with_max_level(tracing::Level::TRACE)
        .init();

    // create a worker
    let mut w = Worker::builder()
        .graceful_shutdown_period(2_000)
        .register_fn("job_type", |j| async move {
            println!("{:?}", j);
            Ok::<(), IOError>(())
        })
        .connect(None)
        .await
        .expect("Connected to server");

    // create a cancellation token
    let token = CancellationToken::new();

    // create a child token and pass it to the spawned task (one could - alternatively - just
    // clone the "parent" token)
    let child_token = token.child_token();

    let handle = tokio::spawn(async move { w.run(&["default"], Some(child_token)).await });

    // cancel the task
    token.cancel();

    let nrunning = handle.await.expect("joined ok").expect("no worker errors");

    // nrunng will be 0, since our workers are idle in this example:
    // we are not pushing jobs to the Faktory server
    tracing::info!(
        "Number of workers that were running when the signal was sent: {}",
        nrunning
    );
}
