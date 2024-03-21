use faktory::{main as tokio_main, WorkerBuilder};
use std::io::Error as IOError;

#[tokio_main]
async fn main() {
    tracing_subscriber::fmt::fmt()
        .with_max_level(tracing::Level::TRACE)
        .init();

    // create a worker
    let mut w = WorkerBuilder::default();
    // register a handler
    w.register("job_type", |j| async move {
        println!("{:?}", j);
        Ok::<(), IOError>(())
    });

    // connect to Faktrory server
    let w = w.connect(None).await.expect("Connected to server");

    // this will terminate under one of the following conditions:
    // - signal from the Faktory server;
    // - ctrl+c signal;
    // - worker panic;
    w.run_to_completion(&["default"]).await
}
