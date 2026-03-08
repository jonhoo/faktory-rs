use faktory::worker::Worker;
use std::io::Error as IOError;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::fmt()
        .with_max_level(tracing::Level::TRACE)
        .init();

    // this will terminate under one of the following conditions:
    // - signal from the Faktory server;
    // - ctrl+c signal;
    // - worker panic;
    Worker::builder()
        .register_fn("job_type", |j| async move {
            println!("{j:?}");
            Ok::<(), IOError>(())
        })
        .connect()
        .await
        .expect("Connected to server")
        .run_to_completion(&["default"])
        .await
}
