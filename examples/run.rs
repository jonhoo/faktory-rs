use async_trait::async_trait;
use faktory::{Client, Job, JobRunner, Worker};
use std::io::{Error as IOError, Result as IOResult};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time;

pub struct JobHandler<T> {
    chan: Arc<mpsc::Sender<T>>,
}

impl<T> JobHandler<T> {
    pub fn new(chan: Arc<mpsc::Sender<T>>) -> Self {
        Self { chan }
    }
}

impl JobHandler<u64> {
    async fn process_one(&self, job: Job) -> IOResult<()> {
        time::sleep(time::Duration::from_millis(100)).await;
        let args = job.args();
        let x = args[0].as_u64().expect("'x' to be an integer");
        let y = args[1].as_u64().expect("'y' to be an interger");
        self.chan.send(x + y).await.expect("no error");
        Ok(())
    }
}

#[async_trait]
impl JobRunner for JobHandler<u64> {
    type Error = IOError;

    async fn run(&self, job: Job) -> Result<(), Self::Error> {
        self.process_one(job).await.unwrap();
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::fmt()
        .with_max_level(tracing::Level::TRACE)
        .init();

    // create a sender and a receiver; the former will be passed
    // to the job handler and will be used to send the processing result
    // down to the receiving end;
    let (tx, mut rx) = mpsc::channel(100);
    let tx = Arc::new(tx);

    // create a producing client
    let mut c = Client::connect(None)
        .await
        .expect("client successfully connected");

    // push one job onto the default queue
    c.enqueue(Job::builder("job_type").args(vec![5, 8]).build())
        .await
        .expect("enqueued ok");

    // create a worker
    let mut w = Worker::builder()
        .register("job_type", JobHandler::new(Arc::clone(&tx)))
        .connect(None)
        .await
        .expect("Connected to server");

    let _handle = tokio::spawn(async move { w.run(&["default"]).await });

    // wait for processing results
    let res = rx.recv().await.expect("some calculation result");

    println!(
        "Send a job with args `vec![5, 8]`. Received result `{}`",
        res
    );
}
