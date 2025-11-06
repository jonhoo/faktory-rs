use async_trait::async_trait;
use tokio::time;
use faktory::bb8::{PooledClient, ClientConnectionManager};
use faktory::{Job, JobRunner, Worker};
use std::io::{Error as IOError, Result as IOResult};

pub struct JobHandler;

impl JobHandler {
    async fn process_one(&self, job: Job) -> IOResult<()> {
        time::sleep(time::Duration::from_millis(100)).await;
        let args = job.args();
        let x = args[0].as_u64().expect("'x' to be an integer");
        let y = args[1].as_u64().expect("'y' to be an interger");
        println!("Processed job: {} + {} = {}", x, y, x + y);
        Ok(())
    }
}

#[async_trait]
impl JobRunner for JobHandler {
    type Error = IOError;

    async fn run(&self, job: Job) -> Result<(), Self::Error> {
        self.process_one(job).await.unwrap();
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let manager = ClientConnectionManager::from_env().expect("from_env succeeded");

    let pool = PooledClient::builder()
        .max_size(10)
        .build(manager)
        .await
        .expect("build succeeded");

    for i in 0..10 {
        let pool = pool.clone();
        tokio::spawn(async move {
            let mut client = pool.get().await.expect("get client from pool");

            client
                .enqueue(Job::builder("job_type")
                    .args(vec![5, 8]).build())
                .await
                .expect("enqueued ok");
        });
    }

    let mut w = Worker::builder()
        .register("job_type", JobHandler)
        .connect()
        .await
        .expect("Connected to server");

    tokio::spawn(async move {
        w.run(&["default"]).await.expect("Worker run succeeded");
    });
    
    // wait some time to let jobs be processed
    time::sleep(time::Duration::from_secs(5)).await;
}