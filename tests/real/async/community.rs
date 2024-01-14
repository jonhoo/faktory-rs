extern crate faktory;

use faktory::{AsyncProducer, JobBuilder};

use crate::skip_check;

#[tokio::test]
async fn async_hello_p() {
    skip_check!();
    let p = AsyncProducer::connect(None).await.unwrap();
    drop(p);
}

#[tokio::test]
async fn async_enqueue_job() {
    skip_check!();
    let mut p = AsyncProducer::connect(None).await.unwrap();
    p.enqueue(JobBuilder::new("order").build()).await.unwrap();
}
