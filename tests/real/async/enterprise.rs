//! This modle contains a port of the test suite from
//! the `tests/real/enterprise.rs` module.
//!
//! Main diff:
//! - tests are marked this `async_` prefix;
//! - tokio multi-threaded rt used;
//! - AsyncConsumerBuilder used instead of ConsumerBuilder;
//! - AsyncProducer used instead of Producer;
//! - await used where needed;

extern crate faktory;

use std::io;

use faktory::{AsyncConsumerBuilder, AsyncProducer, Job, JobBuilder};
use tokio::time;

use crate::skip_if_not_enterprise;
use crate::utils::learn_faktory_url;

async fn print_job(j: Job) -> io::Result<()> {
    Ok(eprintln!("{:?}", j))
}

#[tokio::test(flavor = "multi_thread")]
async fn async_ent_expiring_job() {
    skip_if_not_enterprise!();

    let url = learn_faktory_url();
    let local = "async_ent_expiring_job";

    // prepare a producer ("client" in Faktory terms) and consumer ("worker"):
    let mut producer = AsyncProducer::connect(Some(&url)).await.unwrap();
    let mut consumer = AsyncConsumerBuilder::default();
    consumer.register("AnExpiringJob", |j| Box::pin(print_job(j)));
    let mut consumer = consumer.connect(Some(&url)).await.unwrap();

    // prepare an expiring job:
    let job_ttl_secs: u64 = 3;

    let ttl = chrono::Duration::seconds(job_ttl_secs as i64);
    let job1 = JobBuilder::new("AnExpiringJob")
        .args(vec!["ISBN-13:9781718501850"])
        .queue(local)
        .expires_at(chrono::Utc::now() + ttl)
        .build();

    // enqueue and fetch immediately job1:
    producer.enqueue(job1).await.unwrap();
    let had_job = consumer.run_one(0, &[local]).await.unwrap();
    assert!(had_job);

    // check that the queue is drained:
    let had_job = consumer.run_one(0, &[local]).await.unwrap();
    assert!(!had_job);

    // prepare another one:
    let job2 = JobBuilder::new("AnExpiringJob")
        .args(vec!["ISBN-13:9781718501850"])
        .queue(local)
        .expires_at(chrono::Utc::now() + ttl)
        .build();

    // enqueue and then fetch job2, but after ttl:
    producer.enqueue(job2).await.unwrap();
    tokio::time::sleep(time::Duration::from_secs(job_ttl_secs * 2)).await;
    let had_job = consumer.run_one(0, &[local]).await.unwrap();

    // For the non-enterprise edition of Faktory, this assertion will
    // fail, which should be taken into account when running the test suite on CI.
    assert!(!had_job);
}
