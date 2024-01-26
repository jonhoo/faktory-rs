//! This modle contains a port of the test suite from
//! the `tests/real/community.rs` module.
//!
//! Main diff:
//! - tests are marked this `async_` prefix;
//! - tokio multi-threaded rt used;
//! - AsyncConsumerBuilder used instead of ConsumerBuilder;
//! - AsyncProducer used instead of Producer;
//! - await used where needed;

extern crate faktory;

use std::{io, sync};

use faktory::{AsyncConsumerBuilder, AsyncProducer, Job, JobBuilder};
use serde_json::Value;

use crate::skip_check;

#[tokio::test(flavor = "multi_thread")]
async fn async_hello_p() {
    skip_check!();
    let p = AsyncProducer::connect(None).await.unwrap();
    drop(p);
}

#[tokio::test(flavor = "multi_thread")]
async fn async_enqueue_job() {
    skip_check!();
    let mut p = AsyncProducer::connect(None).await.unwrap();
    p.enqueue(JobBuilder::new("order").build()).await.unwrap();
}

async fn process_order(j: Job) -> Result<(), std::io::Error> {
    println!("{:?}", j);
    assert_eq!(j.kind(), "order");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn async_roundtrip() {
    skip_check!();
    let jid = String::from("x-job-id-0123456782");
    let mut c = AsyncConsumerBuilder::default();
    c.register("order", |job| Box::pin(process_order(job)));
    c.register("image", |job| {
        Box::pin(async move {
            println!("{:?}", job);
            assert_eq!(job.kind(), "image");
            Ok(())
        })
    });
    let mut c = c.connect(None).await.unwrap();
    let mut p = AsyncProducer::connect(None).await.unwrap();
    p.enqueue(
        JobBuilder::new("order")
            .jid(&jid)
            .args(vec!["ISBN-13:9781718501850"])
            .queue("roundtrip_async")
            .build(),
    )
    .await
    .unwrap();
    let had_one = c.run_one(0, &["roundtrip_async"]).await.unwrap();
    assert!(had_one);

    let drained = !c.run_one(0, &["roundtrip_async"]).await.unwrap();
    assert!(drained);
}

#[tokio::test(flavor = "multi_thread")]
async fn async_multi() {
    skip_check!();
    let local = "multi_async";

    let (tx, rx) = sync::mpsc::channel();
    let tx = sync::Arc::new(sync::Mutex::new(tx));
    let mut c = AsyncConsumerBuilder::default();
    c.hostname("tester".to_string()).wid(local.to_string());

    c.register(local, move |j| {
        let tx = sync::Arc::clone(&tx);
        Box::pin(async move {
            tx.lock().unwrap().send(j).unwrap();
            Ok::<(), io::Error>(())
        })
    });

    let mut c = c.connect(None).await.unwrap();

    let mut p = AsyncProducer::connect(None).await.unwrap();
    p.enqueue(Job::new(local, vec![Value::from(1), Value::from("foo")]).on_queue(local))
        .await
        .unwrap();
    p.enqueue(Job::new(local, vec![Value::from(2), Value::from("bar")]).on_queue(local))
        .await
        .unwrap();

    c.run_one(0, &[local]).await.unwrap();
    let job = rx.recv().unwrap();
    assert_eq!(job.queue, local);
    assert_eq!(job.kind(), local);
    assert_eq!(job.args(), &[Value::from(1), Value::from("foo")]);

    c.run_one(0, &[local]).await.unwrap();
    let job = rx.recv().unwrap();
    assert_eq!(job.queue, local);
    assert_eq!(job.kind(), local);
    assert_eq!(job.args(), &[Value::from(2), Value::from("bar")]);
}

#[tokio::test(flavor = "multi_thread")]
async fn async_fail() {
    skip_check!();
    let local = "fail_async";

    let (tx, rx) = sync::mpsc::channel();
    let tx = sync::Arc::new(sync::Mutex::new(tx));
    let mut c = AsyncConsumerBuilder::default();
    c.hostname("tester".to_string()).wid(local.to_string());

    c.register(local, move |j| {
        let tx = sync::Arc::clone(&tx);
        Box::pin(async move {
            tx.lock().unwrap().send(j).unwrap();
            Err(io::Error::new(io::ErrorKind::Other, "nope"))
        })
    });

    let mut c = c.connect(None).await.unwrap();

    let mut p = AsyncProducer::connect(None).await.unwrap();

    // note that *enqueueing* the jobs didn't fail!
    p.enqueue(Job::new(local, vec![Value::from(1), Value::from("foo")]).on_queue(local))
        .await
        .unwrap();
    p.enqueue(Job::new(local, vec![Value::from(2), Value::from("bar")]).on_queue(local))
        .await
        .unwrap();

    c.run_one(0, &[local]).await.unwrap();
    c.run_one(0, &[local]).await.unwrap();
    drop(c);
    assert_eq!(rx.into_iter().take(2).count(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn async_queue() {
    skip_check!();
    let local = "pause";

    let (tx, rx) = sync::mpsc::channel();
    let tx = sync::Arc::new(sync::Mutex::new(tx));

    let mut c = AsyncConsumerBuilder::default();
    c.hostname("tester".to_string()).wid(local.to_string());
    c.register(local, move |_job| {
        let tx = sync::Arc::clone(&tx);
        Box::pin(async move { tx.lock().unwrap().send(true) })
    });
    let mut c = c.connect(None).await.unwrap();

    let mut p = AsyncProducer::connect(None).await.unwrap();
    p.enqueue(Job::new(local, vec![Value::from(1)]).on_queue(local))
        .await
        .unwrap();
    p.queue_pause(&[local]).await.unwrap();

    let had_job = c.run_one(0, &[local]).await.unwrap();
    assert!(!had_job);
    let worker_executed = rx.try_recv().is_ok();
    assert!(!worker_executed);

    p.queue_resume(&[local]).await.unwrap();

    let had_job = c.run_one(0, &[local]).await.unwrap();
    assert!(had_job);
    let worker_executed = rx.try_recv().is_ok();
    assert!(worker_executed);
}

async fn assert_args_empty(j: Job) -> io::Result<()> {
    assert!(j.args().is_empty());
    Ok(eprintln!("{:?}", j))
}

async fn assert_args_not_empty(j: Job) -> io::Result<()> {
    assert!(j.args().len() != 0);
    Ok(eprintln!("{:?}", j))
}

#[tokio::test(flavor = "multi_thread")]
async fn async_test_jobs_created_with_builder() {
    skip_check!();

    // prepare a producer ("client" in Faktory terms) and consumer ("worker"):
    let mut producer = AsyncProducer::connect(None).await.unwrap();
    let mut consumer = AsyncConsumerBuilder::default();
    consumer.register("rebuild_index", |j| Box::pin(assert_args_empty(j)));
    consumer.register("register_order", |j| Box::pin(assert_args_not_empty(j)));

    let mut consumer = consumer.connect(None).await.unwrap();

    // prepare some jobs with JobBuilder:
    let job1 = JobBuilder::new("rebuild_index")
        .queue("async_test_jobs_created_with_builder_0")
        .build();

    let job2 = Job::builder("register_order")
        .args(vec!["ISBN-13:9781718501850"])
        .queue("async_test_jobs_created_with_builder_1")
        .build();

    let mut job3 = Job::new("register_order", vec!["ISBN-13:9781718501850"]);
    job3.queue = "async_test_jobs_created_with_builder_1".to_string();

    // enqueue ...
    producer.enqueue(job1).await.unwrap();
    producer.enqueue(job2).await.unwrap();
    producer.enqueue(job3).await.unwrap();

    // ... and execute:
    let had_job = consumer
        .run_one(0, &["async_test_jobs_created_with_builder_0"])
        .await
        .unwrap();
    assert!(had_job);

    let had_job = consumer
        .run_one(0, &["async_test_jobs_created_with_builder_1"])
        .await
        .unwrap();
    assert!(had_job);

    let had_job = consumer
        .run_one(0, &["async_test_jobs_created_with_builder_1"])
        .await
        .unwrap();
    assert!(had_job);
}
