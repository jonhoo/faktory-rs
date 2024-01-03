extern crate faktory;
extern crate serde_json;
extern crate url;

use faktory::*;
use serde_json::Value;
use std::io;
use std::sync;

macro_rules! skip_check {
    () => {
        if std::env::var_os("FAKTORY_URL").is_none() {
            return;
        }
    };
}

#[test]
fn hello_p() {
    skip_check!();
    let p = Producer::connect(None).unwrap();
    drop(p);
}

#[test]
fn hello_c() {
    skip_check!();
    let mut c = ConsumerBuilder::default();
    c.hostname("tester".to_string())
        .wid("hello".to_string())
        .labels(vec!["foo".to_string(), "bar".to_string()]);
    c.register("never_called", |_| -> io::Result<()> { unreachable!() });
    let c = c.connect(None).unwrap();
    drop(c);
}

#[test]
fn roundtrip() {
    skip_check!();
    let local = "roundtrip";

    let (tx, rx) = sync::mpsc::channel();
    let tx = sync::Arc::new(sync::Mutex::new(tx));
    let mut c = ConsumerBuilder::default();
    c.hostname("tester".to_string()).wid(local.to_string());
    {
        let tx = sync::Arc::clone(&tx);
        c.register(local, move |j| -> io::Result<()> {
            tx.lock().unwrap().send(j).unwrap();
            Ok(())
        });
    }
    let mut c = c.connect(None).unwrap();

    let mut p = Producer::connect(None).unwrap();
    p.enqueue(Job::new(local, vec!["z"]).on_queue(local))
        .unwrap();
    c.run_one(0, &[local]).unwrap();
    let job = rx.recv().unwrap();
    assert_eq!(job.queue, local);
    assert_eq!(job.kind(), local);
    assert_eq!(job.args(), &[Value::from("z")]);
}

#[test]
fn multi() {
    skip_check!();
    let local = "multi";

    let (tx, rx) = sync::mpsc::channel();
    let tx = sync::Arc::new(sync::Mutex::new(tx));
    let mut c = ConsumerBuilder::default();
    c.hostname("tester".to_string()).wid(local.to_string());
    {
        let tx = sync::Arc::clone(&tx);
        c.register(local, move |j| -> io::Result<()> {
            tx.lock().unwrap().send(j).unwrap();
            Ok(())
        });
    }
    let mut c = c.connect(None).unwrap();

    let mut p = Producer::connect(None).unwrap();
    p.enqueue(Job::new(local, vec![Value::from(1), Value::from("foo")]).on_queue(local))
        .unwrap();
    p.enqueue(Job::new(local, vec![Value::from(2), Value::from("bar")]).on_queue(local))
        .unwrap();

    c.run_one(0, &[local]).unwrap();
    let job = rx.recv().unwrap();
    assert_eq!(job.queue, local);
    assert_eq!(job.kind(), local);
    assert_eq!(job.args(), &[Value::from(1), Value::from("foo")]);

    c.run_one(0, &[local]).unwrap();
    let job = rx.recv().unwrap();
    assert_eq!(job.queue, local);
    assert_eq!(job.kind(), local);
    assert_eq!(job.args(), &[Value::from(2), Value::from("bar")]);
}

#[test]
fn fail() {
    skip_check!();
    let local = "fail";

    let (tx, rx) = sync::mpsc::channel();
    let tx = sync::Arc::new(sync::Mutex::new(tx));
    let mut c = ConsumerBuilder::default();
    c.hostname("tester".to_string()).wid(local.to_string());
    {
        let tx = sync::Arc::clone(&tx);
        c.register(local, move |j| -> io::Result<()> {
            tx.lock().unwrap().send(j).unwrap();
            Err(io::Error::new(io::ErrorKind::Other, "nope"))
        });
    }
    let mut c = c.connect(None).unwrap();

    let mut p = Producer::connect(None).unwrap();

    // note that *enqueueing* the jobs didn't fail!
    p.enqueue(Job::new(local, vec![Value::from(1), Value::from("foo")]).on_queue(local))
        .unwrap();
    p.enqueue(Job::new(local, vec![Value::from(2), Value::from("bar")]).on_queue(local))
        .unwrap();

    c.run_one(0, &[local]).unwrap();
    c.run_one(0, &[local]).unwrap();
    drop(c);
    assert_eq!(rx.into_iter().take(2).count(), 2);

    // TODO: check that jobs *actually* failed!
}

#[test]
fn queue() {
    skip_check!();
    let local = "pause";

    let (tx, rx) = sync::mpsc::channel();
    let tx = sync::Arc::new(sync::Mutex::new(tx));

    let mut c = ConsumerBuilder::default();
    c.hostname("tester".to_string()).wid(local.to_string());
    c.register(local, move |_job| tx.lock().unwrap().send(true));
    let mut c = c.connect(None).unwrap();

    let mut p = Producer::connect(None).unwrap();
    p.enqueue(Job::new(local, vec![Value::from(1)]).on_queue(local))
        .unwrap();
    p.queue_pause(&[local]).unwrap();

    let had_job = c.run_one(0, &[local]).unwrap();
    assert!(!had_job);
    let worker_executed = rx.try_recv().is_ok();
    assert!(!worker_executed);

    p.queue_resume(&[local]).unwrap();

    let had_job = c.run_one(0, &[local]).unwrap();
    assert!(had_job);
    let worker_executed = rx.try_recv().is_ok();
    assert!(worker_executed);
}

#[test]
fn test_jobs_created_with_builder() {
    skip_check!();

    // prepare a producer ("client" in Faktory terms) and consumer ("worker"):
    let mut producer = Producer::connect(None).unwrap();
    let mut consumer = ConsumerBuilder::default();
    consumer.register("rebuild_index", move |job| -> io::Result<_> {
        assert!(job.args().is_empty());
        Ok(eprintln!("{:?}", job))
    });
    consumer.register("register_order", move |job| -> io::Result<_> {
        assert!(job.args().len() != 0);
        Ok(eprintln!("{:?}", job))
    });

    let mut consumer = consumer.connect(None).unwrap();

    // prepare some jobs with JobBuilder:
    let job1 = JobBuilder::new("rebuild_index")
        .queue("test_jobs_created_with_builder_0")
        .build();

    let job2 = Job::builder("register_order")
        .args(vec!["ISBN-13:9781718501850"])
        .queue("test_jobs_created_with_builder_1")
        .build();

    let mut job3 = Job::new("register_order", vec!["ISBN-13:9781718501850"]);
    job3.queue = "test_jobs_created_with_builder_1".to_string();

    // enqueue ...
    producer.enqueue(job1).unwrap();
    producer.enqueue(job2).unwrap();
    producer.enqueue(job3).unwrap();

    // ... and execute:
    let had_job = consumer
        .run_one(0, &["test_jobs_created_with_builder_0"])
        .unwrap();
    assert!(had_job);

    let had_job = consumer
        .run_one(0, &["test_jobs_created_with_builder_1"])
        .unwrap();
    assert!(had_job);

    let had_job = consumer
        .run_one(0, &["test_jobs_created_with_builder_1"])
        .unwrap();
    assert!(had_job);
}
