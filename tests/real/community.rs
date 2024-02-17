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
fn test_jobs_pushed_in_bulk() {
    skip_check!();

    let local_1 = "test_jobs_pushed_in_bulk_1";
    let local_2 = "test_jobs_pushed_in_bulk_2";
    let local_3 = "test_jobs_pushed_in_bulk_3";
    let local_4 = "test_jobs_pushed_in_bulk_4";

    let mut p = Producer::connect(None).unwrap();
    let (enqueued_count, errors) = p
        .enqueue_many([
            Job::builder("common").queue(local_1).build(),
            Job::builder("common").queue(local_2).build(),
            Job::builder("special").queue(local_2).build(),
        ])
        .unwrap();
    assert_eq!(enqueued_count, 3);
    assert!(errors.is_none()); // error-free

    // From the Faktory source code, we know that:
    // 1) job ID should be at least 8 chars long string; NB! Should be taken in account when introducing `Jid` new type;
    // 2) jobtype should be a non-empty string;
    // 3) job cannot be reserved for more than 86400 days;
    // ref: https://github.com/contribsys/faktory/blob/main/manager/manager.go#L192-L203
    // Let's break these rules:

    let (enqueued_count, errors) = p
        .enqueue_many(vec![
            Job::builder("broken").jid("short").queue(local_3).build(), // jid.len() < 8
            Job::builder("") // empty string jobtype
                .jid("3sZCbdp8e9WX__0")
                .queue(local_3)
                .build(),
            Job::builder("broken")
                .jid("3sZCbdp8e9WX__1")
                .queue(local_3)
                .reserve_for(864001) // reserve_for exceeded
                .build(),
            // plus some valid ones:
            Job::builder("very_special").queue(local_4).build(),
            Job::builder("very_special").queue(local_4).build(),
        ])
        .unwrap();

    // 3 out of 5 not enqueued;
    let errors = errors.unwrap();
    assert_eq!(errors.len(), 3);
    assert_eq!(
        errors.get("short").unwrap(),
        "jobs must have a reasonable jid parameter"
    );
    assert_eq!(
        errors.get("3sZCbdp8e9WX__0").unwrap(),
        "jobs must have a jobtype parameter"
    );
    assert_eq!(
        errors.get("3sZCbdp8e9WX__1").unwrap(),
        "jobs cannot be reserved for more than one day"
    );

    assert_eq!(enqueued_count, 2);
    // Let's check that the two well-formatted jobs
    // have _really_ been enqueued, i.e. that `enqueue_many`
    // is not an  all-or-nothing operation:
    let mut c = ConsumerBuilder::default();
    c.hostname("tester".to_string()).wid(local_3.to_string());
    c.register("very_special", move |_job| -> io::Result<()> { Ok(()) });
    c.register("broken", move |_job| -> io::Result<()> { Ok(()) });
    let mut c = c.connect(None).unwrap();

    // we targeted "very_special" jobs to "local_4" queue
    assert!(c.run_one(0, &[local_4]).unwrap());
    assert!(c.run_one(0, &[local_4]).unwrap());
    assert!(!c.run_one(0, &[local_4]).unwrap()); // drained

    // also let's check that the 'broken' jobs have NOT been enqueued,
    // reminder: we target the broken jobs to "local_3" queue
    assert!(!c.run_one(0, &[local_3]).unwrap()); // empty
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
