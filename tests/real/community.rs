use crate::skip_check;
use faktory::{Client, Job, JobBuilder, WorkerBuilder};
use serde_json::Value;
use std::{future::Future, io, sync};

#[tokio::test(flavor = "multi_thread")]
async fn hello_client() {
    skip_check!();
    let p = Client::connect(None).await.unwrap();
    drop(p);
}

#[tokio::test(flavor = "multi_thread")]
async fn hello_worker() {
    skip_check!();
    let mut c = WorkerBuilder::<io::Error>::default();
    c.hostname("tester".to_string())
        .labels(vec!["foo".to_string(), "bar".to_string()]);
    c.register("never_called", |_| async move { unreachable!() });
    let c = c.connect(None).await.unwrap();
    drop(c);
}

#[tokio::test(flavor = "multi_thread")]
async fn enqueue_job() {
    skip_check!();
    let mut p = Client::connect(None).await.unwrap();
    p.enqueue(JobBuilder::new("order").build()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn roundtrip() {
    skip_check!();

    let local = "roundtrip";
    let jid = String::from("x-job-id-0123456782");

    let mut c = WorkerBuilder::default();
    c.register("order", move |job| async move {
        assert_eq!(job.kind(), "order");
        assert_eq!(job.queue, local);
        assert_eq!(job.args(), &[Value::from("ISBN-13:9781718501850")]);
        Ok::<(), io::Error>(())
    });
    c.register("image", |_| async move { unreachable!() });
    let mut c = c.connect(None).await.unwrap();
    let mut p = Client::connect(None).await.unwrap();
    p.enqueue(
        JobBuilder::new("order")
            .jid(&jid)
            .args(vec!["ISBN-13:9781718501850"])
            .queue(local)
            .build(),
    )
    .await
    .unwrap();
    let had_one = c.run_one(0, &[local]).await.unwrap();
    assert!(had_one);

    let drained = !c.run_one(0, &[local]).await.unwrap();
    assert!(drained);
}

#[tokio::test(flavor = "multi_thread")]
async fn multi() {
    skip_check!();
    let local = "multi_async";

    let (tx, rx) = sync::mpsc::channel();
    let tx = sync::Arc::new(sync::Mutex::new(tx));
    let mut c = WorkerBuilder::default();
    c.hostname("tester".to_string()).wid(local.into());

    c.register(local, move |j| {
        let tx = sync::Arc::clone(&tx);
        Box::pin(async move {
            tx.lock().unwrap().send(j).unwrap();
            Ok::<(), io::Error>(())
        })
    });

    let mut c = c.connect(None).await.unwrap();

    let mut p = Client::connect(None).await.unwrap();
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
async fn fail() {
    skip_check!();
    let local = "fail";

    let (tx, rx) = sync::mpsc::channel();
    let tx = sync::Arc::new(sync::Mutex::new(tx));
    let mut c = WorkerBuilder::default();
    c.hostname("tester".to_string()).wid(local.into());

    c.register(local, move |j| {
        let tx = sync::Arc::clone(&tx);
        Box::pin(async move {
            tx.lock().unwrap().send(j).unwrap();
            Err(io::Error::new(io::ErrorKind::Other, "nope"))
        })
    });

    let mut c = c.connect(None).await.unwrap();

    let mut p = Client::connect(None).await.unwrap();

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
async fn queue() {
    skip_check!();
    let local = "pause";

    let (tx, rx) = sync::mpsc::channel();
    let tx = sync::Arc::new(sync::Mutex::new(tx));

    let mut c = WorkerBuilder::default();
    c.hostname("tester".to_string()).wid(local.into());
    c.register(local, move |_job| {
        let tx = sync::Arc::clone(&tx);
        Box::pin(async move { tx.lock().unwrap().send(true) })
    });
    let mut c = c.connect(None).await.unwrap();

    let mut p = Client::connect(None).await.unwrap();
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

#[tokio::test(flavor = "multi_thread")]
async fn test_jobs_pushed_in_bulk() {
    skip_check!();

    let local_1 = "test_jobs_pushed_in_bulk_1";
    let local_2 = "test_jobs_pushed_in_bulk_2";
    let local_3 = "test_jobs_pushed_in_bulk_3";
    let local_4 = "test_jobs_pushed_in_bulk_4";

    let mut p = Client::connect(None).await.unwrap();
    let (enqueued_count, errors) = p
        .enqueue_many(vec![
            Job::builder("common").queue(local_1).build(),
            Job::builder("common").queue(local_2).build(),
            Job::builder("special").queue(local_2).build(),
        ])
        .await
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
        .enqueue_many([
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
        .await
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
    let mut c = WorkerBuilder::default();
    c.hostname("tester".to_string()).wid(local_3.into());
    c.register("very_special", move |_job| async {
        Ok::<(), io::Error>(())
    });
    c.register("broken", move |_job| async { Ok::<(), io::Error>(()) });
    let mut c = c.connect(None).await.unwrap();

    // we targeted "very_special" jobs to "local_4" queue
    assert!(c.run_one(0, &[local_4]).await.unwrap());
    assert!(c.run_one(0, &[local_4]).await.unwrap());
    assert!(!c.run_one(0, &[local_4]).await.unwrap()); // drained

    // also let's check that the 'broken' jobs have NOT been enqueued,
    // reminder: we target the broken jobs to "local_3" queue
    assert!(!c.run_one(0, &[local_3]).await.unwrap()); // empty
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
async fn test_jobs_created_with_builder() {
    skip_check!();

    // prepare a client and a worker:
    let mut cl = Client::connect(None).await.unwrap();
    let mut w = WorkerBuilder::default();
    w.register("rebuild_index", assert_args_empty);
    w.register("register_order", assert_args_not_empty);

    let mut w = w.connect(None).await.unwrap();

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
    cl.enqueue(job1).await.unwrap();
    cl.enqueue(job2).await.unwrap();
    cl.enqueue(job3).await.unwrap();

    // ... and execute:
    let had_job = w
        .run_one(0, &["test_jobs_created_with_builder_0"])
        .await
        .unwrap();
    assert!(had_job);

    let had_job = w
        .run_one(0, &["test_jobs_created_with_builder_1"])
        .await
        .unwrap();
    assert!(had_job);

    let had_job = w
        .run_one(0, &["test_jobs_created_with_builder_1"])
        .await
        .unwrap();
    assert!(had_job);
}

use std::pin::Pin;
use tokio::sync::mpsc;
fn process_hard_task(
    sender: sync::Arc<mpsc::Sender<bool>>,
) -> Box<
    dyn Fn(Job) -> Pin<Box<dyn Future<Output = Result<(), io::Error>> + Send>>
        + Send
        + Sync
        + 'static,
> {
    return Box::new(move |j: Job| {
        let sender = sync::Arc::clone(&sender);
        Box::pin(async move {
            let complexity = j.args().iter().next().unwrap().as_u64().unwrap();
            sender.send(true).await.unwrap(); // inform that we are now starting to process the job
            tokio::time::sleep(tokio::time::Duration::from_millis(complexity)).await;
            Ok::<(), io::Error>(())
        })
    });
}

#[tokio::test(flavor = "multi_thread")]
async fn test_shutdown_signals_handling() {
    use faktory::{channel, Message};
    skip_check!();

    let qname = "test_shutdown_signals_handling";
    let jkind = "heavy";
    let shutdown_timeout = 500;

    // get a client and a job to enqueue
    let mut cl = Client::connect(None).await.unwrap();
    let j = JobBuilder::new(jkind)
        .queue(qname)
        // task will be being processed for at least 1 second
        .args(vec![1000])
        .build();

    let (tx, mut rx_for_test_purposes) = tokio::sync::mpsc::channel::<bool>(1);
    let tx = sync::Arc::new(tx);

    // get a connected worker
    let mut w = WorkerBuilder::default();
    w.graceful_shutdown_period(shutdown_timeout);
    w.register(jkind, process_hard_task(tx));
    let mut w = w.connect(None).await.unwrap();

    // start consuming
    let (tx, rx) = channel();
    let jh = tokio::spawn(async move { w.run(&[qname], Some(rx)).await });

    // enqueue the job and wait for a message from the handler and ...
    cl.enqueue(j).await.unwrap();
    rx_for_test_purposes.recv().await;
    // ... immediately signal to return control
    tx.send(Message::ReturnControl).expect("sent ok");

    // one worker was processing a task when we interrupted it
    let nrunning = jh.await.expect("joined ok").unwrap();
    assert_eq!(nrunning, 1);

    // let's repeat the same actions with a little tweak:
    // we will signal to return control right away.
    let (tx, mut rx_for_test_purposes) = tokio::sync::mpsc::channel::<bool>(1);
    let tx = sync::Arc::new(tx);
    let mut w = WorkerBuilder::default();
    w.graceful_shutdown_period(shutdown_timeout);
    w.register(jkind, process_hard_task(tx));
    let mut w = w.connect(None).await.unwrap();
    let (tx, rx) = channel();
    let jh = tokio::spawn(async move { w.run(&[qname], Some(rx)).await });
    cl.enqueue(JobBuilder::new(jkind).queue(qname).args(vec![1000]).build())
        .await
        .unwrap();
    rx_for_test_purposes.recv().await;
    // signalling to yield immediately
    tx.send(Message::ReturnControlNow).expect("sent ok");
    let nrunning = jh.await.expect("joined ok").unwrap();
    // we did not even have a change to examine the current workers state
    assert_eq!(nrunning, 0);
}
