use crate::utils::launch_isolated_faktory;
use crate::{assert_gt, assert_gte, assert_lt, skip_check, skip_if_containers_not_enabled};
use chrono::Utc;
use faktory::mutate::{Filter, JobSet};
use faktory::{
    Client, Job, JobBuilder, JobId, JobRunner, StopReason, Worker, WorkerBuilder, WorkerId,
};
use rand::Rng;
use serde_json::Value;
use std::collections::HashMap;
use std::panic::panic_any;
use std::sync::Arc;
use std::time::Duration;
use std::{io, sync};
use tokio::sync::mpsc::error::SendError;
use tokio::time::{self as tokio_time};
use tokio_util::sync::CancellationToken;

#[tokio::test(flavor = "multi_thread")]
async fn hello_client() {
    skip_check!();
    let p = Client::connect().await.unwrap();
    drop(p);
}

#[tokio::test(flavor = "multi_thread")]
async fn hello_worker() {
    skip_check!();
    let w = Worker::builder::<io::Error>()
        .hostname("tester".to_string())
        .labels(vec!["foo".to_string(), "bar".to_string()])
        .register_fn("never_called", |_| async move { unreachable!() })
        .connect()
        .await
        .unwrap();
    drop(w);
}

#[tokio::test(flavor = "multi_thread")]
async fn enqueue_job() {
    skip_check!();
    let mut p = Client::connect().await.unwrap();
    p.enqueue(JobBuilder::new("order").build()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn roundtrip() {
    skip_check!();

    let local = "roundtrip";
    let jid = JobId::new("x-job-id-0123456782");

    let mut worker = Worker::builder()
        .labels(vec!["rust".into(), local.into()])
        .workers(1)
        .wid(WorkerId::random())
        .register_fn("order", move |job| async move {
            assert_eq!(job.kind(), "order");
            assert_eq!(job.queue, local);
            assert_eq!(job.args(), &[Value::from("ISBN-13:9781718501850")]);
            Ok::<(), io::Error>(())
        })
        .register_fn("image", |_| async move { unreachable!() })
        .connect()
        .await
        .unwrap();

    let mut client = Client::connect().await.unwrap();
    client
        .enqueue(
            JobBuilder::new("order")
                .jid(jid)
                .args(vec!["ISBN-13:9781718501850"])
                .queue(local)
                .build(),
        )
        .await
        .unwrap();
    let had_one = worker.run_one(0, &[local]).await.unwrap();
    assert!(had_one);

    let drained = !worker.run_one(0, &[local]).await.unwrap();
    assert!(drained);
}

#[tokio::test(flavor = "multi_thread")]
async fn server_state() {
    skip_check!();

    let local = "server_state";

    // prepare a worker
    let mut w = WorkerBuilder::default()
        .register_fn(local, move |_| async move { Ok::<(), io::Error>(()) })
        .connect()
        .await
        .unwrap();

    // prepare a producing client
    let mut client = Client::connect().await.unwrap();

    // examine server state before pushing anything
    let server_state = client.current_info().await.unwrap();
    // the Faktory release we are writing bindings and testing
    // against is at least "1.8.0"
    assert_eq!(server_state.server.version.major, 1);
    assert_gte!(server_state.server.version.minor, 8);
    assert!(server_state.data.queues.get(local).is_none());
    // the following two assertions are not super-helpful but
    // there is not much info we can make meaningful assetions on anyhow
    // (like memusage, server description string, version, etc.)
    assert_gte!(
        server_state.server.connections,
        2,
        "{}",
        server_state.server.connections
    ); // at least two clients from the current test
    assert_ne!(server_state.server.uptime.as_secs(), 0); // if IPC is happenning, this should hold :)

    // push 1 job
    client
        .enqueue(
            JobBuilder::new(local)
                .args(vec!["abc"])
                .queue(local)
                .build(),
        )
        .await
        .unwrap();

    // let's give Faktory a second to get updated
    tokio_time::sleep(Duration::from_secs(1)).await;

    // we only pushed 1 job on this queue
    let server_state = client.current_info().await.unwrap();
    assert_eq!(*server_state.data.queues.get(local).unwrap(), 1);

    // It is tempting to make an assertion like `total enqueued this time >= total enqueued last time + 1`,
    // but since we've got a server shared among numerous tests that are running in parallel, this
    // assertion will not always work (though it will be true in the majority of test runs). Imagine
    // a situation where between our last asking for server data state and now they have consumed all
    // the pending jobs in _other_ queues. This is highly unlikely, but _is_ possible. So the only
    // more or less guaranteed thing is that there is one pending job in our _local_ queue. It is "more or less"
    // because another test _may_ still push onto and consume from this queue if we copypasta this test's
    // contents and forget to update the local queue name, i.e. do not guarantee isolation at the queues level.
    assert_gte!(
        server_state.data.total_enqueued,
        1,
        "`total_enqueued` equals {} which is not greater than or equal to {}",
        server_state.data.total_enqueued,
        1
    );
    // Similar to the case above, we may want to assert `number of queues this time >= number of queues last time + 1`,
    // but it may not always hold, due to the fact that there is a `remove_queue` operation and if they
    // use it in other tests as a clean-up phase (just like we are doing at the end of this test),
    // the `server_state.data.total_queues` may reach `1`, meaning only our local queue is left.
    assert_gte!(
        server_state.data.total_queues,
        1,
        "`total_queues` equals {} which is not greater than or equal to {}",
        server_state.data.total_queues,
        1
    );

    // let's know consume that job ...
    assert!(w.run_one(0, &[local]).await.unwrap());

    // ... and verify the queue has got 0 pending jobs
    //
    // NB! If this is not passing locally, make sure to launch a fresh Faktory container,
    // because if you have not pruned its volume the Faktory will still keep the queue name
    // as registered.
    // But generally, we are performing a clean-up by consuming the jobs from the local queue/
    // and then deleting the queue programmatically, so there is normally no need to prune docker
    // volumes to perform the next test run. Also note that on CI we are always starting a-fresh.
    let server_state = client.current_info().await.unwrap();
    assert_eq!(*server_state.data.queues.get(local).unwrap(), 0);
    // `total_processed` should be at least +1 job from last read
    assert_gte!(
        server_state.data.total_processed,
        1,
        "{}",
        server_state.data.total_processed
    );

    client.queue_remove(&[local]).await.unwrap();
    assert!(client
        .current_info()
        .await
        .unwrap()
        .data
        .queues
        .get(local)
        .is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn multi() {
    skip_check!();
    let local = "multi_async";

    let (tx, rx) = sync::mpsc::channel();
    let tx = sync::Arc::new(sync::Mutex::new(tx));

    let mut w = WorkerBuilder::default()
        .hostname("tester".to_string())
        .wid(WorkerId::new(local))
        .register_fn(local, move |j| {
            let tx = sync::Arc::clone(&tx);
            Box::pin(async move {
                tx.lock().unwrap().send(j).unwrap();
                Ok::<(), io::Error>(())
            })
        })
        .connect()
        .await
        .unwrap();

    let mut p = Client::connect().await.unwrap();
    p.enqueue(Job::new(local, vec![Value::from(1), Value::from("foo")]).on_queue(local))
        .await
        .unwrap();
    p.enqueue(Job::new(local, vec![Value::from(2), Value::from("bar")]).on_queue(local))
        .await
        .unwrap();

    w.run_one(0, &[local]).await.unwrap();
    let job = rx.recv().unwrap();
    assert_eq!(job.queue, local);
    assert_eq!(job.kind(), local);
    assert_eq!(job.args(), &[Value::from(1), Value::from("foo")]);

    w.run_one(0, &[local]).await.unwrap();
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

    let mut w = WorkerBuilder::default()
        .hostname("tester".to_string())
        .wid(WorkerId::new(local))
        .register_fn(local, move |j| {
            let tx = sync::Arc::clone(&tx);
            Box::pin(async move {
                tx.lock().unwrap().send(j).unwrap();
                Err(io::Error::new(io::ErrorKind::Other, "nope"))
            })
        })
        .connect()
        .await
        .unwrap();

    let mut p = Client::connect().await.unwrap();

    // note that *enqueueing* the jobs didn't fail!
    p.enqueue(Job::new(local, vec![Value::from(1), Value::from("foo")]).on_queue(local))
        .await
        .unwrap();
    p.enqueue(Job::new(local, vec![Value::from(2), Value::from("bar")]).on_queue(local))
        .await
        .unwrap();

    w.run_one(0, &[local]).await.unwrap();
    w.run_one(0, &[local]).await.unwrap();
    drop(w);
    assert_eq!(rx.into_iter().take(2).count(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn queue_control_actions() {
    skip_check!();

    let local_1 = "queue_control_pause_and_resume_1";
    let local_2 = "queue_control_pause_and_resume_2";

    let (tx, rx) = sync::mpsc::channel();
    let tx_1 = sync::Arc::new(sync::Mutex::new(tx));
    let tx_2 = sync::Arc::clone(&tx_1);

    let mut worker = WorkerBuilder::default()
        .hostname("tester".to_string())
        .wid(WorkerId::new(local_1))
        .register_fn(local_1, move |_job| {
            let tx = sync::Arc::clone(&tx_1);
            Box::pin(async move { tx.lock().unwrap().send(true) })
        })
        .register_fn(local_2, move |_job| {
            let tx = sync::Arc::clone(&tx_2);
            Box::pin(async move { tx.lock().unwrap().send(true) })
        })
        .connect()
        .await
        .unwrap();

    let mut client = Client::connect().await.unwrap();

    // enqueue three jobs
    client
        .enqueue_many([
            Job::new(local_1, vec![Value::from(1)]).on_queue(local_1),
            Job::new(local_1, vec![Value::from(1)]).on_queue(local_1),
            Job::new(local_1, vec![Value::from(1)]).on_queue(local_1),
        ])
        .await
        .unwrap();

    // pause the queue
    client.queue_pause(&[local_1]).await.unwrap();

    // try to consume from that queue
    let had_job = worker.run_one(0, &[local_1]).await.unwrap();
    assert!(!had_job);
    let worker_executed = rx.try_recv().is_ok();
    assert!(!worker_executed);

    // resume that queue and ...
    client.queue_resume(&[local_1]).await.unwrap();

    // ... be able to consume from it
    let had_job = worker.run_one(0, &[local_1]).await.unwrap();
    assert!(had_job);
    let worker_executed = rx.try_recv().is_ok();
    assert!(worker_executed);

    // push two jobs on the other queue (reminder: we got two jobs
    // remaining on the first queue):
    client
        .enqueue_many([
            Job::new(local_2, vec![Value::from(1)]).on_queue(local_2),
            Job::new(local_2, vec![Value::from(1)]).on_queue(local_2),
        ])
        .await
        .unwrap();

    // pause both queues the queues
    client.queue_pause(&[local_1, local_2]).await.unwrap();

    // try to consume from them
    assert!(!worker.run_one(0, &[local_1]).await.unwrap());
    assert!(!worker.run_one(0, &[local_2]).await.unwrap());
    assert!(!rx.try_recv().is_ok());

    // now, resume the queues and ...
    client.queue_resume(&[local_1, local_2]).await.unwrap();

    // ... be able to consume from both of them
    assert!(worker.run_one(0, &[local_1]).await.unwrap());
    assert!(rx.try_recv().is_ok());
    assert!(worker.run_one(0, &[local_2]).await.unwrap());
    assert!(rx.try_recv().is_ok());

    // let's inspect the sever state
    let server_state = client.current_info().await.unwrap();
    let queues = &server_state.data.queues;
    assert_eq!(*queues.get(local_1).unwrap(), 1); // 1 job remaining
    assert_eq!(*queues.get(local_2).unwrap(), 1); // also 1 job remaining

    // let's now remove the queues
    client.queue_remove(&[local_1, local_2]).await.unwrap();

    // though there _was_ a job in each queue, consuming from
    // the removed queues will not yield anything
    assert!(!worker.run_one(0, &[local_1]).await.unwrap());
    assert!(!worker.run_one(0, &[local_2]).await.unwrap());
    assert!(!rx.try_recv().is_ok());

    // let's inspect the sever state again
    let server_state = client.current_info().await.unwrap();
    let queues = &server_state.data.queues;
    // our queue are not even mentioned in the server report:
    assert!(queues.get(local_1).is_none());
    assert!(queues.get(local_2).is_none());
}

// Run the following test with:
// FAKTORY_URL=tcp://127.0.0.1:7419 cargo test --locked --all-features --all-targets queue_control_actions_wildcard -- --include-ignored
#[tokio::test(flavor = "multi_thread")]
#[ignore = "this test requires a dedicated test run since the commands being tested will affect all queues on the Faktory server"]
async fn queue_control_actions_wildcard() {
    skip_check!();

    let local_1 = "queue_control_wildcard_1";
    let local_2 = "queue_control_wildcard_2";

    // prepare a client and remove any left-overs
    // from the previous test run
    let mut client = Client::connect().await.unwrap();
    client
        .requeue(JobSet::Retries, Filter::from_kind(local_1))
        .await
        .unwrap();
    client
        .requeue(JobSet::Retries, Filter::from_kind(local_2))
        .await
        .unwrap();
    client.queue_remove(&[local_1]).await.unwrap();
    client.queue_remove(&[local_2]).await.unwrap();

    let (tx, rx) = sync::mpsc::channel();
    let tx_1 = sync::Arc::new(sync::Mutex::new(tx));
    let tx_2 = sync::Arc::clone(&tx_1);

    let mut worker = WorkerBuilder::default()
        .hostname("tester".to_string())
        .wid(WorkerId::new(local_1))
        .register_fn(local_1, move |_job| {
            let tx = sync::Arc::clone(&tx_1);
            Box::pin(async move { tx.lock().unwrap().send(true) })
        })
        .register_fn(local_2, move |_job| {
            let tx = sync::Arc::clone(&tx_2);
            Box::pin(async move { tx.lock().unwrap().send(true) })
        })
        .connect()
        .await
        .unwrap();

    // enqueue two jobs on each queue
    client
        .enqueue_many([
            Job::new(local_1, vec![Value::from(1)]).on_queue(local_1),
            Job::new(local_1, vec![Value::from(1)]).on_queue(local_1),
            Job::new(local_2, vec![Value::from(1)]).on_queue(local_2),
            Job::new(local_2, vec![Value::from(1)]).on_queue(local_2),
        ])
        .await
        .unwrap();

    // pause all queues the queues
    client.queue_pause_all().await.unwrap();

    // try to consume from queues
    assert!(!worker.run_one(0, &[local_1]).await.unwrap());
    assert!(!worker.run_one(0, &[local_2]).await.unwrap());
    assert!(!rx.try_recv().is_ok());

    // now, resume all the queues and ...
    client.queue_resume_all().await.unwrap();

    // ... be able to consume from both of them
    assert!(worker.run_one(0, &[local_1]).await.unwrap());
    assert!(rx.try_recv().is_ok());
    assert!(worker.run_one(0, &[local_2]).await.unwrap());
    assert!(rx.try_recv().is_ok());

    // let's inspect the sever state
    let server_state = client.current_info().await.unwrap();
    let queues = &server_state.data.queues;
    assert_eq!(*queues.get(local_1).unwrap(), 1); // 1 job remaining
    assert_eq!(*queues.get(local_2).unwrap(), 1); // also 1 job remaining

    // let's now remove all the queues
    client.queue_remove_all().await.unwrap();

    // though there _was_ a job in each queue, consuming from
    // the removed queues will not yield anything
    assert!(!worker.run_one(0, &[local_1]).await.unwrap());
    assert!(!worker.run_one(0, &[local_2]).await.unwrap());
    assert!(!rx.try_recv().is_ok());

    // let's inspect the sever state again
    let server_state = client.current_info().await.unwrap();
    let queues = &server_state.data.queues;
    // our queue are not even mentioned in the server report:
    assert!(queues.get(local_1).is_none());
    assert!(queues.get(local_2).is_none());

    // let's also test here one bit from the Faktory MUTATION API,
    // which affects the entire target set;
    // for this, let's enqueue a few jobs that are not supposed to be
    // consumed immediately, rather in a few minutes; this they these
    // jobs will get into the `scheduled` set
    let soon = Utc::now() + chrono::Duration::seconds(2);
    client
        .enqueue_many([
            Job::builder(local_1)
                .args(vec![Value::from(1)])
                .queue(local_1)
                .at(soon)
                .build(),
            Job::builder(local_1)
                .args(vec![Value::from(1)])
                .queue(local_1)
                .at(soon)
                .build(),
            Job::builder(local_2)
                .args(vec![Value::from(1)])
                .queue(local_2)
                .at(soon)
                .build(),
            Job::builder(local_2)
                .args(vec![Value::from(1)])
                .queue(local_2)
                .at(soon)
                .build(),
        ])
        .await
        .unwrap();

    // now, let's just clear all the scheduled jobs
    client.clear(JobSet::Scheduled).await.unwrap();

    tokio_time::sleep(Duration::from_secs(2)).await;

    // the queue is empty
    assert!(!worker.run_one(0, &[local_1]).await.unwrap());

    // even if we force-schedule those jobs
    client
        .requeue(JobSet::Scheduled, Filter::empty())
        .await
        .unwrap();

    // still empty, meaing the jobs have been purged for good
    assert!(!worker.run_one(0, &[local_1]).await.unwrap());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_jobs_pushed_in_bulk() {
    skip_check!();

    let local_1 = "test_jobs_pushed_in_bulk_1";
    let local_2 = "test_jobs_pushed_in_bulk_2";
    let local_3 = "test_jobs_pushed_in_bulk_3";
    let local_4 = "test_jobs_pushed_in_bulk_4";

    let mut p = Client::connect().await.unwrap();
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
            Job::builder("broken")
                .jid(JobId::new("short"))
                .queue(local_3)
                .build(), // jid.len() < 8
            Job::builder("") // empty string jobtype
                .jid(JobId::new("3sZCbdp8e9WX__0"))
                .queue(local_3)
                .build(),
            Job::builder("broken")
                .jid(JobId::new("3sZCbdp8e9WX__1"))
                .queue(local_3)
                .reserve_for(Duration::from_secs(864001)) // reserve_for exceeded
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
    let mut c = WorkerBuilder::default()
        .hostname("tester".to_string())
        .wid(WorkerId::new(local_3))
        .register_fn("very_special", move |_job| async {
            Ok::<(), io::Error>(())
        })
        .register_fn("broken", move |_job| async { Ok::<(), io::Error>(()) })
        .connect()
        .await
        .unwrap();

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
    let mut cl = Client::connect().await.unwrap();
    let mut w = Worker::builder()
        .register_fn("rebuild_index", assert_args_empty)
        .register_fn("register_order", assert_args_not_empty)
        .connect()
        .await
        .unwrap();

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

use std::future::Future;
use std::pin::Pin;
use tokio::sync::mpsc::{self, Sender};
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
            let complexity = j.args()[0].as_u64().unwrap();
            sender.send(true).await.unwrap(); // inform that we are now starting to process the job
            tokio::time::sleep(tokio::time::Duration::from_millis(complexity)).await;
            Ok::<(), io::Error>(())
        })
    });
}

#[tokio::test(flavor = "multi_thread")]
async fn test_shutdown_signals_handling() {
    skip_check!();

    let qname = "test_shutdown_signals_handling";
    let jkind = "heavy";
    let shutdown_timeout = Duration::from_millis(500);

    // get a client and a job to enqueue
    let mut cl = Client::connect().await.unwrap();
    let j = JobBuilder::new(jkind)
        .queue(qname)
        // task will be being processed for at least 1 second
        .args(vec![1000])
        .build();

    let (tx, mut rx_for_test_purposes) = tokio::sync::mpsc::channel::<bool>(1);
    let tx = sync::Arc::new(tx);

    // create a token
    let token = CancellationToken::new();
    let child_token = token.child_token();

    // create a signalling future
    let signal = async move { child_token.cancelled().await };

    // get a connected worker
    let mut w = WorkerBuilder::default()
        .with_graceful_shutdown(signal)
        .shutdown_timeout(shutdown_timeout)
        .register_fn(jkind, process_hard_task(tx))
        .connect()
        .await
        .unwrap();

    // start consuming
    let jh = tokio::spawn(async move { w.run(&[qname]).await });

    // enqueue the job and wait for a message from the handler and ...
    cl.enqueue(j).await.unwrap();
    rx_for_test_purposes.recv().await;

    assert!(!jh.is_finished());

    // ... immediately signal to return control
    token.cancel();

    // one worker was processing a task when we interrupted it
    let stop_details = jh.await.expect("joined ok").unwrap();
    assert_eq!(stop_details.reason, StopReason::GracefulShutdown);
    assert_eq!(stop_details.workers_still_running, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_jobs_with_blocking_handlers() {
    skip_check!();

    let local = "test_jobs_with_blocking_handlers";

    let mut w = Worker::builder()
        .register_blocking_fn("cpu_intensive", |_j| {
            // Imagine some compute heavy operations:serializing, sorting, matrix multiplication, etc.
            std::thread::sleep(Duration::from_millis(1000));
            Ok::<(), io::Error>(())
        })
        .register_fn("io_intensive", |_j| async move {
            // Imagine fetching data for this user from various origins,
            // updating an entry on them in the database, and then sending them
            // an email and pushing a follow-up task on the Faktory queue
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok::<(), io::Error>(())
        })
        .register_fn(
            "general_workload",
            |_j| async move { Ok::<(), io::Error>(()) },
        )
        .connect()
        .await
        .unwrap();

    Client::connect()
        .await
        .unwrap()
        .enqueue_many([
            Job::builder("cpu_intensive").queue(local).build(),
            Job::builder("io_intensive").queue(local).build(),
            Job::builder("general_workload").queue(local).build(),
        ])
        .await
        .unwrap();

    for _ in 0..2 {
        assert!(w.run_one(0, &[local]).await.unwrap());
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_panic_and_errors_in_handler() {
    skip_check!();

    let job_kind_vs_error_msg = HashMap::from([
        ("panic_SYNC_handler_str", "panic_SYNC_handler_str"),
        ("panic_SYNC_handler_String", "panic_SYNC_handler_String"),
        ("panic_SYNC_handler_int", "job processing panicked"),
        ("error_from_SYNC_handler", "error_from_SYNC_handler"),
        ("panic_ASYNC_handler_str", "panic_ASYNC_handler_str"),
        ("panic_ASYNC_handler_String", "panic_ASYNC_handler_String"),
        ("panic_ASYNC_handler_int", "job processing panicked"),
        ("error_from_ASYNC_handler", "error_from_ASYNC_handler"),
        (
            "no_handler_registered_for_this_jobtype_initially",
            "No handler for no_handler_registered_for_this_jobtype_initially",
        ),
    ]);
    let njobs = job_kind_vs_error_msg.keys().len();

    // clean up is needed when re-using the same Faktory container, since the
    // Faktory server could have re-scheduled (or might be doing it right now)
    // the failed jobs from the previous test run; to keep things clean, we are
    // force-rescheduling and immediatey dropping any remainders
    let local = "test_panic_and_errors_in_handler";
    let mut c = Client::connect().await.unwrap();
    let pattern = format!(r#"*\"args\":\[\"{}\"\]*"#, local);
    c.requeue(JobSet::Retries, Filter::from_pattern(pattern.as_str()))
        .await
        .unwrap();
    c.queue_remove(&[local]).await.unwrap();

    let mut w = Worker::builder::<io::Error>()
        //  sync handlers
        .register_blocking_fn("panic_SYNC_handler_str", |_j| {
            panic!("panic_SYNC_handler_str");
        })
        .register_blocking_fn("panic_SYNC_handler_String", |_j| {
            panic_any("panic_SYNC_handler_String".to_string());
        })
        .register_blocking_fn("panic_SYNC_handler_int", |_j| {
            panic_any(0);
        })
        .register_blocking_fn("error_from_SYNC_handler", |_j| {
            Err::<(), io::Error>(io::Error::new(
                io::ErrorKind::Other,
                "error_from_SYNC_handler",
            ))
        })
        // async handlers
        .register_fn("panic_ASYNC_handler_str", |_j| async move {
            panic!("panic_ASYNC_handler_str");
        })
        .register_fn("panic_ASYNC_handler_String", |_j| async move {
            panic_any("panic_ASYNC_handler_String".to_string());
        })
        .register_fn("panic_ASYNC_handler_int", |_j| async move {
            panic_any(0);
        })
        .register_fn("error_from_ASYNC_handler", |_j| async move {
            Err::<(), io::Error>(io::Error::new(
                io::ErrorKind::Other,
                "error_from_ASYNC_handler",
            ))
        })
        .connect()
        .await
        .unwrap();

    // let's enqueue jobs first time and ...
    c.enqueue_many(
        job_kind_vs_error_msg
            .keys()
            .map(|&jkind| Job::builder(jkind).queue(local).args([local]).build())
            .collect::<Vec<_>>(),
    )
    .await
    .unwrap();

    // ... consume all the jobs from the queue and _fail_ them
    // "in different ways" (see our worker setup above);
    //
    // we _did_ consume and process the job, the processing result itself though
    // was a failure; however, a panic in the handler was "intercepted" and communicated
    // to the Faktory server via the FAIL command, the error message and the backtrace, if any,
    // will then be available to a Web UI user or to the worker consumes the retried job
    // (if `Job::retry` is Some and > 0 and there are `Failure::retry_remaining` attempts left)
    // in this job's `failure` field; see how we are consuming the retried jobs later
    // in this test and examin the failure details;
    //
    // also note how the test run is not interrupted here with a panic
    for _ in 0..njobs {
        assert!(w.run_one(0, &[local]).await.unwrap());
    }

    // let's now make sure all the jobs are re-enqueued
    c.requeue(JobSet::Retries, Filter::from_pattern(pattern.as_str()))
        .await
        .unwrap();

    // now, let's create a worker who will only send jobs to the
    // test's main thread to make some assertions;
    struct JobHandler {
        chan: Arc<Sender<Job>>,
    }
    impl JobHandler {
        pub fn new(chan: Arc<Sender<Job>>) -> Self {
            Self { chan }
        }
    }
    #[async_trait::async_trait]
    impl JobRunner for JobHandler {
        type Error = SendError<Job>;
        async fn run(&self, job: Job) -> Result<(), Self::Error> {
            self.chan.send(job).await
        }
    }
    let (tx, mut rx) = tokio::sync::mpsc::channel(njobs);
    let tx = sync::Arc::new(tx);

    // unlike the previus worker, this one is not failing the jobs,
    // it is rather helping us to inspect them
    let mut w = Worker::builder()
        .register("panic_SYNC_handler_str", JobHandler::new(tx.clone()))
        .register("panic_SYNC_handler_String", JobHandler::new(tx.clone()))
        .register("panic_SYNC_handler_int", JobHandler::new(tx.clone()))
        .register("error_from_SYNC_handler", JobHandler::new(tx.clone()))
        .register("panic_ASYNC_handler_str", JobHandler::new(tx.clone()))
        .register("panic_ASYNC_handler_String", JobHandler::new(tx.clone()))
        .register("panic_ASYNC_handler_int", JobHandler::new(tx.clone()))
        .register("error_from_ASYNC_handler", JobHandler::new(tx.clone()))
        .register(
            "no_handler_registered_for_this_jobtype_initially",
            JobHandler::new(tx.clone()),
        )
        .connect()
        .await
        .unwrap();

    for _ in 0..njobs {
        assert!(w.run_one(0, &[local]).await.unwrap()); // reminder: we've requeued the failed jobs
    }

    // Let's await till the worker sends the jobs to us.
    //
    // Note that if a tokio task inside `Worker::run_job` in cancelled(1), we may not receive a job
    // via the channel and so `rx.recv_many` will just hang (and so the entire test run),
    // hence a timeout we are adding here.
    //
    // (1)If you are curious, this can be easily reproduced inside the `Callback::Async(_)` arm
    // of `Worker::run_job`, by swapping this line:
    // ```rust
    // spawn(processing_task).await
    // ```
    // for something like:
    // ```rust
    // let handle = spawn(processing_task);
    // handle.abort();
    // handle.await
    // ```
    // and then running this test.
    let mut jobs: Vec<Job> = Vec::with_capacity(njobs);
    let nreceived =
        tokio::time::timeout(Duration::from_secs(5), rx.recv_many(jobs.as_mut(), njobs))
            .await
            .expect("all jobs to be recieved within the timeout period");

    // all the jobs are now in the test's main thread
    assert_eq!(nreceived, njobs);

    // let's verify that errors messages in each job's `Failure` are as expected
    for job in jobs {
        let error_message_got = job.failure().as_ref().unwrap().message.as_ref().unwrap();
        let error_message_expected = *job_kind_vs_error_msg.get(job.kind()).unwrap();
        assert_eq!(error_message_got, error_message_expected);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn mutation_requeue_jobs() {
    // for this test we need to launch a dedicated instance of the "Faktory" server
    // in docker container - this is to guarantee isolation
    skip_if_containers_not_enabled!();

    let local = "mutation_requeue_jobs";
    let ctx = launch_isolated_faktory(None).await;
    let test_started_at = Utc::now();
    let max_retries: u32 = rand::rng().random_range(2..25);
    let panic_message = "Failure should be recorded";

    // prepare a client ...
    let mut client = Client::connect_to(&ctx.faktory_url).await.unwrap();
    // ... and a worker that will fail the job unconditionally
    let mut worker = Worker::builder::<io::Error>()
        .register_fn(local, move |_job| async move {
            panic_any(panic_message);
        })
        .connect_to(&ctx.faktory_url)
        .await
        .unwrap();

    // enqueue a job
    let job = JobBuilder::new(local)
        .queue(local)
        .retry(max_retries as isize)
        .build();
    let job_id = job.id().clone();
    client.enqueue(job).await.unwrap();

    // consume and fail it
    let had_one = worker.run_one(0, &[local]).await.unwrap();
    assert!(had_one);

    // the job is now in `retries` set and is due to
    // be rescheduled by the Faktory server after a while, but ...
    let had_one = worker.run_one(0, &[local]).await.unwrap();
    assert!(!had_one);

    // ... we can force it, so let's requeue the job and ...
    client
        .requeue(JobSet::Retries, Filter::from_ids(&[&job_id]))
        .await
        .unwrap();

    // ... this time, instead of failing the job, let's
    // create a new woker that will just send the job
    // to the test thread so that we can inspect and
    // assert on the failure from the first run
    let (tx, rx) = sync::mpsc::channel();
    let tx = sync::Arc::new(sync::Mutex::new(tx));
    let mut w = WorkerBuilder::default()
        .hostname("tester".to_string())
        .wid(WorkerId::new(local))
        .register_fn(local, move |j| {
            let tx = sync::Arc::clone(&tx);
            Box::pin(async move {
                tx.lock().unwrap().send(j).unwrap();
                Ok::<(), io::Error>(())
            })
        })
        .connect_to(&ctx.faktory_url)
        .await
        .unwrap();
    assert!(w.run_one(0, &[local]).await.unwrap());
    let job = rx.recv().unwrap();

    assert_eq!(job.id(), &job_id); // sanity check

    let failure_info = job.failure().unwrap();
    assert_eq!(failure_info.retry_count, 0);
    assert_eq!(
        failure_info.retry_remaining,
        max_retries as usize - failure_info.retry_count
    );
    assert_lt!(failure_info.failed_at, Utc::now());
    assert_gt!(failure_info.failed_at, test_started_at);
    assert!(failure_info.next_at.is_some());
    assert_eq!(failure_info.message.as_ref().unwrap(), panic_message);
    assert!(failure_info.backtrace.is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn mutation_kill_and_requeue_and_discard() {
    skip_check!();

    // prepare a client and clean up the queue
    // to ensure there are no left-overs
    let local = "mutation_kill_vs_discard";
    let mut client = Client::connect().await.unwrap();
    client
        .requeue(JobSet::Retries, Filter::from_kind(local))
        .await
        .unwrap();

    // enqueue a couple of jobs and ...
    client.queue_remove(&[local]).await.unwrap();
    let soon = Utc::now() + chrono::Duration::seconds(2);
    client
        .enqueue_many([
            Job::builder(local)
                .args(vec![Value::from(1)])
                .queue(local)
                .at(soon)
                .build(),
            Job::builder(local)
                .args(vec![Value::from(2)])
                .queue(local)
                .at(soon)
                .build(),
        ])
        .await
        .unwrap();

    // kill them ...
    client
        .kill(JobSet::Scheduled, Filter::from_kind(local))
        .await
        .unwrap();

    // the two jobs were moved from `scheduled` to `dead`,
    // and so the queue is empty
    let njobs = client
        .current_info()
        .await
        .unwrap()
        .data
        .queues
        .get(local)
        .map(|v| *v)
        .unwrap_or_default();
    assert_eq!(njobs, 0);

    // let's now enqueue those jobs
    client
        .requeue(JobSet::Dead, Filter::from_kind(local))
        .await
        .unwrap();

    // they transitioned from `dead` to being enqueued
    let njobs = client
        .current_info()
        .await
        .unwrap()
        .data
        .queues
        .get(local)
        .map(|v| *v)
        .unwrap_or_default();
    assert_eq!(njobs, 2);

    // prepare a worker that will fail the job unconditionally
    let mut worker = Worker::builder::<io::Error>()
        .register_fn(local, move |_job| async move {
            panic!("force fail this job");
        })
        .connect()
        .await
        .unwrap();

    // cosume them (and immediately fail them) and make
    // sure the queue is drained
    assert!(worker.run_one(0, &[local]).await.unwrap());
    assert!(worker.run_one(0, &[local]).await.unwrap());
    assert!(!worker.run_one(0, &[local]).await.unwrap());
    let njobs = client
        .current_info()
        .await
        .unwrap()
        .data
        .queues
        .get(local)
        .map(|v| *v)
        .unwrap_or_default();
    assert_eq!(njobs, 0); // sanity check

    // so the jobs have transitioned from being enqueued
    // to the `retries` set, and we can now completely discard them
    client
        .discard(JobSet::Retries, Filter::from_kind(local))
        .await
        .unwrap();

    // Double-check
    client
        .requeue(JobSet::Retries, Filter::from_kind(local))
        .await
        .unwrap();
    client
        .requeue(JobSet::Dead, Filter::from_kind(local))
        .await
        .unwrap();
    client
        .requeue(JobSet::Scheduled, Filter::from_kind(local))
        .await
        .unwrap();

    // Gone for good
    let njobs = client
        .current_info()
        .await
        .unwrap()
        .data
        .queues
        .get(local)
        .map(|v| *v)
        .unwrap_or_default();
    assert_eq!(njobs, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn mutation_requeue_specific_jobs_only() {
    skip_check!();

    // prepare a client and clean up the queue
    // to ensure there are no left-overs
    let local = "mutation_requeue_specific_jobs_only";
    let mut client = Client::connect().await.unwrap();
    client
        .requeue(JobSet::Retries, Filter::from_kind(local))
        .await
        .unwrap();
    client.queue_remove(&[local]).await.unwrap();

    // prepare a worker that will fail the job unconditionally
    let mut worker = Worker::builder::<io::Error>()
        .register_fn(local, move |_job| async move {
            panic!("force fail this job");
        })
        .connect()
        .await
        .unwrap();

    // enqueue two jobs
    let job1 = JobBuilder::new(local)
        .retry(10)
        .queue(local)
        .args(["fizz"])
        .build();
    let job_id1 = job1.id().clone();
    let job2 = JobBuilder::new(local)
        .retry(10)
        .queue(local)
        .args(["buzz"])
        .build();
    let job_id2 = job2.id().clone();
    assert_ne!(job_id1, job_id2); // sanity check
    client.enqueue_many([job1, job2]).await.unwrap();

    // cosume them (and immediately fail) and make
    // sure the queue is drained
    assert!(worker.run_one(0, &[local]).await.unwrap());
    assert!(worker.run_one(0, &[local]).await.unwrap());
    assert!(!worker.run_one(0, &[local]).await.unwrap());

    // now, let's requeue a job by id:
    client
        .requeue(JobSet::Retries, Filter::from_ids(&[&job_id1]))
        .await
        .unwrap();

    // we can now see that only one job has been re-scheduled, just like we wanted
    // see: https://github.com/contribsys/faktory/blob/10ccc2270dc2a1c95c3583f7c291a51b0292bb62/server/mutate.go#L100-L110
    assert!(worker.run_one(0, &[local]).await.unwrap());
    assert!(!worker.run_one(0, &[local]).await.unwrap()); // drained

    // and - for completeness - let's requeue the jobs using
    // the comination of `kind` + `pattern` (jobtype and regexp in Faktory's terms);
    // let's first make sure to force-reschedule the jobs:
    client
        .requeue(JobSet::Retries, Filter::empty())
        .await
        .unwrap();
    assert!(worker.run_one(0, &[local]).await.unwrap());
    assert!(worker.run_one(0, &[local]).await.unwrap());
    assert!(!worker.run_one(0, &[local]).await.unwrap()); // drained

    // now let's use `kind` + `pattern` combo to only immeduately
    // reschedule the job with "fizz" in the `args`, but not the
    // one with "buzz":
    client
        .requeue(
            JobSet::Retries,
            Filter::from_kind_and_pattern(local, r#"*\"args\":\[\"fizz\"\]*"#),
        )
        .await
        .unwrap();

    // and indeed only one job has behttps://github.com/contribsys/faktory/blob/b4a93227a3323ab4b1365b0c37c2fac4f9588cc8/server/mutate.go#L83-L94en re-scheduled,
    // see: https://github.com/contribsys/faktory/blob/b4a93227a3323ab4b1365b0c37c2fac4f9588cc8/server/mutate.go#L83-L94
    assert!(worker.run_one(0, &[local]).await.unwrap());
    assert!(!worker.run_one(0, &[local]).await.unwrap()); // drained

    // just for sanity's sake, let's re-queue the "buzz" one:
    client
        .requeue(
            JobSet::Retries,
            Filter::from_pattern(r#"*\"args\":\[\"buzz\"\]*"#),
        )
        .await
        .unwrap();
    assert!(worker.run_one(0, &[local]).await.unwrap());
    assert!(!worker.run_one(0, &[local]).await.unwrap()); // drained
}
