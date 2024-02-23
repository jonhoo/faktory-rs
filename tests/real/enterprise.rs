extern crate faktory;

use crate::skip_if_not_enterprise;
use crate::utils::learn_faktory_url;
use chrono::Utc;
use faktory::ent::*;
use faktory::*;
use serde_json::Value;
use std::io;
use tokio::time;

async fn print_job(j: Job) -> io::Result<()> {
    Ok(eprintln!("{:?}", j))
}
macro_rules! assert_had_one {
    ($c:expr, $q:expr) => {
        let had_one_job = $c.run_one(0, &[$q]).await.unwrap();
        assert!(had_one_job);
    };
}

macro_rules! assert_is_empty {
    ($c:expr, $q:expr) => {
        let had_one_job = $c.run_one(0, &[$q]).await.unwrap();
        assert!(!had_one_job);
    };
}

fn some_jobs<S>(kind: S, q: S, count: usize) -> impl Iterator<Item = Job>
where
    S: Into<String> + Clone + 'static,
{
    (0..count)
        .into_iter()
        .map(move |_| Job::builder(kind.clone()).queue(q.clone()).build())
}

#[tokio::test(flavor = "multi_thread")]
async fn ent_expiring_job() {
    skip_if_not_enterprise!();

    let url = learn_faktory_url();
    let local = "ent_expiring_job";

    // prepare a producer ("client" in Faktory terms) and consumer ("worker"):
    let mut p = Producer::connect(Some(&url)).await.unwrap();
    let mut c = ConsumerBuilder::default();
    c.register("AnExpiringJob", |j| Box::pin(print_job(j)));
    let mut c = c.connect(Some(&url)).await.unwrap();

    // prepare an expiring job:
    let job_ttl_secs: u64 = 3;

    let ttl = chrono::Duration::seconds(job_ttl_secs as i64);
    let job1 = JobBuilder::new("AnExpiringJob")
        .args(vec!["ISBN-13:9781718501850"])
        .queue(local)
        .expires_at(chrono::Utc::now() + ttl)
        .build();

    // enqueue and fetch immediately job1:
    p.enqueue(job1).await.unwrap();
    assert_had_one!(&mut c, "ent_expiring_job");

    // check that the queue is drained:
    assert_is_empty!(&mut c, "ent_expiring_job");

    // prepare another one:
    let job2 = JobBuilder::new("AnExpiringJob")
        .args(vec!["ISBN-13:9781718501850"])
        .queue(local)
        .expires_at(chrono::Utc::now() + ttl)
        .build();

    // enqueue and then fetch job2, but after ttl:
    p.enqueue(job2).await.unwrap();
    tokio::time::sleep(time::Duration::from_secs(job_ttl_secs * 2)).await;
    // For the non-enterprise edition of Faktory, this assertion will
    // fail, which should be taken into account when running the test suite on CI.
    assert_is_empty!(&mut c, local);
}

#[tokio::test(flavor = "multi_thread")]
async fn ent_unique_job() {
    use faktory::error;
    use serde_json::Value;

    skip_if_not_enterprise!();

    let url = learn_faktory_url();

    let job_type = "order";

    // prepare producer and consumer:
    let mut p = Producer::connect(Some(&url)).await.unwrap();
    let mut c = ConsumerBuilder::default();
    c.register(job_type, |j| Box::pin(print_job(j)));
    let mut c = c.connect(Some(&url)).await.unwrap();

    // Reminder. Jobs are considered unique for kind + args + queue.
    // So the following two jobs, will be accepted by Faktory, since we
    // are not setting 'unique_for' when creating those jobs:
    let queue_name = "ent_unique_job";
    let args = vec![Value::from("ISBN-13:9781718501850"), Value::from(100)];
    let job1 = JobBuilder::new(job_type)
        .args(args.clone())
        .queue(queue_name)
        .build();

    p.enqueue(job1).await.unwrap();
    let job2 = JobBuilder::new(job_type)
        .args(args.clone())
        .queue(queue_name)
        .build();

    p.enqueue(job2).await.unwrap();

    let had_job = c.run_one(0, &[queue_name]).await.unwrap();
    assert!(had_job);
    let had_another_one = c.run_one(0, &[queue_name]).await.unwrap();
    assert!(had_another_one);
    let and_that_is_it_for_now = !c.run_one(0, &[queue_name]).await.unwrap();
    assert!(and_that_is_it_for_now);

    // let's now create a unique job and followed by a job with
    // the same args and kind (jobtype in Faktory terms) and pushed
    // to the same queue:
    let unique_for_secs = 3;
    let job1 = Job::builder(job_type)
        .args(args.clone())
        .queue(queue_name)
        .unique_for(unique_for_secs)
        .build();

    p.enqueue(job1).await.unwrap();

    // this one is a 'duplicate' ...
    let job2 = Job::builder(job_type)
        .args(args.clone())
        .queue(queue_name)
        .unique_for(unique_for_secs)
        .build();
    // ... so the server will respond accordingly:
    let res = p.enqueue(job2).await.unwrap_err();

    if let error::Error::Protocol(error::Protocol::UniqueConstraintViolation { msg }) = res {
        assert_eq!(msg, "Job not unique");
    } else {
        panic!("Expected protocol error.")
    }

    // Let's now consume the job which is 'holding' a unique lock:
    let had_job = c.run_one(0, &[queue_name]).await.unwrap();

    assert!(had_job);

    // And check that the queue is really empty (`job2` from above
    // has not been queued indeed):
    let queue_is_empty = !c.run_one(0, &[queue_name]).await.unwrap();

    assert!(queue_is_empty);

    // Now let's repeat the latter case, but providing different args to job2:
    let job1 = JobBuilder::new(job_type)
        .args(args.clone())
        .queue(queue_name)
        .unique_for(unique_for_secs)
        .build();

    p.enqueue(job1).await.unwrap();

    // this one is *NOT* a 'duplicate' ...
    let job2 = JobBuilder::new(job_type)
        .args(vec![Value::from("ISBN-13:9781718501850"), Value::from(101)])
        .queue(queue_name)
        .unique_for(unique_for_secs)
        .build();
    // ... so the server will accept it:

    p.enqueue(job2).await.unwrap();

    let had_job = c.run_one(0, &[queue_name]).await.unwrap();
    assert!(had_job);
    let had_another_one = c.run_one(0, &[queue_name]).await.unwrap();
    assert!(had_another_one);

    assert_had_one!(&mut c, queue_name);
    assert_had_one!(&mut c, queue_name);
    // and the queue is empty again:
    assert_is_empty!(&mut c, queue_name);
}

#[tokio::test(flavor = "multi_thread")]
async fn ent_unique_job_until_success() {
    use faktory::error;
    use std::io;
    use tokio::time;

    skip_if_not_enterprise!();

    let url = learn_faktory_url();

    let queue_name = "ent_unique_job_until_success";
    let job_type = "order";

    // the job will be being executed for at least 3 seconds,
    // but is unique for 4 seconds;
    let difficulty_level = 3;
    let unique_for = 4;

    let url1 = url.clone();
    let handle = tokio::spawn(async move {
        // prepare producer and consumer, where the former can
        // send a job difficulty level as a job's args and the lattter
        // will sleep for a corresponding period of time, pretending
        // to work hard:
        let mut producer_a = Producer::connect(Some(&url1)).await.unwrap();
        let mut consumer_a = ConsumerBuilder::default_async();
        consumer_a.register(job_type, |job| {
            Box::pin(async move {
                let args = job.args().to_owned();
                let mut args = args.iter();
                let diffuculty_level = args
                    .next()
                    .expect("job difficulty level is there")
                    .to_owned();
                let sleep_secs =
                    serde_json::from_value::<i64>(diffuculty_level).expect("a valid number");
                time::sleep(time::Duration::from_secs(sleep_secs as u64)).await;
                eprintln!("{:?}", job);
                Ok::<(), io::Error>(())
            })
        });
        let mut consumer_a = consumer_a.connect(Some(&url1)).await.unwrap();
        let job = JobBuilder::new(job_type)
            .args(vec![difficulty_level])
            .queue(queue_name)
            .unique_for(unique_for)
            .unique_until_success() // Faktory's default
            .build();
        producer_a.enqueue(job).await.unwrap();
        let had_job = consumer_a.run_one(0, &[queue_name]).await.unwrap();
        assert!(had_job);
    });

    // let spawned thread gain momentum:
    time::sleep(time::Duration::from_secs(1)).await;

    // continue
    let mut producer_b = Producer::connect(Some(&url)).await.unwrap();

    // this one is a 'duplicate' because the job is still
    // being executed in the spawned thread:
    let job = JobBuilder::new(job_type)
        .args(vec![difficulty_level])
        .queue(queue_name)
        .unique_for(unique_for)
        .build();

    // as a result:
    let res = producer_b.enqueue(job).await.unwrap_err();
    if let error::Error::Protocol(error::Protocol::UniqueConstraintViolation { msg }) = res {
        assert_eq!(msg, "Job not unique");
    } else {
        panic!("Expected protocol error.")
    }

    handle.await.expect("should join successfully");

    // Now that the job submitted in a spawned thread has been successfully executed
    // (with ACK sent to server), the producer 'B' can push another one:
    producer_b
        .enqueue(
            JobBuilder::new(job_type)
                .args(vec![difficulty_level])
                .queue(queue_name)
                .unique_for(unique_for)
                .build(),
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn ent_unique_job_until_start() {
    use tokio::time;

    skip_if_not_enterprise!();

    let url = learn_faktory_url();

    let queue_name = "ent_unique_job_until_start";
    let job_type = "order";
    let difficulty_level = 3;
    let unique_for = 4;

    let url1 = url.clone();
    let handle = tokio::spawn(async move {
        let mut producer_a = Producer::connect(Some(&url1)).await.unwrap();
        let mut consumer_a = ConsumerBuilder::default_async();
        consumer_a.register(job_type, |job| {
            Box::pin(async move {
                let args = job.args().to_owned();
                let mut args = args.iter();
                let diffuculty_level = args
                    .next()
                    .expect("job difficulty level is there")
                    .to_owned();
                let sleep_secs =
                    serde_json::from_value::<i64>(diffuculty_level).expect("a valid number");
                time::sleep(time::Duration::from_secs(sleep_secs as u64)).await;
                eprintln!("{:?}", job);
                Ok::<(), io::Error>(())
            })
        });
        let mut consumer_a = consumer_a.connect(Some(&url1)).await.unwrap();
        producer_a
            .enqueue(
                JobBuilder::new(job_type)
                    .args(vec![difficulty_level])
                    .queue(queue_name)
                    .unique_for(unique_for)
                    .unique_until_start() // NB!
                    .build(),
            )
            .await
            .unwrap();
        // as soon as the job is fetched, the unique lock gets released
        let had_job = consumer_a.run_one(0, &[queue_name]).await.unwrap();
        assert!(had_job);
    });

    // let spawned thread gain momentum:
    time::sleep(time::Duration::from_secs(1)).await;

    // the unique lock has been released by this time, so the job is enqueued successfully:
    let mut producer_b = Producer::connect(Some(&url)).await.unwrap();
    producer_b
        .enqueue(
            JobBuilder::new(job_type)
                .args(vec![difficulty_level])
                .queue(queue_name)
                .unique_for(unique_for)
                .build(),
        )
        .await
        .unwrap();

    handle.await.expect("should join successfully");
}

#[tokio::test(flavor = "multi_thread")]
async fn ent_unique_job_bypass_unique_lock() {
    use faktory::error;

    skip_if_not_enterprise!();

    let url = learn_faktory_url();
    let mut producer = Producer::connect(Some(&url)).await.unwrap();
    let queue_name = "ent_unique_job_bypass_unique_lock";
    let job1 = Job::builder("order")
        .queue(queue_name)
        .unique_for(60)
        .build();

    // Now the following job is _technically_ a 'duplicate', BUT if the `unique_for` value is not set,
    // the uniqueness lock will be bypassed on the server. This special case is mentioned in the docs:
    // https://github.com/contribsys/faktory/wiki/Ent-Unique-Jobs#bypassing-uniqueness
    let job2 = Job::builder("order") // same jobtype and args (args are just not set)
        .queue(queue_name) // same queue
        .build(); // NB: `unique_for` not set

    producer.enqueue(job1).await.unwrap();
    producer.enqueue(job2).await.unwrap(); // bypassing the lock!

    // This _is_ a 'duplicate'.
    let job3 = Job::builder("order")
        .queue(queue_name)
        .unique_for(60) // NB
        .build();

    let res = producer.enqueue(job3).await.unwrap_err(); // NOT bypassing the lock!

    if let error::Error::Protocol(error::Protocol::UniqueConstraintViolation { msg }) = res {
        assert_eq!(msg, "Job not unique");
    } else {
        panic!("Expected protocol error.")
    }

    // let's consume three times from the queue to verify that the first two jobs
    // have been enqueued for real, while the last one has not.
    let mut c = ConsumerBuilder::default_async();
    c.register("order", |j| Box::pin(print_job(j)));
    let mut c = c.connect(Some(&url)).await.unwrap();

    assert!(c.run_one(0, &[queue_name]).await.unwrap());
    assert!(c.run_one(0, &[queue_name]).await.unwrap());
    assert!(!c.run_one(0, &[queue_name]).await.unwrap()); // empty;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_tracker_can_send_and_retrieve_job_execution_progress() {
    use std::{
        sync::{Arc, Mutex},
        thread, time,
    };

    skip_if_not_enterprise!();

    let url = learn_faktory_url();

    let t = Arc::new(Mutex::new(
        Client::connect(Some(&url))
            .await
            .expect("job progress tracker created successfully"),
    ));

    let mut p = Producer::connect(Some(&url)).await.unwrap();

    let job_tackable = JobBuilder::new("order")
        .args(vec![Value::from("ISBN-13:9781718501850")])
        .queue("test_tracker_can_send_progress_update")
        .build();

    let mut job_ordinary = JobBuilder::new("order")
        .args(vec![Value::from("ISBN-13:9781718501850")])
        .queue("test_tracker_can_send_progress_update")
        .build();
    // NB! Jobs are trackable by default, so we need to unset the "track" flag.
    assert_eq!(job_ordinary.custom.remove("track"), Some(Value::from(1)));

    // let's remember this job's id:
    let job_id = job_tackable.id().to_owned();

    p.enqueue(job_tackable).await.expect("enqueued");

    let mut c = ConsumerBuilder::default();

    {
        let job_id = job_id.clone();
        let url = url.clone();
        c.register("order", move |job| {
            let job_id = job_id.clone();
            let url = url.clone();
            Box::pin(async move {
                let mut t = Client::connect(Some(&url))
                    .await
                    .expect("job progress tracker created successfully");

                // trying to set progress on a community edition of Faktory will give:
                // 'an internal server error occurred: tracking subsystem is only available in Faktory Enterprise'
                assert!(t
                    .set_progress(
                        ProgressUpdate::builder(&job_id.clone())
                            .desc("Still processing...".to_owned())
                            .percent(32)
                            .build(),
                    )
                    .await
                    .is_ok());
                // Let's update the progress once again, to check the 'set_progress' shortcut:
                assert!(t
                    .set_progress(ProgressUpdate::set(&job_id.clone(), 33))
                    .await
                    .is_ok());

                // let's sleep for a while ...
                thread::sleep(time::Duration::from_secs(2));

                // ... and read the progress info
                let result = t
                    .get_progress(job_id.clone())
                    .await
                    .expect("Retrieved progress update over the wire");

                assert!(result.is_some());
                let result = result.unwrap();
                assert_eq!(result.jid, job_id.clone());
                match result.state {
                    JobState::Working => {}
                    _ => panic!("expected job's state to be 'working'"),
                }
                assert!(result.updated_at.is_some());
                assert_eq!(result.percent, Some(33));
                // considering the job done
                Ok::<(), io::Error>(eprintln!("{:?}", job))
            })
        });
    }
    let mut c = c
        .connect(Some(&url))
        .await
        .expect("Successfully ran a handshake with 'Faktory'");
    assert_had_one!(&mut c, "test_tracker_can_send_progress_update");

    let progress = t
        .lock()
        .expect("lock acquired successfully")
        .get_progress(job_id.clone())
        .await
        .expect("Retrieved progress update over the wire once again")
        .expect("Some progress");

    assert_eq!(progress.jid, job_id);
    // 'Faktory' will be keeping last known update for at least 30 minutes:
    assert_eq!(progress.percent, Some(33));

    // But it actually knows the job's real status, since the consumer (worker)
    // informed it immediately after finishing with the job:
    assert_eq!(progress.state, JobState::Success);

    // Let's update the status once again to verify the 'update_builder' method
    // on the `Progress` struct works as expected:
    let upd = progress
        .update_builder()
        .desc("Final stage.".to_string())
        .percent(99)
        .build();
    assert!(t.lock().unwrap().set_progress(upd).await.is_ok());

    let progress = t
        .lock()
        .unwrap()
        .get_progress(job_id)
        .await
        .expect("Retrieved progress update over the wire once again")
        .expect("Some progress");

    if progress.percent != Some(100) {
        let upd = progress.update_percent(100);
        assert_eq!(upd.desc, progress.desc);
        assert!(t.lock().unwrap().set_progress(upd).await.is_ok())
    }

    // What about 'ordinary' job ?
    let job_id = job_ordinary.id().to_owned().clone();

    // Sending it ...
    p.enqueue(job_ordinary)
        .await
        .expect("Successfuly send to Faktory");

    // ... and asking for its progress
    let progress = t
        .lock()
        .expect("lock acquired")
        .get_progress(job_id.clone())
        .await
        .expect("Retrieved progress update over the wire once again")
        .expect("Some progress");

    // From the docs:
    // There are several reasons why a job's state might be unknown:
    //    The JID is invalid or was never actually enqueued.
    //    The job was not tagged with the track variable in the job's custom attributes: custom:{"track":1}.
    //    The job's tracking structure has expired in Redis. It lives for 30 minutes and a big queue backlog can lead to expiration.
    assert_eq!(progress.jid, job_id);

    // Returned from Faktory: '{"jid":"f7APFzrS2RZi9eaA","state":"unknown","updated_at":""}'
    assert_eq!(progress.state, JobState::Unknown);
    assert!(progress.updated_at.is_none());
    assert!(progress.percent.is_none());
    assert!(progress.desc.is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_batch_of_jobs_can_be_initiated() {
    skip_if_not_enterprise!();
    let url = learn_faktory_url();

    let mut p = Producer::connect(Some(&url)).await.unwrap();
    let mut c = ConsumerBuilder::default();
    c.register("thumbnail", move |_job| {
        Box::pin(async move { Ok::<(), io::Error>(()) })
    });
    c.register("clean_up", move |_job| Box::pin(async move { Ok(()) }));
    let mut c = c.connect(Some(&url)).await.unwrap();
    let mut t = Client::connect(Some(&url))
        .await
        .expect("job progress tracker created successfully");

    let job_1 = Job::builder("thumbnail")
        .args(vec!["path/to/original/image1"])
        .queue("test_batch_of_jobs_can_be_initiated")
        .build();
    let job_2 = Job::builder("thumbnail")
        .args(vec!["path/to/original/image2"])
        .queue("test_batch_of_jobs_can_be_initiated")
        .build();
    let job_3 = Job::builder("thumbnail")
        .args(vec!["path/to/original/image3"])
        .queue("test_batch_of_jobs_can_be_initiated")
        .add_to_custom_data("bid", "check-check")
        .build();

    let cb_job = Job::builder("clean_up")
        .queue("test_batch_of_jobs_can_be_initiated__CALLBACKs")
        .build();

    let batch = Batch::builder()
        .description("Image resizing workload")
        .with_complete_callback(cb_job);

    let time_just_before_batch_init = Utc::now();

    let mut b = p.start_batch(batch).await.unwrap();

    // let's remember batch id:
    let bid = b.id().to_string();

    assert!(b.add(job_1).await.unwrap().is_none());
    assert!(b.add(job_2).await.unwrap().is_none());
    assert_eq!(b.add(job_3).await.unwrap().unwrap(), "check-check");
    b.commit().await.unwrap();

    // The batch has been committed, let's see its status:
    let time_just_before_getting_status = Utc::now();

    let s = t
        .get_batch_status(bid.clone())
        .await
        .expect("successfully fetched batch status from server...")
        .expect("...and it's not none");

    // Just to make a meaningfull assertion about the BatchStatus's 'created_at' field:
    assert!(s.created_at > time_just_before_batch_init);
    assert!(s.created_at < time_just_before_getting_status);
    assert_eq!(s.bid, bid);
    assert_eq!(s.description, Some("Image resizing workload".into()));
    assert_eq!(s.total, 3); // three jobs registered
    assert_eq!(s.pending, 3); // and none executed just yet
    assert_eq!(s.failed, 0);
    // Docs do not mention it, but the golang client does:
    // https://github.com/contribsys/faktory/blob/main/client/batch.go#L17-L19
    assert_eq!(s.success_callback_state, CallbackState::Pending); // we did not even provide the 'success' callback
    assert_eq!(s.complete_callback_state, CallbackState::Pending);

    // consume and execute job 1 ...
    assert_had_one!(&mut c, "test_batch_of_jobs_can_be_initiated");
    // ... and try consuming from the "callback" queue:
    assert_is_empty!(&mut c, "test_batch_of_jobs_can_be_initiated__CALLBACKs");

    // let's ask the Faktory server about the batch status after
    // we have consumed one job from this batch:
    let s = t
        .get_batch_status(bid.clone())
        .await
        .expect("successfully fetched batch status from server...")
        .expect("...and it's not none");

    // this is because we have just consumed and executed 1 of 3 jobs:
    assert_eq!(s.total, 3);
    assert_eq!(s.pending, 2);
    assert_eq!(s.failed, 0);

    // now, consume and execute job 2
    assert_had_one!(&mut c, "test_batch_of_jobs_can_be_initiated");
    // ... and check the callback queue again:
    assert_is_empty!(&mut c, "test_batch_of_jobs_can_be_initiated__CALLBACKs"); // not just yet ...

    // let's check batch status once again:
    let s = t
        .get_batch_status(bid.clone())
        .await
        .expect("successfully fetched batch status from server...")
        .expect("...and it's not none");

    // this is because we have just consumed and executed 2 of 3 jobs:
    assert_eq!(s.total, 3);
    assert_eq!(s.pending, 1);
    assert_eq!(s.failed, 0);

    // finally, consume and execute job 3 - the last one from the batch
    assert_had_one!(&mut c, "test_batch_of_jobs_can_be_initiated");

    // let's check batch status to see what happens after
    // all the jobs from the batch have been executed:
    let s = t
        .get_batch_status(bid.clone())
        .await
        .expect("successfully fetched batch status from server...")
        .expect("...and it's not none");

    // this is because we have just consumed and executed 2 of 3 jobs:
    assert_eq!(s.total, 3);
    assert_eq!(s.pending, 0);
    assert_eq!(s.failed, 0);
    assert_eq!(s.complete_callback_state, CallbackState::Enqueued);

    // let's now successfully consume from the "callback" queue:
    assert_had_one!(&mut c, "test_batch_of_jobs_can_be_initiated__CALLBACKs");

    // let's check batch status one last time:
    let s = t
        .get_batch_status(bid.clone())
        .await
        .expect("successfully fetched batch status from server...")
        .expect("...and it's not none");

    // this is because we have just consumed and executed 2 of 3 jobs:
    assert_eq!(s.complete_callback_state, CallbackState::FinishedOk);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_batches_can_be_nested() {
    skip_if_not_enterprise!();
    let url = learn_faktory_url();

    // Set up 'producer', 'consumer', and 'tracker':
    let mut p = Producer::connect(Some(&url)).await.unwrap();
    let mut c = ConsumerBuilder::default();
    c.register("jobtype", move |_job| {
        Box::pin(async move { Ok::<(), io::Error>(()) })
    });
    let mut _c = c.connect(Some(&url)).await.unwrap();
    let mut t = Client::connect(Some(&url))
        .await
        .expect("job progress tracker created successfully");

    // Prepare some jobs:
    let parent_job1 = Job::builder("jobtype")
        .queue("test_batches_can_be_nested")
        .build();
    let child_job_1 = Job::builder("jobtype")
        .queue("test_batches_can_be_nested")
        .build();
    let child_job_2 = Job::builder("jobtype")
        .queue("test_batches_can_be_nested")
        .build();
    let grand_child_job_1 = Job::builder("jobtype")
        .queue("test_batches_can_be_nested")
        .build();

    // Sccording to Faktory docs:
    // "The callback for a parent batch will not enqueue until the callback for the child batch has finished."
    // See: https://github.com/contribsys/faktory/wiki/Ent-Batches#guarantees
    let parent_cb_job = Job::builder("clean_up")
        .queue("test_batches_can_be_nested__CALLBACKs")
        .build();
    let child_cb_job = Job::builder("clean_up")
        .queue("test_batches_can_be_nested__CALLBACKs")
        .build();
    let grandchild_cb_job = Job::builder("clean_up")
        .queue("test_batches_can_be_nested__CALLBACKs")
        .build();

    // batches start
    let parent_batch = Batch::builder()
        .description("Parent batch")
        .with_success_callback(parent_cb_job);
    let mut parent_batch = p.start_batch(parent_batch).await.unwrap();
    let parent_batch_id = parent_batch.id().to_owned();
    parent_batch.add(parent_job1).await.unwrap();

    let child_batch = Batch::builder()
        .description("Child batch")
        .with_success_callback(child_cb_job);
    let mut child_batch = parent_batch.start_batch(child_batch).await.unwrap();
    let child_batch_id = child_batch.id().to_owned();
    child_batch.add(child_job_1).await.unwrap();
    child_batch.add(child_job_2).await.unwrap();

    let grandchild_batch = Batch::builder()
        .description("Grandchild batch")
        .with_success_callback(grandchild_cb_job);
    let mut grandchild_batch = child_batch.start_batch(grandchild_batch).await.unwrap();
    let grandchild_batch_id = grandchild_batch.id().to_owned();
    grandchild_batch.add(grand_child_job_1).await.unwrap();

    grandchild_batch.commit().await.unwrap();
    child_batch.commit().await.unwrap();
    parent_batch.commit().await.unwrap();
    // batches finish

    let parent_status = t
        .get_batch_status(parent_batch_id.clone())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(parent_status.description, Some("Parent batch".to_string()));
    assert_eq!(parent_status.total, 1);
    assert_eq!(parent_status.parent_bid, None);

    let child_status = t
        .get_batch_status(child_batch_id.clone())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(child_status.description, Some("Child batch".to_string()));
    assert_eq!(child_status.total, 2);
    assert_eq!(child_status.parent_bid, Some(parent_batch_id));

    let grandchild_status = t
        .get_batch_status(grandchild_batch_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        grandchild_status.description,
        Some("Grandchild batch".to_string())
    );
    assert_eq!(grandchild_status.total, 1);
    assert_eq!(grandchild_status.parent_bid, Some(child_batch_id));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_callback_will_not_be_queued_unless_batch_gets_committed() {
    skip_if_not_enterprise!();
    let url = learn_faktory_url();

    // prepare a producer, a consumer of 'order' jobs, and a tracker:
    let mut p = Producer::connect(Some(&url)).await.unwrap();
    let mut c = ConsumerBuilder::default();
    c.register("order", move |_job| Box::pin(async move { Ok(()) }));
    c.register("order_clean_up", move |_job| {
        Box::pin(async move { Ok::<(), io::Error>(()) })
    });
    let mut c = c.connect(Some(&url)).await.unwrap();
    let mut t = Client::connect(Some(&url)).await.unwrap();

    let mut jobs = some_jobs(
        "order",
        "test_callback_will_not_be_queued_unless_batch_gets_committed",
        3,
    );
    let mut callbacks = some_jobs(
        "order_clean_up",
        "test_callback_will_not_be_queued_unless_batch_gets_committed__CALLBACKs",
        1,
    );

    // start a 'batch':
    let mut b = p
        .start_batch(
            Batch::builder()
                .description("Orders processing workload")
                .with_success_callback(callbacks.next().unwrap()),
        )
        .await
        .unwrap();
    let bid = b.id().to_string();

    // push 3 jobs onto this batch, but DO NOT commit the batch:
    for _ in 0..3 {
        b.add(jobs.next().unwrap()).await.unwrap();
    }

    // check this batch's status:
    let s = t.get_batch_status(bid.clone()).await.unwrap().unwrap();
    assert_eq!(s.total, 3);
    assert_eq!(s.pending, 3);
    assert_eq!(s.success_callback_state, CallbackState::Pending);

    // consume those 3 jobs successfully;
    for _ in 0..3 {
        assert_had_one!(
            &mut c,
            "test_callback_will_not_be_queued_unless_batch_gets_committed"
        );
    }

    // verify the queue is drained:
    assert_is_empty!(
        &mut c,
        "test_callback_will_not_be_queued_unless_batch_gets_committed"
    );

    // check this batch's status again:
    let s = t.get_batch_status(bid.clone()).await.unwrap().unwrap();
    assert_eq!(s.total, 3);
    assert_eq!(s.pending, 0);
    assert_eq!(s.failed, 0);
    assert_eq!(s.success_callback_state, CallbackState::Pending); // not just yet;

    // to double-check, let's assert the success callbacks queue is empty:
    assert_is_empty!(
        &mut c,
        "test_callback_will_not_be_queued_unless_batch_gets_committed__CALLBACKs"
    );

    // now let's COMMIT the batch ...
    b.commit().await.unwrap();

    // ... and check batch status:
    let s = t.get_batch_status(bid.clone()).await.unwrap().unwrap();
    assert_eq!(s.success_callback_state, CallbackState::Enqueued);

    // finally, let's consume from the success callbacks queue ...
    assert_had_one!(
        &mut c,
        "test_callback_will_not_be_queued_unless_batch_gets_committed__CALLBACKs"
    );

    // ... and see the final status:
    let s = t.get_batch_status(bid.clone()).await.unwrap().unwrap();
    assert_eq!(s.success_callback_state, CallbackState::FinishedOk);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_callback_will_be_queued_upon_commit_even_if_batch_is_empty() {
    use std::{thread, time};

    skip_if_not_enterprise!();
    let url = learn_faktory_url();
    let mut p = Producer::connect(Some(&url)).await.unwrap();
    let mut t = Client::connect(Some(&url)).await.unwrap();
    let q_name = "test_callback_will_be_queued_upon_commit_even_if_batch_is_empty";
    let complete_cb_jobtype = "complete_callback_jobtype";
    let success_cb_jobtype = "success_cb_jobtype";
    let complete_cb = some_jobs(complete_cb_jobtype, q_name, 1).next().unwrap();
    let success_cb = some_jobs(success_cb_jobtype, q_name, 1).next().unwrap();
    let b = p
        .start_batch(
            Batch::builder()
                .description("Orders processing workload")
                .with_callbacks(success_cb, complete_cb),
        )
        .await
        .unwrap();
    let bid = b.id().to_owned();

    let s = t.get_batch_status(bid.clone()).await.unwrap().unwrap();
    assert_eq!(s.total, 0); // no jobs in the batch;
    assert_eq!(s.success_callback_state, CallbackState::Pending);
    assert_eq!(s.complete_callback_state, CallbackState::Pending);

    b.commit().await.unwrap();

    // let's give the Faktory server some time:
    thread::sleep(time::Duration::from_secs(2));

    let s = t.get_batch_status(bid.clone()).await.unwrap().unwrap();
    assert_eq!(s.total, 0); // again, there are no jobs in the batch ...

    // The docs say "If you don't push any jobs into the batch, any callbacks will fire immediately upon BATCH COMMIT."
    // and "the success callback for a batch will always enqueue after the complete callback"
    assert_eq!(s.complete_callback_state, CallbackState::Enqueued);
    assert_eq!(s.success_callback_state, CallbackState::Pending);

    let mut c = ConsumerBuilder::default();
    c.register(complete_cb_jobtype, |_job| Box::pin(async { Ok(()) }));
    c.register(success_cb_jobtype, |_job| {
        Box::pin(async {
            Err(io::Error::new(
                io::ErrorKind::Other,
                "we want this one to fail to test the 'CallbackState' behavior",
            ))
        })
    });

    let mut c = c.connect(Some(&url)).await.unwrap();

    assert_had_one!(&mut c, q_name); // complete callback consumed

    let s = t.get_batch_status(bid.clone()).await.unwrap().unwrap();
    assert_eq!(s.total, 0);
    match s.complete_callback_state {
        CallbackState::FinishedOk => {}
        _ => panic!("Expected the callback to have been successfully executed"),
    }
    match s.success_callback_state {
        CallbackState::Enqueued => {}
        _ => panic!("Expected the callback to have been enqueued, since the `complete` callback has already executed"),
    }
    assert_had_one!(&mut c, q_name); // success callback consumed

    let s = t.get_batch_status(bid.clone()).await.unwrap().unwrap();
    assert_eq!(s.total, 0);
    assert_eq!(s.complete_callback_state, CallbackState::FinishedOk);
    // Still `Enqueued` due to the fact that it was not finished with success.
    // Had we registered a handler for `success_cb_jobtype` returing Ok(()) rather then Err(),
    // the state would be `FinishedOk` just like it's the case with the `complete` callback.
    assert_eq!(s.success_callback_state, CallbackState::Enqueued);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_batch_can_be_reopened_add_extra_jobs_and_batches_added() {
    skip_if_not_enterprise!();
    let url = learn_faktory_url();
    let mut p = Producer::connect(Some(&url)).await.unwrap();
    let mut t = Client::connect(Some(&url)).await.unwrap();
    let mut jobs = some_jobs("order", "test_batch_can_be_reopned_add_extra_jobs_added", 4);
    let mut callbacks = some_jobs(
        "order_clean_up",
        "test_batch_can_be_reopned_add_extra_jobs_added__CALLBACKs",
        1,
    );

    let b = Batch::builder()
        .description("Orders processing workload")
        .with_success_callback(callbacks.next().unwrap());

    let mut b = p.start_batch(b).await.unwrap();
    let bid = b.id().to_string();
    b.add(jobs.next().unwrap()).await.unwrap(); // 1 job
    b.add(jobs.next().unwrap()).await.unwrap(); // 2 jobs

    let status = t.get_batch_status(bid.clone()).await.unwrap().unwrap();
    assert_eq!(status.total, 2);
    assert_eq!(status.pending, 2);

    // ############################## SUBTEST 0 ##########################################
    // Let's try to open/reopen a batch we have never declared:
    let b = p
        .open_batch(String::from("non-existent-batch-id"))
        .await
        .unwrap();
    // The server will error back on this, with "No such batch <provided batch id>", but
    // we are handling this case for the end-user and returning `Ok(None)` instead, indicating
    // this way that there is not such batch.
    assert!(b.is_none());
    // ########################## END OF SUBTEST 0 #######################################

    // ############################## SUBTEST 1 ##########################################
    // Let's fist of all try to open the batch we have not committed yet:
    // [We can use `producer::open_batch` specifying a bid OR - even we previously retrived
    // a status for this batch, we can go with `status::open` providing an exclusive ref to producer]
    let mut b = status.open(&mut p).await.unwrap().unwrap();
    b.add(jobs.next().unwrap()).await.unwrap(); // 3 jobs

    b.commit().await.unwrap(); // committig the batch

    let status = t.get_batch_status(bid.clone()).await.unwrap().unwrap();
    assert_eq!(status.total, 3);
    assert_eq!(status.pending, 3);

    // Subtest 1 result:
    // The Faktory server let's us open the uncommitted batch. This is something not mention
    // in the docs, but still worth checking.
    // ########################### END OF SUBTEST 1 ######################################

    // ############################## SUBTEST 2 ##########################################
    // From the docs:
    // """Note that, once committed, only a job within the batch may reopen it.
    // Faktory will return an error if you dynamically add jobs from "outside" the batch;
    // this is to prevent a race condition between callbacks firing and an outsider adding more jobs."""
    // Ref: https://github.com/contribsys/faktory/wiki/Ent-Batches#batch-open-bid (Jan 10, 2024)

    // Let's try to open an already committed batch:
    let mut b = p
        .open_batch(bid.clone())
        .await
        .expect("no error")
        .expect("is some");
    b.add(jobs.next().unwrap()).await.unwrap(); // 4 jobs
    b.commit().await.unwrap(); // committing the batch again!

    let s = t.get_batch_status(bid.clone()).await.unwrap().unwrap();
    assert_eq!(s.total, 4);
    assert_eq!(s.pending, 4);

    // Subtest 2 result:
    // We managed to open a batch "from outside" and the server accepted the job INSTEAD OF ERRORING BACK.
    // ############################ END OF SUBTEST 2 #######################################

    // ############################## SUBTEST 3 ############################################
    // Let's see if we will be able to - again - open the committed batch "from outside" and
    // add a nested batch to it.
    let mut b = p.open_batch(bid.clone()).await.unwrap().expect("is some");
    let mut nested_callbacks = some_jobs(
        "order_clean_up__NESTED",
        "test_batch_can_be_reopned_add_extra_jobs_added__CALLBACKs__NESTED",
        2,
    );
    let nested_batch_declaration = Batch::builder()
        .description("Orders processing workload. Nested stage")
        .with_callbacks(
            nested_callbacks.next().unwrap(),
            nested_callbacks.next().unwrap(),
        );
    let nested_batch = b.start_batch(nested_batch_declaration).await.unwrap();
    let nested_bid = nested_batch.id().to_string();
    // committing the nested batch without any jobs
    // since those are just not relevant for this test:
    nested_batch.commit().await.unwrap();

    let s = t
        .get_batch_status(nested_bid.clone())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(s.total, 0);
    assert_eq!(s.parent_bid, Some(bid)); // this is really our child batch
    assert_eq!(s.complete_callback_state, CallbackState::Enqueued);

    // Subtest 3 result:
    // We managed to open an already committed batch "from outside" and the server accepted
    // a nested batch INSTEAD OF ERRORING BACK.
    // ############################ END OF SUBTEST 3 #######################################

    // The following subtest assertions should be adjusted once fixes are introduced to
    // the Faktory as per https://github.com/contribsys/faktory/issues/465
    // The idea is we should not be able to push to a batch for which the server have already
    // enqeued a callback.
    //
    // ############################## SUBTEST 4 ############################################
    // From the docs:
    // """Once a callback has enqueued for a batch, you may not add anything to the batch."""
    // ref: https://github.com/contribsys/faktory/wiki/Ent-Batches#guarantees (Jan 10, 2024)

    // Let's try to re-open the nested batch that we have already committed and add some jobs to it.
    let mut b = p
        .open_batch(nested_bid.clone())
        .await
        .expect("no error")
        .expect("is some");
    let mut more_jobs = some_jobs(
        "order_clean_up__NESTED",
        "test_batch_can_be_reopned_add_extra_jobs_added__NESTED",
        2,
    );
    b.add(more_jobs.next().unwrap()).await.unwrap();
    b.add(more_jobs.next().unwrap()).await.unwrap();
    b.commit().await.unwrap();

    let s = t
        .get_batch_status(nested_bid.clone())
        .await
        .unwrap()
        .unwrap();
    match s.complete_callback_state {
        CallbackState::Enqueued => {}
        _ => panic!("Expected the callback to have been enqueued"),
    }
    assert_eq!(s.pending, 2); // ... though there are pending jobs
    assert_eq!(s.total, 2);

    // Subtest 4 result:
    // We were able to add more jobs to the batch for which the Faktory server had already
    // queued the callback.
    // ############################## END OF SUBTEST 4 #####################################

    // ############################## OVERALL RESULTS ######################################
    // The guarantees that definitely hold:
    //
    // 1) the callbacks will fire immediately after the jobs of this batch have been executed, provided the batch has been committed;
    //
    // 2) the callbacks will fire immediately for an empty batch, provided it has been committed;
    //
    // 3) the 'complete' callback will always be queued first
    // (this is shown as part the test 'test_callback_will_be_queue_upon_commit_even_if_batch_is_empty');
}
