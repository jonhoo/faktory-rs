extern crate faktory;

use crate::skip_if_not_enterprise;
use crate::utils::learn_faktory_url;
use faktory::{ConsumerBuilder, Job, JobBuilder, Producer};
use std::io;
use tokio::time;

async fn print_job(j: Job) -> io::Result<()> {
    Ok(eprintln!("{:?}", j))
}

#[tokio::test(flavor = "multi_thread")]
async fn ent_expiring_job() {
    skip_if_not_enterprise!();

    let url = learn_faktory_url();
    let local = "ent_expiring_job";

    // prepare a producer ("client" in Faktory terms) and consumer ("worker"):
    let mut producer = Producer::connect(Some(&url)).await.unwrap();
    let mut consumer = ConsumerBuilder::default();
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

#[tokio::test(flavor = "multi_thread")]
async fn ent_unique_job() {
    use faktory::error;
    use serde_json::Value;

    skip_if_not_enterprise!();

    let url = learn_faktory_url();

    let job_type = "order";

    // prepare producer and consumer:
    let mut producer = Producer::connect(Some(&url)).await.unwrap();
    let mut consumer = ConsumerBuilder::default();
    consumer.register(job_type, |j| Box::pin(print_job(j)));
    let mut consumer = consumer.connect(Some(&url)).await.unwrap();

    // Reminder. Jobs are considered unique for kind + args + queue.
    // So the following two jobs, will be accepted by Faktory, since we
    // are not setting 'unique_for' when creating those jobs:
    let queue_name = "ent_unique_job";
    let args = vec![Value::from("ISBN-13:9781718501850"), Value::from(100)];
    let job1 = JobBuilder::new(job_type)
        .args(args.clone())
        .queue(queue_name)
        .build();
    producer.enqueue(job1).await.unwrap();
    let job2 = JobBuilder::new(job_type)
        .args(args.clone())
        .queue(queue_name)
        .build();
    producer.enqueue(job2).await.unwrap();

    let had_job = consumer.run_one(0, &[queue_name]).await.unwrap();
    assert!(had_job);
    let had_another_one = consumer.run_one(0, &[queue_name]).await.unwrap();
    assert!(had_another_one);
    let and_that_is_it_for_now = !consumer.run_one(0, &[queue_name]).await.unwrap();
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
    producer.enqueue(job1).await.unwrap();
    // this one is a 'duplicate' ...
    let job2 = Job::builder(job_type)
        .args(args.clone())
        .queue(queue_name)
        .unique_for(unique_for_secs)
        .build();
    // ... so the server will respond accordingly:
    let res = producer.enqueue(job2).await.unwrap_err();
    if let error::Error::Protocol(error::Protocol::UniqueConstraintViolation { msg }) = res {
        assert_eq!(msg, "Job not unique");
    } else {
        panic!("Expected protocol error.")
    }

    // Let's now consume the job which is 'holding' a unique lock:
    let had_job = consumer.run_one(0, &[queue_name]).await.unwrap();
    assert!(had_job);

    // And check that the queue is really empty (`job2` from above
    // has not been queued indeed):
    let queue_is_empty = !consumer.run_one(0, &[queue_name]).await.unwrap();
    assert!(queue_is_empty);

    // Now let's repeat the latter case, but providing different args to job2:
    let job1 = JobBuilder::new(job_type)
        .args(args.clone())
        .queue(queue_name)
        .unique_for(unique_for_secs)
        .build();
    producer.enqueue(job1).await.unwrap();
    // this one is *NOT* a 'duplicate' ...
    let job2 = JobBuilder::new(job_type)
        .args(vec![Value::from("ISBN-13:9781718501850"), Value::from(101)])
        .queue(queue_name)
        .unique_for(unique_for_secs)
        .build();
    // ... so the server will accept it:
    producer.enqueue(job2).await.unwrap();

    let had_job = consumer.run_one(0, &[queue_name]).await.unwrap();
    assert!(had_job);
    let had_another_one = consumer.run_one(0, &[queue_name]).await.unwrap();
    assert!(had_another_one);

    // and the queue is empty again:
    let had_job = consumer.run_one(0, &[queue_name]).await.unwrap();
    assert!(!had_job);
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
