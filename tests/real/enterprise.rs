extern crate faktory;
extern crate serde_json;
extern crate url;

use faktory::*;
use std::io;

macro_rules! skip_if_not_enterprise {
    () => {
        if std::env::var_os("FAKTORY_ENT").is_none() {
            return;
        }
    };
}

fn learn_faktory_url() -> String {
    let url = std::env::var_os("FAKTORY_URL").expect(
        "Enterprise Faktory should be running for this test, and 'FAKTORY_URL' environment variable should be provided",
    );
    url.to_str().expect("Is a utf-8 string").to_owned()
}

#[test]
fn ent_expiring_job() {
    use std::{thread, time};

    skip_if_not_enterprise!();

    let url = learn_faktory_url();

    // prepare a producer ("client" in Faktory terms) and consumer ("worker"):
    let mut producer = Producer::connect(Some(&url)).unwrap();
    let mut consumer = ConsumerBuilder::default();
    consumer.register("AnExpiringJob", move |job| -> io::Result<_> {
        Ok(eprintln!("{:?}", job))
    });
    let mut consumer = consumer.connect(Some(&url)).unwrap();

    // prepare an expiring job:
    let job_ttl_secs: u64 = 3;

    let ttl = chrono::Duration::seconds(job_ttl_secs as i64);
    let job1 = JobBuilder::new("AnExpiringJob")
        .args(vec!["ISBN-13:9781718501850"])
        .expires_at(chrono::Utc::now() + ttl)
        .build();

    // enqueue and fetch immediately job1:
    producer.enqueue(job1).unwrap();
    let had_job = consumer.run_one(0, &["default"]).unwrap();
    assert!(had_job);

    // check that the queue is drained:
    let had_job = consumer.run_one(0, &["default"]).unwrap();
    assert!(!had_job);

    // prepare another one:
    let job2 = JobBuilder::new("AnExpiringJob")
        .args(vec!["ISBN-13:9781718501850"])
        .expires_at(chrono::Utc::now() + ttl)
        .build();

    // enquere and then fetch job2, but after ttl:
    producer.enqueue(job2).unwrap();
    thread::sleep(time::Duration::from_secs(job_ttl_secs * 2));
    let had_job = consumer.run_one(0, &["default"]).unwrap();

    // For the non-enterprise edition of Faktory, this assertion will
    // fail, which should be take into account when running the test suite on CI.
    assert!(!had_job);
}

#[test]
fn ent_unique_job() {
    use faktory::error;
    use serde_json::Value;

    skip_if_not_enterprise!();

    let url = learn_faktory_url();

    let job_type = "order";

    // prepare producer and consumer:
    let mut producer = Producer::connect(Some(&url)).unwrap();
    let mut consumer = ConsumerBuilder::default();
    consumer.register(job_type, |job| -> io::Result<_> {
        Ok(eprintln!("{:?}", job))
    });
    let mut consumer = consumer.connect(Some(&url)).unwrap();

    // Reminder. Jobs are considered unique for kind + args + queue.
    // So the following two jobs, will be accepted by Faktory, since we
    // are not setting 'unique_for' when creating those jobs:
    let queue_name = "ent_unique_job";
    let args = vec![Value::from("ISBN-13:9781718501850"), Value::from(100)];
    let job1 = JobBuilder::new(job_type)
        .args(args.clone())
        .queue(queue_name)
        .build();
    producer.enqueue(job1).unwrap();
    let job2 = JobBuilder::new(job_type)
        .args(args.clone())
        .queue(queue_name)
        .build();
    producer.enqueue(job2).unwrap();

    let had_job = consumer.run_one(0, &[queue_name]).unwrap();
    assert!(had_job);
    let had_another_one = consumer.run_one(0, &[queue_name]).unwrap();
    assert!(had_another_one);
    let and_that_is_it_for_now = !consumer.run_one(0, &[queue_name]).unwrap();
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
    producer.enqueue(job1).unwrap();
    // this one is a 'duplicate' ...
    let job2 = Job::builder(job_type)
        .args(args.clone())
        .queue(queue_name)
        .unique_for(unique_for_secs)
        .build();
    // ... so the server will respond accordingly:
    let res = producer.enqueue(job2).unwrap_err();
    if let error::Error::Protocol(error::Protocol::Internal { msg }) = res {
        assert_eq!(msg, "NOTUNIQUE Job not unique");
    } else {
        panic!("Expected protocol error.")
    }

    let had_job = consumer.run_one(0, &[queue_name]).unwrap();
    assert!(had_job);
    let had_another_one = consumer.run_one(0, &[queue_name]).unwrap();

    // For the non-enterprise edition of Faktory, this assertion WILL FAIL:
    assert!(!had_another_one);

    // Now let's repeat the latter case, but providing different args to job2:
    let job1 = JobBuilder::new(job_type)
        .args(args.clone())
        .queue(queue_name)
        .unique_for(unique_for_secs)
        .build();
    producer.enqueue(job1).unwrap();
    // this one is *NOT* a 'duplicate' ...
    let job2 = JobBuilder::new(job_type)
        .args(vec![Value::from("ISBN-13:9781718501850"), Value::from(101)])
        .queue(queue_name)
        .unique_for(unique_for_secs)
        .build();
    // ... so the server will accept it:
    producer.enqueue(job2).unwrap();

    let had_job = consumer.run_one(0, &[queue_name]).unwrap();
    assert!(had_job);
    let had_another_one = consumer.run_one(0, &[queue_name]).unwrap();
    assert!(had_another_one);

    // and the queue is empty again:
    let had_job = consumer.run_one(0, &[queue_name]).unwrap();
    assert!(!had_job);
}

#[test]
fn ent_unique_job_until_success() {
    use faktory::error;
    use std::thread;
    use std::time;

    skip_if_not_enterprise!();

    let url = learn_faktory_url();

    let queue_name = "ent_unique_job_until_success";
    let job_type = "order";

    // the job will be being executed for at least 3 seconds,
    // but is unique for 4 seconds;
    let difficulty_level = 3;
    let unique_for = 4;

    let url1 = url.clone();
    let handle = thread::spawn(move || {
        // prepare producer and consumer, where the former can
        // send a job difficulty level as a job's args and the lattter
        // will sleep for a corresponding period of time, pretending
        // to work hard:
        let mut producer_a = Producer::connect(Some(&url1)).unwrap();
        let mut consumer_a = ConsumerBuilder::default();
        consumer_a.register(job_type, |job| -> io::Result<_> {
            let args = job.args().to_owned();
            let mut args = args.iter();
            let diffuculty_level = args
                .next()
                .expect("job difficulty level is there")
                .to_owned();
            let sleep_secs =
                serde_json::from_value::<i64>(diffuculty_level).expect("a valid number");
            thread::sleep(time::Duration::from_secs(sleep_secs as u64));
            Ok(eprintln!("{:?}", job))
        });
        let mut consumer_a = consumer_a.connect(Some(&url1)).unwrap();
        let job = JobBuilder::new(job_type)
            .args(vec![difficulty_level])
            .queue(queue_name)
            .unique_for(unique_for)
            .unique_until_success() // Faktory's default
            .build();
        producer_a.enqueue(job).unwrap();
        let had_job = consumer_a.run_one(0, &[queue_name]).unwrap();
        assert!(had_job);
    });

    // let spawned thread gain momentum:
    thread::sleep(time::Duration::from_secs(1));

    // continue
    let mut producer_b = Producer::connect(Some(&url)).unwrap();

    // this one is a 'duplicate' because the job is still
    // being executed in the spawned thread:
    let job = JobBuilder::new(job_type)
        .args(vec![difficulty_level])
        .queue(queue_name)
        .unique_for(unique_for)
        .build();

    // as a result:
    let res = producer_b.enqueue(job).unwrap_err();
    if let error::Error::Protocol(error::Protocol::Internal { msg }) = res {
        assert_eq!(msg, "NOTUNIQUE Job not unique");
    } else {
        panic!("Expected protocol error.")
    }

    handle.join().expect("should join successfully");

    // Not that the job submitted in a spawned thread has been successfully executed
    // (with ACK sent to server), the producer 'B' can push another one:
    assert!(producer_b
        .enqueue(
            JobBuilder::new(job_type)
                .args(vec![difficulty_level])
                .queue(queue_name)
                .unique_for(unique_for)
                .build()
        )
        .is_ok());
}

#[test]
fn ent_unique_job_until_start() {
    use std::thread;
    use std::time;

    skip_if_not_enterprise!();

    let url = learn_faktory_url();

    let queue_name = "ent_unique_job_until_start";
    let job_type = "order";
    let difficulty_level = 3;
    let unique_for = 4;

    let url1 = url.clone();
    let handle = thread::spawn(move || {
        let mut producer_a = Producer::connect(Some(&url1)).unwrap();
        let mut consumer_a = ConsumerBuilder::default();
        consumer_a.register(job_type, |job| -> io::Result<_> {
            let args = job.args().to_owned();
            let mut args = args.iter();
            let diffuculty_level = args
                .next()
                .expect("job difficulty level is there")
                .to_owned();
            let sleep_secs =
                serde_json::from_value::<i64>(diffuculty_level).expect("a valid number");
            thread::sleep(time::Duration::from_secs(sleep_secs as u64));
            Ok(eprintln!("{:?}", job))
        });
        let mut consumer_a = consumer_a.connect(Some(&url1)).unwrap();
        producer_a
            .enqueue(
                JobBuilder::new(job_type)
                    .args(vec![difficulty_level])
                    .queue(queue_name)
                    .unique_for(unique_for)
                    .unique_until_start() // NB!
                    .build(),
            )
            .unwrap();
        // as soon as the job is fetched, the unique lock gets released
        let had_job = consumer_a.run_one(0, &[queue_name]).unwrap();
        assert!(had_job);
    });

    // let spawned thread gain momentum:
    thread::sleep(time::Duration::from_secs(1));

    // the unique lock has been released by this time, so the job is enqueued successfully:
    let mut producer_b = Producer::connect(Some(&url)).unwrap();
    assert!(producer_b
        .enqueue(
            JobBuilder::new(job_type)
                .args(vec![difficulty_level])
                .queue(queue_name)
                .unique_for(unique_for)
                .build()
        )
        .is_ok());

    handle.join().expect("should join successfully");
}
