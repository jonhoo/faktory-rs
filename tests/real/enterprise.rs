extern crate faktory;
extern crate serde_json;
extern crate url;

use chrono::Utc;
use faktory::*;
use serde_json::Value;
use std::io;

macro_rules! skip_if_not_enterprise {
    () => {
        if std::env::var_os("FAKTORY_ENT").is_none() {
            return;
        }
    };
}

macro_rules! assert_had_one {
    ($c:expr, $q:expr) => {
        let had_one_job = $c.run_one(0, &[$q]).unwrap();
        assert!(had_one_job);
    };
}

macro_rules! assert_is_empty {
    ($c:expr, $q:expr) => {
        let had_one_job = $c.run_one(0, &[$q]).unwrap();
        assert!(!had_one_job);
    };
}

fn learn_faktory_url() -> String {
    let url = std::env::var_os("FAKTORY_URL").expect(
        "Enterprise Faktory should be running for this test, and 'FAKTORY_URL' environment variable should be provided",
    );
    url.to_str().expect("Is a utf-8 string").to_owned()
}

fn some_jobs<S>(kind: S, q: S, count: usize) -> impl Iterator<Item = Job>
where
    S: Into<String> + Clone + 'static,
{
    (0..count)
        .into_iter()
        .map(move |_| Job::builder(kind.clone()).queue(q.clone()).build())
}

#[test]
fn ent_expiring_job() {
    use std::{thread, time};

    skip_if_not_enterprise!();

    let url = learn_faktory_url();

    // prepare a producer ("client" in Faktory terms) and consumer ("worker"):
    let mut p = Producer::connect(Some(&url)).unwrap();
    let mut c = ConsumerBuilder::default();
    c.register("AnExpiringJob", move |job| -> io::Result<_> {
        Ok(eprintln!("{:?}", job))
    });
    let mut c = c.connect(Some(&url)).unwrap();

    // prepare an expiring job:
    let job_ttl_secs: u64 = 3;

    let ttl = chrono::Duration::seconds(job_ttl_secs as i64);
    let job1 = JobBuilder::new("AnExpiringJob")
        .args(vec!["ISBN-13:9781718501850"])
        .queue("ent_expiring_job")
        .expires_at(chrono::Utc::now() + ttl)
        .build();

    // enqueue and fetch immediately job1:
    p.enqueue(job1).unwrap();
    assert_had_one!(&mut c, "ent_expiring_job");

    // check that the queue is drained:
    assert_is_empty!(&mut c, "ent_expiring_job");

    // prepare another one:
    let job2 = JobBuilder::new("AnExpiringJob")
        .args(vec!["ISBN-13:9781718501850"])
        .queue("ent_expiring_job")
        .expires_at(chrono::Utc::now() + ttl)
        .build();

    // enqueue and then fetch job2, but after ttl:
    p.enqueue(job2).unwrap();
    thread::sleep(time::Duration::from_secs(job_ttl_secs * 2));

    // For the non-enterprise edition of Faktory, this assertion will
    // fail, which should be taken into account when running the test suite on CI.
    assert_is_empty!(&mut c, "ent_expiring_job");
}

#[test]
fn ent_unique_job() {
    use faktory::error;
    use serde_json::Value;

    skip_if_not_enterprise!();

    let url = learn_faktory_url();

    let job_type = "order";

    // prepare producer and consumer:
    let mut p = Producer::connect(Some(&url)).unwrap();
    let mut c = ConsumerBuilder::default();
    c.register(job_type, |job| -> io::Result<_> {
        Ok(eprintln!("{:?}", job))
    });
    let mut c = c.connect(Some(&url)).unwrap();

    // Reminder. Jobs are considered unique for kind + args + queue.
    // So the following two jobs, will be accepted by Faktory, since we
    // are not setting 'unique_for' when creating those jobs:
    let queue_name = "ent_unique_job";
    let args = vec![Value::from("ISBN-13:9781718501850"), Value::from(100)];
    let job1 = JobBuilder::new(job_type)
        .args(args.clone())
        .queue(queue_name)
        .build();
    p.enqueue(job1).unwrap();
    let job2 = JobBuilder::new(job_type)
        .args(args.clone())
        .queue(queue_name)
        .build();
    p.enqueue(job2).unwrap();

    let had_job = c.run_one(0, &[queue_name]).unwrap();
    assert!(had_job);
    let had_another_one = c.run_one(0, &[queue_name]).unwrap();
    assert!(had_another_one);
    let and_that_is_it_for_now = !c.run_one(0, &[queue_name]).unwrap();
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
    p.enqueue(job1).unwrap();
    // this one is a 'duplicate' ...
    let job2 = Job::builder(job_type)
        .args(args.clone())
        .queue(queue_name)
        .unique_for(unique_for_secs)
        .build();
    // ... so the server will respond accordingly:
    let res = p.enqueue(job2).unwrap_err();
    if let error::Error::Protocol(error::Protocol::UniqueConstraintViolation { msg }) = res {
        assert_eq!(msg, "Job not unique");
    } else {
        panic!("Expected protocol error.")
    }

    // Let's now consume the job which is 'holding' a unique lock:
    let had_job = c.run_one(0, &[queue_name]).unwrap();
    assert!(had_job);

    // And check that the queue is really empty (`job2` from above
    // has not been queued indeed):
    let queue_is_empty = !c.run_one(0, &[queue_name]).unwrap();
    assert!(queue_is_empty);

    // Now let's repeat the latter case, but providing different args to job2:
    let job1 = JobBuilder::new(job_type)
        .args(args.clone())
        .queue(queue_name)
        .unique_for(unique_for_secs)
        .build();
    p.enqueue(job1).unwrap();
    // this one is *NOT* a 'duplicate' ...
    let job2 = JobBuilder::new(job_type)
        .args(vec![Value::from("ISBN-13:9781718501850"), Value::from(101)])
        .queue(queue_name)
        .unique_for(unique_for_secs)
        .build();
    // ... so the server will accept it:
    p.enqueue(job2).unwrap();

    assert_had_one!(&mut c, queue_name);
    assert_had_one!(&mut c, queue_name);
    // and the queue is empty again:
    assert_is_empty!(&mut c, queue_name);
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
    if let error::Error::Protocol(error::Protocol::UniqueConstraintViolation { msg }) = res {
        assert_eq!(msg, "Job not unique");
    } else {
        panic!("Expected protocol error.")
    }

    handle.join().expect("should join successfully");

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
        .unwrap();
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
    producer_b
        .enqueue(
            JobBuilder::new(job_type)
                .args(vec![difficulty_level])
                .queue(queue_name)
                .unique_for(unique_for)
                .build(),
        )
        .unwrap();

    handle.join().expect("should join successfully");
}

#[test]
fn ent_unique_job_bypass_unique_lock() {
    use faktory::error;

    skip_if_not_enterprise!();

    let url = learn_faktory_url();

    let mut producer = Producer::connect(Some(&url)).unwrap();

    let job1 = Job::builder("order")
        .queue("ent_unique_job_bypass_unique_lock")
        .unique_for(60)
        .build();

    // Now the following job is _technically_ a 'duplicate', BUT if the `unique_for` value is not set,
    // the uniqueness lock will be bypassed on the server. This special case is mentioned in the docs:
    // https://github.com/contribsys/faktory/wiki/Ent-Unique-Jobs#bypassing-uniqueness
    let job2 = Job::builder("order") // same jobtype and args (args are just not set)
        .queue("ent_unique_job_bypass_unique_lock") // same queue
        .build(); // NB: `unique_for` not set

    producer.enqueue(job1).unwrap();
    producer.enqueue(job2).unwrap(); // bypassing the lock!

    // This _is_ a 'duplicate'.
    let job3 = Job::builder("order")
        .queue("ent_unique_job_bypass_unique_lock")
        .unique_for(60) // NB
        .build();

    let res = producer.enqueue(job3).unwrap_err(); // NOT bypassing the lock!

    if let error::Error::Protocol(error::Protocol::UniqueConstraintViolation { msg }) = res {
        assert_eq!(msg, "Job not unique");
    } else {
        panic!("Expected protocol error.")
    }
}

#[test]
fn test_tracker_can_send_and_retrieve_job_execution_progress() {
    use std::{
        io,
        sync::{Arc, Mutex},
        thread, time,
    };

    skip_if_not_enterprise!();

    let url = learn_faktory_url();

    let t = Arc::new(Mutex::new(
        Tracker::connect(Some(&url)).expect("job progress tracker created successfully"),
    ));

    let t_captured = Arc::clone(&t);

    let mut p = Producer::connect(Some(&url)).unwrap();

    let job_tackable = JobBuilder::new("order")
        .args(vec![Value::from("ISBN-13:9781718501850")])
        .queue("test_tracker_can_send_progress_update")
        .build_trackable();

    let job_ordinary = JobBuilder::new("order")
        .args(vec![Value::from("ISBN-13:9781718501850")])
        .queue("test_tracker_can_send_progress_update")
        .build();

    // let's remember this job's id:
    let job_id = job_tackable.id().to_owned();
    let job_id_captured = job_id.clone();

    p.enqueue(job_tackable).expect("enqueued");

    let mut c = ConsumerBuilder::default();
    c.register("order", move |job| -> io::Result<_> {
        // trying to set progress on a community edition of Faktory will give:
        // 'an internal server error occurred: tracking subsystem is only available in Faktory Enterprise'
        let result = t_captured.lock().expect("lock acquired").set_progress(
            ProgressUpdateBuilder::new(&job_id_captured)
                .desc("Still processing...".to_owned())
                .percent(32)
                .build(),
        );
        assert!(result.is_ok());
        // let's sleep for a while ...
        thread::sleep(time::Duration::from_secs(2));

        // ... and read the progress info
        let result = t_captured
            .lock()
            .expect("lock acquired")
            .get_progress(job_id_captured.clone())
            .expect("Retrieved progress update over the wire");

        assert!(result.is_some());
        let result = result.unwrap();
        assert_eq!(result.jid, job_id_captured.clone());
        assert_eq!(result.state, "working");
        assert!(result.updated_at.is_some());
        assert_eq!(result.desc, Some("Still processing...".to_owned()));
        assert_eq!(result.percent, Some(32));
        // considering the job done
        Ok(eprintln!("{:?}", job))
    });

    let mut c = c
        .connect(Some(&url))
        .expect("Successfully ran a handshake with 'Faktory'");
    assert_had_one!(&mut c, "test_tracker_can_send_progress_update");

    let result = t
        .lock()
        .expect("lock acquired successfully")
        .get_progress(job_id.clone())
        .expect("Retrieved progress update over the wire once again")
        .expect("Some progress");

    assert_eq!(result.jid, job_id);
    // 'Faktory' will be keeping last known update for at least 30 minutes:
    assert_eq!(result.desc, Some("Still processing...".to_owned()));
    assert_eq!(result.percent, Some(32));

    // But it actually knows the job's real status, since the consumer (worker)
    // informed it immediately after finishing with the job:
    assert_eq!(result.state, "success");

    // What about 'ordinary' job ?
    let job_id = job_ordinary.id().to_owned().clone();

    // Sending it ...
    p.enqueue(job_ordinary)
        .expect("Successfuly send to Faktory");

    // ... and asking for its progress
    let progress = t
        .lock()
        .expect("lock acquired")
        .get_progress(job_id.clone())
        .expect("Retrieved progress update over the wire once again")
        .expect("Some progress");

    // From the docs:
    // There are several reasons why a job's state might be unknown:
    //    The JID is invalid or was never actually enqueued.
    //    The job was not tagged with the track variable in the job's custom attributes: custom:{"track":1}.
    //    The job's tracking structure has expired in Redis. It lives for 30 minutes and a big queue backlog can lead to expiration.
    assert_eq!(progress.jid, job_id);

    // Returned from Faktory: '{"jid":"f7APFzrS2RZi9eaA","state":"unknown","updated_at":""}'
    assert_eq!(progress.state, "unknown");
    assert!(progress.updated_at.is_none());
    assert!(progress.percent.is_none());
    assert!(progress.desc.is_none());
}

#[test]
fn test_batch_of_jobs_can_be_initiated() {
    skip_if_not_enterprise!();
    let url = learn_faktory_url();

    let mut p = Producer::connect(Some(&url)).unwrap();
    let mut c = ConsumerBuilder::default();
    c.register("thumbnail", move |_job| -> io::Result<_> { Ok(()) });
    c.register("clean_up", move |_job| -> io::Result<_> { Ok(()) });
    let mut c = c.connect(Some(&url)).unwrap();
    let mut t = Tracker::connect(Some(&url)).expect("job progress tracker created successfully");

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
        .build();

    let cb_job = Job::builder("clean_up")
        .queue("test_batch_of_jobs_can_be_initiated__CALLBACKs")
        .build();

    let batch =
        Batch::builder("Image resizing workload".to_string()).with_complete_callback(cb_job);

    let time_just_before_batch_init = Utc::now();

    let mut b = p.start_batch(batch).unwrap();

    // let's remember batch id:
    let bid = b.id().to_string();

    b.add(job_1).unwrap();
    b.add(job_2).unwrap();
    b.add(job_3).unwrap();
    b.commit().unwrap();

    // The batch has been committed, let's see its status:
    let time_just_before_getting_status = Utc::now();

    let s = t
        .get_batch_status(bid.clone())
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
    assert_eq!(s.success_callback_state, ""); // we did not even provide the 'success' callback
    assert_eq!(s.complete_callback_state, ""); // the 'complete' callback is pending

    // consume and execute job 1 ...
    assert_had_one!(&mut c, "test_batch_of_jobs_can_be_initiated");
    // ... and try consuming from the "callback" queue:
    assert_is_empty!(&mut c, "test_batch_of_jobs_can_be_initiated__CALLBACKs");

    // let's ask the Faktory server about the batch status after
    // we have consumed one job from this batch:
    let s = t
        .get_batch_status(bid.clone())
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
        .expect("successfully fetched batch status from server...")
        .expect("...and it's not none");

    // this is because we have just consumed and executed 2 of 3 jobs:
    assert_eq!(s.total, 3);
    assert_eq!(s.pending, 0);
    assert_eq!(s.failed, 0);
    assert_eq!(s.complete_callback_state, "1"); // callback has been enqueued!!

    // let's now successfully consume from the "callback" queue:
    assert_had_one!(&mut c, "test_batch_of_jobs_can_be_initiated__CALLBACKs");

    // let's check batch status one last time:
    let s = t
        .get_batch_status(bid.clone())
        .expect("successfully fetched batch status from server...")
        .expect("...and it's not none");

    // this is because we have just consumed and executed 2 of 3 jobs:
    assert_eq!(s.complete_callback_state, "2"); // means calledback successfully executed
}

#[test]
fn test_batches_can_be_nested() {
    skip_if_not_enterprise!();
    let url = learn_faktory_url();

    // Set up 'producer', 'consumer', and 'tracker':
    let mut p = Producer::connect(Some(&url)).unwrap();
    let mut c = ConsumerBuilder::default();
    c.register("jobtype", move |_job| -> io::Result<_> { Ok(()) });
    let mut _c = c.connect(Some(&url)).unwrap();
    let mut t = Tracker::connect(Some(&url)).expect("job progress tracker created successfully");

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
    let parent_batch =
        Batch::builder("Parent batch".to_string()).with_success_callback(parent_cb_job);
    let mut parent_batch = p.start_batch(parent_batch).unwrap();
    let parent_batch_id = parent_batch.id().to_owned();
    parent_batch.add(parent_job1).unwrap();

    let child_batch = Batch::builder("Child batch".to_string()).with_success_callback(child_cb_job);
    let mut child_batch = parent_batch.start_batch(child_batch).unwrap();
    let child_batch_id = child_batch.id().to_owned();
    child_batch.add(child_job_1).unwrap();
    child_batch.add(child_job_2).unwrap();

    let grandchild_batch =
        Batch::builder("Grandchild batch".to_string()).with_success_callback(grandchild_cb_job);
    let mut grandchild_batch = child_batch.start_batch(grandchild_batch).unwrap();
    let grandchild_batch_id = grandchild_batch.id().to_owned();
    grandchild_batch.add(grand_child_job_1).unwrap();

    grandchild_batch.commit().unwrap();
    child_batch.commit().unwrap();
    parent_batch.commit().unwrap();
    // batches finish

    let parent_status = t
        .get_batch_status(parent_batch_id.clone())
        .unwrap()
        .unwrap();
    assert_eq!(parent_status.description, Some("Parent batch".to_string()));
    assert_eq!(parent_status.total, 1);
    assert_eq!(parent_status.parent_bid, None);

    let child_status = t.get_batch_status(child_batch_id.clone()).unwrap().unwrap();
    assert_eq!(child_status.description, Some("Child batch".to_string()));
    assert_eq!(child_status.total, 2);
    assert_eq!(child_status.parent_bid, Some(parent_batch_id));

    let grandchild_status = t.get_batch_status(grandchild_batch_id).unwrap().unwrap();
    assert_eq!(
        grandchild_status.description,
        Some("Grandchild batch".to_string())
    );
    assert_eq!(grandchild_status.total, 1);
    assert_eq!(grandchild_status.parent_bid, Some(child_batch_id));
}

#[test]
fn test_callback_will_not_be_queued_unless_batch_gets_committed() {
    skip_if_not_enterprise!();
    let url = learn_faktory_url();

    // prepare a producer, a consumer of 'order' jobs, and a tracker:
    let mut p = Producer::connect(Some(&url)).unwrap();
    let mut c = ConsumerBuilder::default();
    c.register("order", move |_job| -> io::Result<_> { Ok(()) });
    c.register("order_clean_up", move |_job| -> io::Result<_> { Ok(()) });
    let mut c = c.connect(Some(&url)).unwrap();
    let mut t = Tracker::connect(Some(&url)).unwrap();

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
            Batch::builder("Orders processing workload".to_string())
                .with_success_callback(callbacks.next().unwrap()),
        )
        .unwrap();
    let bid = b.id().to_string();

    // push 3 jobs onto this batch, but DO NOT commit the batch:
    for _ in 0..3 {
        b.add(jobs.next().unwrap()).unwrap();
    }

    // check this batch's status:
    let s = t.get_batch_status(bid.clone()).unwrap().unwrap();
    assert_eq!(s.total, 3);
    assert_eq!(s.pending, 3);
    assert_eq!(s.success_callback_state, ""); // has not been queued;

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
    let s = t.get_batch_status(bid.clone()).unwrap().unwrap();
    assert_eq!(s.total, 3);
    assert_eq!(s.pending, 0);
    assert_eq!(s.failed, 0);
    assert_eq!(s.success_callback_state, ""); // not just yet;

    // to double-check, let's assert the success callbacks queue is empty:
    assert_is_empty!(
        &mut c,
        "test_callback_will_not_be_queued_unless_batch_gets_committed__CALLBACKs"
    );

    // now let's COMMIT the batch ...
    b.commit().unwrap();

    // ... and check batch status:
    let s = t.get_batch_status(bid.clone()).unwrap().unwrap();
    assert_eq!(s.success_callback_state, "1"); // callback has been queued;

    // finally, let's consume from the success callbacks queue ...
    assert_had_one!(
        &mut c,
        "test_callback_will_not_be_queued_unless_batch_gets_committed__CALLBACKs"
    );

    // ... and see the final status:
    let s = t.get_batch_status(bid.clone()).unwrap().unwrap();
    assert_eq!(s.success_callback_state, "2"); // callback successfully executed;
}

#[test]
fn test_callback_will_be_queue_upon_commit_even_if_batch_is_empty() {
    use std::{thread, time};

    skip_if_not_enterprise!();
    let url = learn_faktory_url();
    let mut p = Producer::connect(Some(&url)).unwrap();
    let mut t = Tracker::connect(Some(&url)).unwrap();
    let jobtype = "callback_jobtype";
    let q_name = "test_callback_will_be_queue_upon_commit_even_if_batch_is_empty";
    let mut callbacks = some_jobs(jobtype, q_name, 2);
    let b = p
        .start_batch(
            Batch::builder("Orders processing workload".to_string())
                .with_callbacks(callbacks.next().unwrap(), callbacks.next().unwrap()),
        )
        .unwrap();
    let bid = b.id().to_owned();

    let s = t.get_batch_status(bid.clone()).unwrap().unwrap();
    assert_eq!(s.total, 0); // no jobs in the batch;
    assert_eq!(s.success_callback_state, ""); // not queued;
    assert_eq!(s.complete_callback_state, ""); // not queued;

    b.commit().unwrap();

    // let's give the Faktory server some time:
    thread::sleep(time::Duration::from_secs(2));

    let s = t.get_batch_status(bid.clone()).unwrap().unwrap();
    assert_eq!(s.total, 0); // again, there are no jobs in the batch ...

    // The docs say "If you don't push any jobs into the batch, any callbacks will fire immediately upon BATCH COMMIT."
    // and "the success callback for a batch will always enqueue after the complete callback"
    assert_eq!(s.complete_callback_state, "1"); // queued
    assert_eq!(s.success_callback_state, ""); // not queued

    let mut c = ConsumerBuilder::default();
    c.register(jobtype, move |_job| -> io::Result<_> { Ok(()) });
    let mut c = c.connect(Some(&url)).unwrap();

    assert_had_one!(&mut c, q_name); // complete callback consumed

    let s = t.get_batch_status(bid.clone()).unwrap().unwrap();
    assert_eq!(s.total, 0);
    assert_eq!(s.complete_callback_state, "2"); // successfully executed
    assert_eq!(s.success_callback_state, "1"); // queued

    assert_had_one!(&mut c, q_name); // success callback consumed

    let s = t.get_batch_status(bid.clone()).unwrap().unwrap();
    assert_eq!(s.total, 0);
    assert_eq!(s.complete_callback_state, "2"); // successfully executed
    assert_eq!(s.success_callback_state, "2"); // successfully executed
}

#[test]
fn test_batch_can_be_reopened_add_extra_jobs_and_batches_added() {
    skip_if_not_enterprise!();
    let url = learn_faktory_url();
    let mut p = Producer::connect(Some(&url)).unwrap();
    let mut t = Tracker::connect(Some(&url)).unwrap();
    let mut jobs = some_jobs("order", "test_batch_can_be_reopned_add_extra_jobs_added", 4);
    let mut callbacks = some_jobs(
        "order_clean_up",
        "test_batch_can_be_reopned_add_extra_jobs_added__CALLBACKs",
        1,
    );

    let b = Batch::builder("Orders processing workload".to_string())
        .with_success_callback(callbacks.next().unwrap());

    let mut b = p.start_batch(b).unwrap();
    let bid = b.id().to_string();
    b.add(jobs.next().unwrap()).unwrap(); // 1 job
    b.add(jobs.next().unwrap()).unwrap(); // 2 jobs

    let status = t.get_batch_status(bid.clone()).unwrap().unwrap();
    assert_eq!(status.total, 2);
    assert_eq!(status.pending, 2);

    // ############################## SUBTEST 0 ##########################################
    // Let's fist of all try to open the batch we have not committed yet:
    let mut b = p.open_batch(bid.clone()).unwrap();
    assert_eq!(b.id(), bid);
    b.add(jobs.next().unwrap()).unwrap(); // 3 jobs

    b.commit().unwrap(); // committig the batch

    let status = t.get_batch_status(bid.clone()).unwrap().unwrap();
    assert_eq!(status.total, 3);
    assert_eq!(status.pending, 3);

    // Subtest 0 result:
    // The Faktory server let's us open the uncommitted batch. This is something not mention
    // in the docs, but still worth checking.

    // ############################## SUBTEST 1 ##########################################
    // From the docs:
    // """Note that, once committed, only a job within the batch may reopen it.
    // Faktory will return an error if you dynamically add jobs from "outside" the batch;
    // this is to prevent a race condition between callbacks firing and an outsider adding more jobs."""
    // Ref: https://github.com/contribsys/faktory/wiki/Ent-Batches#batch-open-bid (Jan 10, 2024)

    // Let's try to open an already committed batch:
    let mut b = p.open_batch(bid.clone()).unwrap();
    assert_eq!(b.id(), bid);
    b.add(jobs.next().unwrap()).unwrap(); // 4 jobs
    b.commit().unwrap(); // committing the batch again!

    let s = t.get_batch_status(bid.clone()).unwrap().unwrap();
    assert_eq!(s.total, 4);
    assert_eq!(s.pending, 4);

    // Subtest 1 result:
    // We managed to open a batch "from outside" and the server accepted the job INSTEAD OF ERRORING BACK.
    // ############################ END OF SUBTEST 1 #######################################

    // ############################## SUBTEST 2 ############################################
    // Let's see if we will be able to - again - open the committed batch "from outside" and
    // add a nested batch to it.
    let mut b = p.open_batch(bid.clone()).unwrap();
    assert_eq!(b.id(), bid); // this is to make sure this is the same batch INDEED
    let mut nested_callbacks = some_jobs(
        "order_clean_up__NESTED",
        "test_batch_can_be_reopned_add_extra_jobs_added__CALLBACKs__NESTED",
        2,
    );
    let nested_batch_declaration =
        Batch::builder("Orders processing workload. Nested stage".to_string()).with_callbacks(
            nested_callbacks.next().unwrap(),
            nested_callbacks.next().unwrap(),
        );
    let nested_batch = b.start_batch(nested_batch_declaration).unwrap();
    let nested_bid = nested_batch.id().to_string();
    // committing the nested batch without any jobs
    // since those are just not relevant for this test:
    nested_batch.commit().unwrap();

    let s = t.get_batch_status(nested_bid.clone()).unwrap().unwrap();
    assert_eq!(s.total, 0);
    assert_eq!(s.parent_bid, Some(bid)); // this is really our child batch
    assert_eq!(s.complete_callback_state, "1"); // has been enqueud

    // Subtest 2 result:
    // We managed to open an already committed batch "from outside" and the server accepted
    // a nested batch INSTEAD OF ERRORING BACK.
    // ############################ END OF SUBTEST 2 #######################################

    // ############################## SUBTEST 3 ############################################
    // From the docs:
    // """Once a callback has enqueued for a batch, you may not add anything to the batch."""
    // ref: https://github.com/contribsys/faktory/wiki/Ent-Batches#guarantees (Jan 10, 2024)

    // Let's try to re-open the nested batch that we have already committed and add some jobs to it.
    let mut b = p.open_batch(nested_bid.clone()).unwrap();
    assert_eq!(b.id(), nested_bid); // this is to make sure this is the same batch INDEED
    let mut more_jobs = some_jobs(
        "order_clean_up__NESTED",
        "test_batch_can_be_reopned_add_extra_jobs_added__NESTED",
        2,
    );
    b.add(more_jobs.next().unwrap()).unwrap();
    b.add(more_jobs.next().unwrap()).unwrap();
    b.commit().unwrap();

    let s = t.get_batch_status(nested_bid.clone()).unwrap().unwrap();
    assert_eq!(s.complete_callback_state, "1"); // again, it has been enqueud ...
    assert_eq!(s.pending, 2); // ... though there are pending jobs
    assert_eq!(s.total, 2);

    // Subtest 3 result:
    // We were able to add more jobs to the batch for which the Faktory server had already
    // queued the callback.
    // ############################## END OF SUBTEST 3 #####################################

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
