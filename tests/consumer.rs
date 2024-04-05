mod mock;

use faktory::*;
use std::{io, time::Duration};
use tokio::{spawn, time::sleep};

#[tokio::test(flavor = "multi_thread")]
async fn hello() {
    let mut s = mock::Stream::default();
    let mut c: WorkerBuilder<io::Error> = WorkerBuilder::default();
    c.hostname("host".to_string())
        .wid("wid".into())
        .labels(vec!["foo".to_string(), "bar".to_string()]);
    c.register("never_called", |_j: Job| async move { unreachable!() });
    let c = c.connect_with(s.clone(), None).await.unwrap();
    let written = s.pop_bytes_written(0);
    assert!(written.starts_with(b"HELLO {"));
    let written: serde_json::Value = serde_json::from_slice(&written[b"HELLO ".len()..]).unwrap();
    let written = written.as_object().unwrap();
    assert_eq!(
        written.get("hostname").and_then(|h| h.as_str()),
        Some("host")
    );
    assert_eq!(written.get("wid").and_then(|h| h.as_str()), Some("wid"));
    assert_eq!(written.get("pid").map(|h| h.is_number()), Some(true));
    assert_eq!(written.get("v").and_then(|h| h.as_i64()), Some(2));
    let labels = written["labels"].as_array().unwrap();
    assert_eq!(labels, &["foo", "bar"]);

    drop(c);
    let written = s.pop_bytes_written(0);
    assert_eq!(written, b"END\r\n");
}

#[tokio::test(flavor = "multi_thread")]
async fn hello_pwd() {
    let mut s = mock::Stream::with_salt(1545, "55104dc76695721d");

    let mut c: WorkerBuilder<io::Error> = WorkerBuilder::default();
    c.register("never_called", |_j: Job| async move { unreachable!() });
    let c = c
        .connect_with(s.clone(), Some("foobar".to_string()))
        .await
        .unwrap();
    let written = s.pop_bytes_written(0);
    assert!(written.starts_with(b"HELLO {"));
    let written: serde_json::Value = serde_json::from_slice(&written[b"HELLO ".len()..]).unwrap();
    let written = written.as_object().unwrap();
    assert_eq!(
        written.get("pwdhash").and_then(|h| h.as_str()),
        Some("6d877f8e5544b1f2598768f817413ab8a357afffa924dedae99eb91472d4ec30")
    );

    drop(c);
}

#[tokio::test(flavor = "multi_thread")]
async fn dequeue() {
    let mut s = mock::Stream::default();
    let mut c = WorkerBuilder::default();
    c.register("foobar", |job: Job| async move {
        assert_eq!(job.args(), &["z"]);
        Ok::<(), io::Error>(())
    });
    let mut c = c.connect_with(s.clone(), None).await.unwrap();
    s.ignore(0);

    s.push_bytes_to_read(
        0,
        b"$188\r\n\
        {\
        \"jid\":\"foojid\",\
        \"queue\":\"default\",\
        \"jobtype\":\"foobar\",\
        \"args\":[\"z\"],\
        \"created_at\":\"2017-11-01T21:02:35.772981326Z\",\
        \"enqueued_at\":\"2017-11-01T21:02:35.773318394Z\",\
        \"reserve_for\":600,\
        \"retry\":25\
        }\r\n",
    );
    s.ok(0); // for the ACK
    if let Err(e) = c.run_one(0, &["default"]).await {
        println!("{:?}", e);
        unreachable!();
    }

    let written = s.pop_bytes_written(0);
    assert_eq!(
        written,
        &b"FETCH default\r\n\
        ACK {\"jid\":\"foojid\"}\r\n"[..]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn dequeue_first_empty() {
    let mut s = mock::Stream::default();
    let mut c = WorkerBuilder::default();
    c.register("foobar", |job: Job| async move {
        assert_eq!(job.args(), &["z"]);
        Ok::<(), io::Error>(())
    });
    let mut c = c.connect_with(s.clone(), None).await.unwrap();
    s.ignore(0);

    s.push_bytes_to_read(
        0,
        b"$0\r\n\r\n$188\r\n\
        {\
        \"jid\":\"foojid\",\
        \"queue\":\"default\",\
        \"jobtype\":\"foobar\",\
        \"args\":[\"z\"],\
        \"created_at\":\"2017-11-01T21:02:35.772981326Z\",\
        \"enqueued_at\":\"2017-11-01T21:02:35.773318394Z\",\
        \"reserve_for\":600,\
        \"retry\":25\
        }\r\n",
    );
    s.ok(0); // for the ACK

    // run once, shouldn't do anything
    match c.run_one(0, &["default"]).await {
        Ok(did_work) => assert!(!did_work),
        Err(e) => {
            println!("{:?}", e);
            unreachable!();
        }
    }
    // run again, this time doing the job
    match c.run_one(0, &["default"]).await {
        Ok(did_work) => assert!(did_work),
        Err(e) => {
            println!("{:?}", e);
            unreachable!();
        }
    }

    let written = s.pop_bytes_written(0);
    assert_eq!(
        written,
        &b"\
        FETCH default\r\n\
        FETCH default\r\n\
        ACK {\"jid\":\"foojid\"}\r\n\
        "[..]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn well_behaved() {
    let mut s = mock::Stream::new(2); // main plus worker
    let mut c = WorkerBuilder::default();
    c.wid("wid".into());
    c.register("foobar", |_| async move {
        // NOTE: this time needs to be so that it lands between the first heartbeat and the second
        sleep(Duration::from_secs(7)).await;
        Ok::<(), io::Error>(())
    });
    let mut c = c.connect_with(s.clone(), None).await.unwrap();
    s.ignore(0);

    // push a job that'll take a while to run
    s.push_bytes_to_read(
        1,
        b"$182\r\n\
            {\
            \"jid\":\"jid\",\
            \"queue\":\"default\",\
            \"jobtype\":\"foobar\",\
            \"args\":[],\
            \"created_at\":\"2017-11-01T21:02:35.772981326Z\",\
            \"enqueued_at\":\"2017-11-01T21:02:35.773318394Z\",\
            \"reserve_for\":600,\
            \"retry\":25\
            }\r\n",
    );

    let jh = spawn(async move { c.run(&["default"]).await });

    // the running thread won't return for a while. the heartbeat thingy is going to eventually
    // send a heartbeat, and we want to respond to that with a "quiet" to make it not accept any
    // more jobs.
    s.push_bytes_to_read(0, b"+{\"state\":\"quiet\"}\r\n");

    // eventually, the job is going to finish and send an ACK
    s.ok(1);

    // then, we want to send a terminate to tell the thread to exit
    s.push_bytes_to_read(0, b"+{\"state\":\"terminate\"}\r\n");

    // at this point, c.run() should eventually return with Ok(0) indicating that it finished.
    assert_eq!(jh.await.unwrap().unwrap(), 0);

    // heartbeat should have seen two beats (quiet + terminate)
    let written = s.pop_bytes_written(0);
    let msgs = "\
                BEAT {\"wid\":\"wid\"}\r\n\
                BEAT {\"wid\":\"wid\"}\r\n\
                END\r\n";
    assert_eq!(std::str::from_utf8(&written[..]).unwrap(), msgs);

    // worker should have fetched once, and acked once
    let written = s.pop_bytes_written(1);
    let msgs = "\r\n\
                FETCH default\r\n\
                ACK {\"jid\":\"jid\"}\r\n\
                END\r\n";
    assert_eq!(
        std::str::from_utf8(&written[(written.len() - msgs.len())..]).unwrap(),
        msgs
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn no_first_job() {
    let mut s = mock::Stream::new(2);
    let mut c = WorkerBuilder::default();
    c.wid("wid".into());
    c.register("foobar", |_| async move {
        // NOTE: this time needs to be so that it lands between the first heartbeat and the second
        sleep(Duration::from_secs(7)).await;
        Ok::<(), io::Error>(())
    });
    let mut c = c.connect_with(s.clone(), None).await.unwrap();
    s.ignore(0);

    // push a job that'll take a while to run
    s.push_bytes_to_read(
        1,
        b"$0\r\n\r\n$182\r\n\
            {\
            \"jid\":\"jid\",\
            \"queue\":\"default\",\
            \"jobtype\":\"foobar\",\
            \"args\":[],\
            \"created_at\":\"2017-11-01T21:02:35.772981326Z\",\
            \"enqueued_at\":\"2017-11-01T21:02:35.773318394Z\",\
            \"reserve_for\":600,\
            \"retry\":25\
            }\r\n",
    );

    let jh = spawn(async move { c.run(&["default"]).await });

    // the running thread won't return for a while. the heartbeat thingy is going to eventually
    // send a heartbeat, and we want to respond to that with a "quiet" to make it not accept any
    // more jobs.
    s.push_bytes_to_read(0, b"+{\"state\":\"quiet\"}\r\n");

    // eventually, the job is going to finish and send an ACK
    s.ok(1);

    // then, we want to send a terminate to tell the thread to exit
    s.push_bytes_to_read(0, b"+{\"state\":\"terminate\"}\r\n");

    // at this point, c.run() should eventually return with Ok(0) indicating that it finished.
    assert_eq!(jh.await.unwrap().unwrap(), 0);

    // heartbeat should have seen two beats (quiet + terminate)
    let written = s.pop_bytes_written(0);
    let msgs = "\
                BEAT {\"wid\":\"wid\"}\r\n\
                BEAT {\"wid\":\"wid\"}\r\n\
                END\r\n";
    assert_eq!(std::str::from_utf8(&written[..]).unwrap(), msgs);

    // worker should have fetched twice, and acked once
    let written = s.pop_bytes_written(1);
    let msgs = "\r\n\
                FETCH default\r\n\
                FETCH default\r\n\
                ACK {\"jid\":\"jid\"}\r\n\
                END\r\n";
    assert_eq!(
        std::str::from_utf8(&written[(written.len() - msgs.len())..]).unwrap(),
        msgs
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn well_behaved_many() {
    let mut s = mock::Stream::new(3);
    let mut c = WorkerBuilder::default();
    c.workers(2);
    c.wid("wid".into());
    c.register("foobar", |_| async move {
        // NOTE: this time needs to be so that it lands between the first heartbeat and the second
        sleep(Duration::from_secs(7)).await;
        Ok::<(), io::Error>(())
    });
    let mut c = c.connect_with(s.clone(), None).await.unwrap();
    s.ignore(0);

    // push two jobs that'll take a while to run
    // they should run in parallel (if they don't, we'd only see one ACK)
    for i in 0..2 {
        // we don't use different jids because we don't want to have to fiddle with them being
        // ACKed in non-deterministic order. this has the unfortunate side-effect that
        // *technically* the implementation could have a bug where it acks the same job twice, and
        // we wouldn't detect it...
        s.push_bytes_to_read(
            i + 1,
            b"$182\r\n\
            {\
            \"jid\":\"jid\",\
            \"queue\":\"default\",\
            \"jobtype\":\"foobar\",\
            \"args\":[],\
            \"created_at\":\"2017-11-01T21:02:35.772981326Z\",\
            \"enqueued_at\":\"2017-11-01T21:02:35.773318394Z\",\
            \"reserve_for\":600,\
            \"retry\":25\
            }\r\n",
        );
    }

    let jh = spawn(async move { c.run(&["default"]).await });

    // the running thread won't return for a while. the heartbeat thingy is going to eventually
    // send a heartbeat, and we want to respond to that with a "quiet" to make it not accept any
    // more jobs.
    s.push_bytes_to_read(0, b"+{\"state\":\"quiet\"}\r\n");

    // eventually, the jobs are going to finish and send an ACK
    s.ok(1);
    s.ok(2);

    // then, we want to send a terminate to tell the thread to exit
    s.push_bytes_to_read(0, b"+{\"state\":\"terminate\"}\r\n");

    // at this point, c.run() should eventually return with Ok(0) indicating that it finished.
    assert_eq!(jh.await.unwrap().unwrap(), 0);

    // heartbeat should have seen two beats (quiet + terminate)
    let written = s.pop_bytes_written(0);
    let msgs = "\
                BEAT {\"wid\":\"wid\"}\r\n\
                BEAT {\"wid\":\"wid\"}\r\n\
                END\r\n";
    assert_eq!(std::str::from_utf8(&written[..]).unwrap(), msgs);

    // each worker should have fetched once, and acked once
    for i in 0..2 {
        let written = s.pop_bytes_written(i + 1);
        let msgs = "\r\n\
                    FETCH default\r\n\
                    ACK {\"jid\":\"jid\"}\r\n\
                    END\r\n";
        assert_eq!(
            std::str::from_utf8(&written[(written.len() - msgs.len())..]).unwrap(),
            msgs
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn terminate() {
    let mut s = mock::Stream::new(2);
    let mut c: WorkerBuilder<io::Error> = WorkerBuilder::default();
    c.wid("wid".into());
    c.register("foobar", |_| async move {
        loop {
            sleep(Duration::from_secs(5)).await;
        }
    });
    let mut c = c.connect_with(s.clone(), None).await.unwrap();
    s.ignore(0);

    s.push_bytes_to_read(
        1,
        b"$186\r\n\
        {\
        \"jid\":\"forever\",\
        \"queue\":\"default\",\
        \"jobtype\":\"foobar\",\
        \"args\":[],\
        \"created_at\":\"2017-11-01T21:02:35.772981326Z\",\
        \"enqueued_at\":\"2017-11-01T21:02:35.773318394Z\",\
        \"reserve_for\":600,\
        \"retry\":25\
        }\r\n",
    );

    let jh = spawn(async move { c.run(&["default"]).await });

    // the running thread won't ever return, because the job never exits. the heartbeat thingy is
    // going to eventually send a heartbeat, and we want to respond to that with a "terminate"
    s.push_bytes_to_read(0, b"+{\"state\":\"terminate\"}\r\n");

    // at this point, c.run() should immediately return with Ok(1) indicating that one job is still
    // running.
    assert_eq!(jh.await.unwrap().unwrap(), 1);

    // heartbeat should have seen one beat (terminate) and then send FAIL
    let written = s.pop_bytes_written(0);
    let beat = b"BEAT {\"wid\":\"wid\"}\r\nFAIL ";
    assert_eq!(&written[0..beat.len()], &beat[..]);
    assert!(written.ends_with(b"\r\nEND\r\n"));
    println!(
        "{}",
        std::str::from_utf8(&written[beat.len()..(written.len() - b"\r\nEND\r\n".len())]).unwrap()
    );
    let written: serde_json::Value =
        serde_json::from_slice(&written[beat.len()..(written.len() - b"\r\nEND\r\n".len())])
            .unwrap();
    assert_eq!(
        written
            .as_object()
            .and_then(|o| o.get("jid"))
            .and_then(|v| v.as_str()),
        Some("forever")
    );

    // worker should have just fetched once
    let written = s.pop_bytes_written(1);
    let msgs = "\r\n\
                FETCH default\r\n";
    assert_eq!(
        std::str::from_utf8(&written[(written.len() - msgs.len())..]).unwrap(),
        msgs
    );
}
