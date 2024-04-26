/// This sketch should help appreciate how mock streams are being distributed across workers.
///
/// Side-note. Note how `CLIENT` (another node), `WEB UI` (browser), `FAKTORY SERVER` (contribsys Faktory binary),
/// and `FAKTORY WORKER (COORDINATOR)` are all separate processes (but `client` and `worker` _can_ belong
/// to the same process), while processing workers are threads (tokio tasks) in the `FAKTORY WORKER` process.
///
///     __________________________
///    |                          |                      __________
///    |          CLIENT          |                     |          |
///    | (PRODUCING AND TRACKING) |                     |  WEB UI  |
///    |__________________________|                     |__________|
///              |                                            |
///              |:7419             _________________         |
///              |                 |                 |        |:7420
///              |---------------> | FAKTORY SERVER  |  <-----|
///                                | localhost:7419  |
///                                | localhost:7420  |
///              |---------------> |_________________|
///              |:7419
///              |
///              |               ___________________________________________________________________________________________________
///              |              |                                                                                                   |
///              |_____________ | FAKTORY WORKER (COORDINATOR)                                                                      |
///                             |  | with at least N + 2 threads: main thread, heartbeat thread, and N processing worker threads -  |
///                             |  | tokio tasks - the actual workers; the desired count is specified via WorkerBuilder::workers(N) |
///                             |  |                                                                                                |
///                             |  |--> HEARTBEAT                                                                                   |
///                             |  |    - send b"BEAT {\"wid\":\"wid\"}" to Faktory every 5 seconds;                                |
///                             |  |    - set workers quiet if Faktory asked so;                                                    |
///                             |  |    - terminate workers if:                                                                     |
///                             |  |         - corresponding signal received from Faktory (returning Ok(true))                      |
///                             |  |         - one of the workers failed (returning Ok(false))                                      |
///                             |  |         - critical error in HEARTBEART thread occurs (returning Err(e)                         |
///                             |  |                                                                                                |
///                             |  |--> WORKER (index 0) with the following life-cycle:                                             |
///                             |       - get owned stream by reconnecting coordinator client via `self.stream.reconnect().await`   |
///                             |         (which for TcpStream will lead to establishing a new TCP connection to localhost:7419);   |                                        |
///                             |       - init a `Client` (say HELLO to HI);                                                        |
///                             |       - loop { self.run_one().await } until critical error or signal from coordinator;            |
///                             |  |--> ...                                                                                         |
///                             |  |--> WORKER (index N)                                                                            |
///                             |___________________________________________________________________________________________________|
///
/// Note how each processing worker is getting its owned stream and how we can control which stream is return
/// by means of implementing `Reconnect` for the stream we are supplying to the `Worker` initially.
///
/// So, what we are doing for testing purposes is:
/// 1) provide a [`Stream`] to [`connect_with`](`WorkerBuilder::connect_with`) that will be holding inside a vector of mock streams
///    and with a reference to the stream of current interest (see `mine` field on [`Stream`]) and a  "pointer" (see `take_next` field
///    on the private `mock::inner::Innner`) to the stream that will be given away on next call of `reconnect`;
/// 2) implement [`AsyncRead`] and [`AsyncWrite`] for the [`Stream`] so that internally we are polling read and write against the stream
///    referenced by [`mine`](Stream::mine).
/// 3) implement [`faktory::Reconnect`] for the [`Stream`] in a way that each time they call the `reconnect` method of the stream
///    we set `mine` to reference the stream that the "pointer" is currently pointing to and increment the "pointer" by 1;
/// 4) implement `Drop` for `mock::Stream` in a way that if the value of the "pointer" is not equal the length of the internal
///    vector of streams, we panic to indicate that we mis-planned things when setting up the test;
mod mock;

use faktory::*;
use std::{io, time::Duration};
use tokio::{spawn, time::sleep};

#[tokio::test(flavor = "multi_thread")]
async fn hello() {
    let mut s = mock::Stream::default();
    let mut c: WorkerBuilder<io::Error> = WorkerBuilder::default();
    c.hostname("host".to_string())
        .wid(WorkerId::new("wid"))
        .labels([
            "will".to_string(),
            "be!".to_string(),
            "overwritten".to_string(),
        ])
        .labels(["foo".to_string(), "bar".to_string()])
        .add_to_labels(["will".to_string()])
        .add_to_labels(["be".to_string(), "added".to_string()]);
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
    assert_eq!(labels, &["foo", "bar", "will", "be", "added"]);

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
    c.wid(WorkerId::new("wid"));
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
    let mut s = mock::Stream::new(2); // main plus worker
    let mut c = WorkerBuilder::default();
    c.wid(WorkerId::new("wid"));
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
    let mut s = mock::Stream::new(3); // main plus 2 workers
    let mut w = WorkerBuilder::default();
    w.workers(2);
    w.wid(WorkerId::new("wid"));
    w.register("foobar", |_| async move {
        // NOTE: this time needs to be so that it lands between the first heartbeat and the second
        sleep(Duration::from_secs(7)).await;
        Ok::<(), io::Error>(())
    });
    let mut w = w.connect_with(s.clone(), None).await.unwrap();
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

    let jh = spawn(async move { w.run(&["default"]).await });

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
    // Internally, the `take_next` member on the `mock::Inner` struct will be incremented from `0` to `1`,
    // while the `Stream::mine` wil be pointing to stream with index 0. See how we are later on ignoring bytes
    // written to this stream by means of `s.ignore(0)`.
    let mut s = mock::Stream::new(2); // main plus worker

    // prepare a worker with only never (!) returning handler
    let mut w: WorkerBuilder<io::Error> = WorkerBuilder::default();
    w.hostname("machine".into());
    w.wid(WorkerId::new("wid"));
    w.register("foobar", |_| async move {
        loop {
            sleep(Duration::from_secs(5)).await;
        }
    });

    let mut w = w.connect_with(s.clone(), None).await.unwrap();
    // what now is being ignored on `mine` channel are these written bytes (pid will vary):
    // b"HELLO {\"hostname\":\"machine\",\"wid\":\"wid\",\"pid\":7332,\"labels\":[\"rust\"],\"v\":2}\r\n"
    // this was the HELLO from main (coordinating) worker
    s.ignore(0);

    // as if a producing client had sent this job to Faktory and Faktory, in its turn,
    // had sent it to the processing (NB) worker, rather than coordinating one (note how we
    // are passing `1` as first arg to `s.push_bytes_to_read`)
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

    let jh = spawn(async move {
        // Note how running a coordinating leads to mock::Stream::reconnect:
        // `Worker::run` -> `Worker::spawn_worker_into` -> `Worker::for_worker` -> `Client::connect_again` -> `Stream::reconnect`
        //
        // So when the `w.run` is triggered, `Stream::reconnect` will fire and the `take_next` member on the `mock::Inner` struct
        // will be incremented from `1` to `2`. But, most importently, `mine` will now be pointing to the second
        // stream (stream with index 1) from this test, and the _actual_ worker (not the master worker (coordinator)) will
        // be talking via this stream.
        w.run(&["default"]).await
    });

    // the running thread won't ever return, because the job never exits. the heartbeat thingy is
    // going to eventually (in ~5 seconds) send a heartbeat, and we want to respond to that with a "terminate"
    s.push_bytes_to_read(0, b"+{\"state\":\"terminate\"}\r\n");

    // at this point, c.run() should immediately return with Ok(1) indicating that one job is still
    // running.
    assert_eq!(jh.await.unwrap().unwrap(), 1);

    // Heartbeat Thread (stream with index 0).
    //
    // Heartbeat thread should have sent one BEAT command, then an immediate FAIL, and a final END:
    // <---------- BEAT ---------><---------------------------- FAIL JOB -----------------------------------------><-END->
    // "BEAT {\"wid\":\"wid\"}\r\nFAIL {\"jid\":\"forever\",\"errtype\":\"unknown\",\"message\":\"terminated\"}\r\nEND\r\n"
    let written = s.pop_bytes_written(0);
    let beat = b"BEAT {\"wid\":\"wid\"}\r\nFAIL ";
    assert_eq!(&written[0..beat.len()], &beat[..]);
    assert!(written.ends_with(b"\r\nEND\r\n"));
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
    assert_eq!(written.get("errtype").unwrap().as_str(), Some("unknown"));
    assert_eq!(written.get("message").unwrap().as_str(), Some("terminated"));

    // Let's give the worker's client a chance to complete clean up on Client's drop (effectively send `END\r\n`),
    // and only after that pop bytes written into its stream. If we do not do this, we will end up with a flaky
    // test, where `END\r\n` will sometimes make it to the writer and sometimes not. The `500` ms are empirical.
    sleep(Duration::from_millis(500)).await;

    // Worker Thread (stream with index 1).
    //
    // Worker thread should have sent HELLO (which in coordinator case we thew away with `s.ignore(0)`), FETCH (
    // consume one job from the "default" queue), and END (which is performed as Client's clean-up).
    // <------------------------------------ HELLO (PASSWORDLESS) -------------------------------------><--- FETCH -----><-END->
    // "HELLO {\"hostname\":\"machine\",\"wid\":\"wid\",\"pid\":12628,\"labels\":[\"rust\"],\"v\":2}\r\nFETCH default\r\nEND\r\n"
    let written = s.pop_bytes_written(1);
    assert!(written.starts_with(b"HELLO {\"hostname\":\"machine\",\"wid\":\"wid\""));
    assert!(written.ends_with(b"\r\nFETCH default\r\nEND\r\n"));

    // P.S. Interestingly, before we switched to `JoinSet` in `Worker::run` internals, this last `END\r\n`
    // of the processing worker never actually got to the bytes written, no matter how much time you sleep
    // before popping those bytes from the mock stream.
    //
    // But generally speaking, the graceful situation is when the number of `HI`s and the number of `END`s are
    // equal. Why did they decide for `END` instead of `BYE` in Faktory ? :smile:
}
