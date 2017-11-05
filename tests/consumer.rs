extern crate faktory;
extern crate mockstream;
extern crate serde_json;
extern crate url;

mod mock;

use faktory::*;
use std::io;
use std::thread;
use std::time::Duration;

#[test]
fn hello() {
    let mut s = mock::Stream::default();
    s.hello();

    let mut c = ConsumerBuilder::default();
    c.hostname("host".to_string())
        .wid("wid".to_string())
        .labels(vec!["foo".to_string(), "bar".to_string()]);
    c.register("never_called", |_| -> io::Result<()> { unreachable!() });
    let c = c.connect_env(s.clone()).unwrap();
    let written = s.pop_bytes_written();
    assert!(written.starts_with(b"HELLO {"));
    let written: serde_json::Value = serde_json::from_slice(&written[b"HELLO ".len()..]).unwrap();
    let written = written.as_object().unwrap();
    assert_eq!(
        written.get("hostname").and_then(|h| h.as_str()),
        Some("host")
    );
    assert_eq!(written.get("wid").and_then(|h| h.as_str()), Some("wid"));
    assert_eq!(written.get("pid").map(|h| h.is_number()), Some(true));
    let labels = written["labels"].as_array().unwrap();
    assert_eq!(labels, &["foo", "bar"]);

    drop(c);
    let written = s.pop_bytes_written();
    assert_eq!(written, b"END\r\n");
}

#[test]
fn dequeue() {
    let mut s = mock::Stream::default();
    s.hello();
    let mut c = ConsumerBuilder::default();
    c.register("foobar", |job| -> io::Result<()> {
        assert_eq!(job.args(), &["z"]);
        Ok(())
    });
    let mut c = c.connect_env(s.clone()).unwrap();
    s.ignore();


    s.push_bytes_to_read(
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
    s.ok(); // for the ACK
    if let Err(e) = c.run_one(0, &["default"]) {
        println!("{:?}", e);
        unreachable!();
    }

    let written = s.pop_bytes_written();
    assert_eq!(
        written,
        &b"FETCH default\r\n\
        ACK {\"jid\":\"foojid\"}\r\n"[..]
    );
}

#[test]
fn dequeue_first_empty() {
    let mut s = mock::Stream::default();
    s.hello();
    let mut c = ConsumerBuilder::default();
    c.register("foobar", |job| -> io::Result<()> {
        assert_eq!(job.args(), &["z"]);
        Ok(())
    });
    let mut c = c.connect_env(s.clone()).unwrap();
    s.ignore();


    s.push_bytes_to_read(
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
    s.ok(); // for the ACK

    // run once, shouldn't do anything
    match c.run_one(0, &["default"]) {
        Ok(did_work) => assert!(!did_work),
        Err(e) => {
            println!("{:?}", e);
            unreachable!();
        }
    }
    // run again, this time doing the job
    match c.run_one(0, &["default"]) {
        Ok(did_work) => assert!(did_work),
        Err(e) => {
            println!("{:?}", e);
            unreachable!();
        }
    }

    let written = s.pop_bytes_written();
    assert_eq!(
        written,
        &b"\
        FETCH default\r\n\
        FETCH default\r\n\
        ACK {\"jid\":\"foojid\"}\r\n\
        "[..]
    );
}

#[test]
fn well_behaved() {
    let mut s = mock::Stream::default();
    s.hello();
    let mut c = ConsumerBuilder::default();
    c.wid("wid".to_string());
    c.register("foobar", |_| -> io::Result<()> {
        // NOTE: this time needs to be so that it lands between the first heartbeat and the second
        thread::sleep(Duration::from_secs(7));
        Ok(())
    });
    let mut c = c.connect_env(s.clone()).unwrap();
    s.ignore();

    // push a job that'll take a while to run
    s.push_bytes_to_read(
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

    let jh = thread::spawn(move || c.run(&["default"]));

    // the running thread won't return for a while. the heartbeat thingy is going to eventually
    // send a heartbeat, and we want to respond to that with a "quiet" to make it not accept any
    // more jobs.
    s.push_bytes_to_read(b"+quiet\r\n");

    // eventually, the job is going to finish and send an ACK
    s.ok();

    // then, we want to send a terminate to tell the thread to exit
    s.push_bytes_to_read(b"+terminate\r\n");

    // at this point, c.run() should eventually return with Ok(0) indicating that it finished.
    assert_eq!(jh.join().unwrap().unwrap(), 0);

    // we should now have seen:
    //
    //  - ACK {"jid": "jid"}
    //  - BEAT {"wid": "wid"}
    //
    let written = s.pop_bytes_written();

    let msgs = "\
                FETCH default\r\n\
                BEAT {\"wid\":\"wid\"}\r\n\
                ACK {\"jid\":\"jid\"}\r\n\
                BEAT {\"wid\":\"wid\"}\r\n\
                END\r\n";
    use std;
    assert_eq!(std::str::from_utf8(&written[..]).unwrap(), msgs);
}

#[test]
fn no_first_job() {
    let mut s = mock::Stream::default();
    s.hello();
    let mut c = ConsumerBuilder::default();
    c.wid("wid".to_string());
    c.register("foobar", |_| -> io::Result<()> {
        // NOTE: this time needs to be so that it lands between the first heartbeat and the second
        thread::sleep(Duration::from_secs(7));
        Ok(())
    });
    let mut c = c.connect_env(s.clone()).unwrap();
    s.ignore();

    // push a job that'll take a while to run
    s.push_bytes_to_read(
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

    let jh = thread::spawn(move || c.run(&["default"]));

    // the running thread won't return for a while. the heartbeat thingy is going to eventually
    // send a heartbeat, and we want to respond to that with a "quiet" to make it not accept any
    // more jobs.
    s.push_bytes_to_read(b"+quiet\r\n");

    // eventually, the job is going to finish and send an ACK
    s.ok();

    // then, we want to send a terminate to tell the thread to exit
    s.push_bytes_to_read(b"+terminate\r\n");

    // at this point, c.run() should eventually return with Ok(0) indicating that it finished.
    assert_eq!(jh.join().unwrap().unwrap(), 0);

    // we should now have seen:
    //
    //  - ACK {"jid": "jid"}
    //  - BEAT {"wid": "wid"}
    //
    let written = s.pop_bytes_written();

    let msgs = "\
                FETCH default\r\n\
                FETCH default\r\n\
                BEAT {\"wid\":\"wid\"}\r\n\
                ACK {\"jid\":\"jid\"}\r\n\
                BEAT {\"wid\":\"wid\"}\r\n\
                END\r\n";
    use std;
    assert_eq!(std::str::from_utf8(&written[..]).unwrap(), msgs);
}

#[test]
fn well_behaved_many() {
    let mut s = mock::Stream::default();
    s.hello();
    let mut c = ConsumerBuilder::default();
    c.workers(2);
    c.wid("wid".to_string());
    c.register("foobar", |_| -> io::Result<()> {
        // NOTE: this time needs to be so that it lands between the first heartbeat and the second
        thread::sleep(Duration::from_secs(7));
        Ok(())
    });
    let mut c = c.connect_env(s.clone()).unwrap();
    s.ignore();

    // push two jobs that'll take a while to run
    // they should run in parallel (if they don't, we'd only see one ACK)
    for _ in 0..2 {
        // we don't use different jids because we don't want to have to fiddle with them being
        // ACKed in non-deterministic order. this has the unfortunate side-effect that
        // *technically* the implementation could have a bug where it acks the same job twice, and
        // we wouldn't detect it...
        s.push_bytes_to_read(
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

    let jh = thread::spawn(move || c.run(&["default"]));

    // the running thread won't return for a while. the heartbeat thingy is going to eventually
    // send a heartbeat, and we want to respond to that with a "quiet" to make it not accept any
    // more jobs.
    s.push_bytes_to_read(b"+quiet\r\n");

    // eventually, the jobs are going to finish and send an ACK
    s.ok();
    s.ok();

    // then, we want to send a terminate to tell the thread to exit
    s.push_bytes_to_read(b"+terminate\r\n");

    // at this point, c.run() should eventually return with Ok(0) indicating that it finished.
    assert_eq!(jh.join().unwrap().unwrap(), 0);

    let written = s.pop_bytes_written();
    let msgs = "\
                FETCH default\r\n\
                FETCH default\r\n\
                BEAT {\"wid\":\"wid\"}\r\n\
                ACK {\"jid\":\"jid\"}\r\n\
                ACK {\"jid\":\"jid\"}\r\n\
                BEAT {\"wid\":\"wid\"}\r\n\
                END\r\n";
    use std;
    assert_eq!(std::str::from_utf8(&written[..]).unwrap(), msgs);
}

#[test]
fn terminate() {
    let mut s = mock::Stream::default();
    s.hello();
    let mut c = ConsumerBuilder::default();
    c.wid("wid".to_string());
    c.register("foobar", |_| -> io::Result<()> {
        loop {
            thread::sleep(Duration::from_secs(5));
        }
    });
    let mut c = c.connect_env(s.clone()).unwrap();
    s.ignore();

    s.push_bytes_to_read(
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

    let jh = thread::spawn(move || c.run(&["default"]));

    // the running thread won't ever return, because the job never exits. the heartbeat thingy is
    // going to eventually send a heartbeat, and we want to respond to that with a "terminate"
    s.push_bytes_to_read(b"+terminate\r\n");

    // at this point, c.run() should immediately return with Ok(1) indicating that one job is still
    // running.
    assert_eq!(jh.join().unwrap().unwrap(), 1);

    // we should now have seen:
    //
    //  - BEAT {"wid": "wid"}
    //  - FAIL {"jid": "jid", ...}
    //
    let written = s.pop_bytes_written();

    let beat = b"FETCH default\r\nBEAT {\"wid\":\"wid\"}\r\nFAIL ";
    assert_eq!(&written[0..beat.len()], &beat[..]);
    assert!(written.ends_with(b"\r\nEND\r\n"));
    let written: serde_json::Value =
        serde_json::from_slice(&written[beat.len()..(written.len() - b"END\r\n".len())]).unwrap();
    assert_eq!(
        written
            .as_object()
            .and_then(|o| o.get("jid"))
            .and_then(|v| v.as_str()),
        Some("forever")
    );
}
