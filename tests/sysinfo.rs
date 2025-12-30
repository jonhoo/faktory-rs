#![cfg(feature = "sysinfo")]

mod mock;

use faktory::{StopReason, WorkerBuilder, WorkerId};
use std::{io, time::Duration};
use tokio::io::BufStream;

#[tokio::test(flavor = "multi_thread")]
async fn well_behaved() {
    // let's first verify that collecting and sending mem stats
    // to Faktory is _not_ enabled by default
    {
        let s = mock::Stream::new_unchecked(2);
        let w = WorkerBuilder::default()
            .wid(WorkerId::new("wid_without_sysinfo"))
            .register_fn("foobar", |_| async move { Ok::<(), io::Error>(()) })
            .connect_with(BufStream::new(s.clone()), None)
            .await
            .unwrap();
        assert!(!w.is_sysinfo_enabled());
    }

    // let's now construct a worker with sysinfo enabled
    let mut s = mock::Stream::new(2);
    let mut w = WorkerBuilder::default()
        .wid(WorkerId::new("wid"))
        .register_fn("foobar", |_| async move {
            // NOTE: this time needs to be so that it lands between the first heartbeat and the second
            tokio::time::sleep(Duration::from_secs(7)).await;
            Ok::<(), io::Error>(())
        })
        .with_sysinfo()
        .connect_with(BufStream::new(s.clone()), None)
        .await
        .unwrap();
    assert!(w.is_sysinfo_enabled()); // NB

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

    let jh = tokio::spawn(async move { w.run(&["default"]).await });

    // the running thread won't return for a while. the heartbeat thingy is going to eventually
    // send a heartbeat, and we want to respond to that with a "quiet" to make it not accept any
    // more jobs.
    s.push_bytes_to_read(0, b"+{\"state\":\"quiet\"}\r\n");

    // eventually, the job is going to finish and send an ACK
    s.ok(1);

    // then, we want to send a terminate to tell the thread to exit
    s.push_bytes_to_read(0, b"+{\"state\":\"terminate\"}\r\n");

    // at this point, c.run() should eventually return with Ok(0) indicating that it finished.
    let details = jh.await.unwrap().unwrap();
    assert_eq!(details.reason, StopReason::ServerInstruction);
    assert_eq!(details.workers_still_running, 0);

    // heartbeat should have seen two beats (quiet + terminate)
    let written = s.pop_bytes_written(0);
    let written = std::str::from_utf8(&written[..]).unwrap();
    // the "written" output will be of the folowing shape (example from one of testruns):
    //
    // "BEAT {\"wid\":\"wid\",\"rss_kb\":7320}\r\nBEAT {\"wid\":\"wid\",\"rss_kb\":7576}\r\nEND\r\n"
    //
    // we cannot assert on the exact value of "rss_kb" without extra test tricks (mocking,
    // intercepting or maybe spying on the worker), but we should be good to go if we just
    // check that the number of commands and their types are the same (two BEATs and one END)
    // as in the non-sysinfo branch and that the BEATs contain those hard-coded wids and an
    // extra "rss_kb" field (note that this field is not there for non-sysinfo, cause it's
    // value is None and it gets skipped during the hearbeat's serialization)
    let cmds: Vec<_> = written.splitn(3, "\r\n").collect();
    let first_beat: serde_json::Value =
        serde_json::from_str(cmds[0].strip_prefix("BEAT ").unwrap()).unwrap();
    assert_eq!(first_beat["wid"], "wid");
    assert!(first_beat["rss_kb"].is_number()); // NB
    assert!(first_beat["rss_kb"].as_u64().unwrap() > 0);
    let second_beat: serde_json::Value =
        serde_json::from_str(cmds[1].strip_prefix("BEAT ").unwrap()).unwrap();
    assert_eq!(second_beat["wid"], "wid");
    assert!(first_beat["rss_kb"].is_number());
    assert_eq!(cmds[2], "END\r\n");

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
