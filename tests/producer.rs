extern crate url;
extern crate faktory;
extern crate mockstream;
extern crate serde_json;

mod mock;

use faktory::*;

#[test]
fn hello() {
    let mut s = mock::Stream::default();
    s.hello();

    let p = Producer::connect_env(s.clone()).unwrap();
    let written = s.pop_bytes_written();
    assert!(written.starts_with(b"HELLO {"));
    let written: serde_json::Value = serde_json::from_slice(&written[b"HELLO ".len()..]).unwrap();
    let written = written.as_object().unwrap();
    assert_eq!(written.get("hostname").map(|h| h.is_string()), Some(true));
    assert_eq!(written.get("wid").map(|h| h.is_string()), Some(true));
    assert_eq!(written.get("pid").map(|h| h.is_number()), Some(true));
    let labels = written["labels"].as_array().unwrap();
    assert_eq!(labels, &["rust"]);

    drop(p);
    let written = s.pop_bytes_written();
    assert_eq!(written, b"END\r\n");
}

#[test]
fn enqueue() {
    let mut s = mock::Stream::default();
    s.hello();
    let mut p = Producer::connect_env(s.clone()).unwrap();
    s.ignore();

    s.ok();
    p.enqueue(Job::new("foobar", vec!["z"])).unwrap();

    let written = s.pop_bytes_written();
    assert!(written.starts_with(b"PUSH {"));
    let written: serde_json::Value = serde_json::from_slice(&written[b"PUSH ".len()..]).unwrap();
    let written = written.as_object().unwrap();
    assert_eq!(written.get("jid").map(|h| h.is_string()), Some(true));
    assert_eq!(
        written.get("queue").and_then(|h| h.as_str()),
        Some("default")
    );
    assert_eq!(
        written.get("jobtype").and_then(|h| h.as_str()),
        Some("foobar")
    );
    assert_eq!(
        written
            .get("args")
            .and_then(|h| h.as_array())
            .and_then(|a| {
                assert_eq!(a.len(), 1);
                a[0].as_str()
            }),
        Some("z")
    );
    assert_eq!(
        written.get("reserve_for").and_then(|h| h.as_u64()),
        Some(600)
    );
    assert_eq!(written.get("retry").and_then(|h| h.as_u64()), Some(25));
    assert_eq!(written.get("backtrace").and_then(|h| h.as_u64()), Some(0));
}
