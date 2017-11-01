extern crate faktory;
extern crate mockstream;
extern crate serde_json;

mod mock;

use faktory::*;
use std::io;

#[test]
fn hello() {
    let mut s = mock::Stream::default();
    s.hello();

    let mut c = ConsumerBuilder::default();
    c.hostname("host".to_string())
        .wid("wid".to_string())
        .labels(vec!["foo".to_string(), "bar".to_string()]);
    let mut c = c.connect_env(s.clone()).unwrap();
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

    c.register("never_called", |_| -> io::Result<()> { unreachable!() });

    drop(c);
    let written = s.pop_bytes_written();
    assert_eq!(written, b"END\r\n");
}

#[test]
fn dequeue() {
    let mut s = mock::Stream::default();
    s.hello();
    let mut c = ConsumerBuilder::default().connect_env(s.clone()).unwrap();
    s.ignore();

    c.register("foobar", |job| -> io::Result<()> {
        assert_eq!(job.args, vec!["z"]);
        Ok(())
    });

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
    if let Err(e) = c.run_one(&["default"]) {
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
