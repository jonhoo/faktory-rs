extern crate faktory;
extern crate serde_json;
extern crate url;

mod mock;

use faktory::*;

#[tokio::test(flavor = "multi_thread")]
async fn hello() {
    let mut s = mock::Stream::default();

    let p = Client::connect_with(s.clone(), None).await.unwrap();
    let written = s.pop_bytes_written(0);
    eprintln!("{:?}", String::from_utf8(written.clone()).unwrap());
    assert!(written.starts_with(b"HELLO {"));
    let written: serde_json::Value = serde_json::from_slice(&written[b"HELLO ".len()..]).unwrap();
    let written = written.as_object().unwrap();
    assert_eq!(written.get("hostname"), None);
    assert_eq!(written.get("wid"), None);
    assert_eq!(written.get("pid"), None);
    assert_eq!(written.get("labels"), None);
    assert_eq!(written.get("v").and_then(|h| h.as_i64()), Some(2));

    drop(p);
    let written = s.pop_bytes_written(0);
    assert_eq!(written, b"END\r\n");
}

#[tokio::test(flavor = "multi_thread")]
async fn hello_pwd() {
    let mut s = mock::Stream::with_salt(1545, "55104dc76695721d");

    let c = Client::connect_with(s.clone(), Some("foobar".to_string()))
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
async fn enqueue() {
    let mut s = mock::Stream::default();
    let mut p = Client::connect_with(s.clone(), None).await.unwrap();
    s.ignore(0);

    s.ok(0);
    p.enqueue(Job::new("foobar", vec!["z"])).await.unwrap();

    let written = s.pop_bytes_written(0);
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
    assert_eq!(written.get("priority").and_then(|h| h.as_u64()), Some(5));
    assert_eq!(written.get("backtrace").and_then(|h| h.as_u64()), Some(0));
}

#[tokio::test(flavor = "multi_thread")]
async fn queue_control() {
    let mut s = mock::Stream::default();
    let mut p = Client::connect_with(s.clone(), None).await.unwrap();
    s.ignore(0);

    s.ok(0);
    p.queue_pause(&["test", "test2"]).await.unwrap();

    s.ok(0);
    p.queue_resume(&["test3".to_string(), "test4".to_string()])
        .await
        .unwrap();

    let written = s.pop_bytes_written(0);
    assert!(written == b"QUEUE PAUSE test test2\r\nQUEUE RESUME test3 test4\r\n");
}
