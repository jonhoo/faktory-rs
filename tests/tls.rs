#![cfg(feature = "tls")]

use faktory::*;
use serde_json::Value;
use std::{env, fs, io, sync};

#[tokio::test(flavor = "multi_thread")]
async fn roundtrip_tls() {
    use tokio_rustls::rustls::{ClientConfig, RootCertStore};

    // We are utilizing the fact that the "FAKTORY_URL_SECURE" environment variable is set
    // as an indicator that the integration test can and should be performed.
    //
    // In case the variable is not set we are returning early. This will show `test <test name> ... ok`
    // in the test run output, which is admittedly confusing. Ideally, we would like to be able to decorate
    // a test with a macro and to see something like `test <test name> ... skipped due to <reason>`, in case
    // the test has been skipped, but it is currently not "natively" supported.
    //
    // See: https://github.com/rust-lang/rust/issues/68007
    if env::var_os("FAKTORY_URL_SECURE").is_none() {
        return;
    }

    let local = "roundtrip_tls";

    let (tx, rx) = sync::mpsc::channel();
    let tx = sync::Arc::new(sync::Mutex::new(tx));
    let mut c = ConsumerBuilder::default();
    c.hostname("tester".to_string()).wid(local.to_string());
    {
        c.register(local, move |j| {
            let tx = sync::Arc::clone(&tx);
            Box::pin(async move {
                tx.lock().unwrap().send(j).unwrap();
                Ok::<(), io::Error>(())
            })
        });
    }

    let cert_path = env::current_dir()
        .unwrap()
        .join("docker")
        .join("certs")
        .join("faktory.local.crt");
    let cert = fs::read(cert_path).unwrap();

    let tls = || async {
        // let connector = if cfg!(target_os = "macos") {
        //     TlsConnector::builder()
        //         // Danger! Only for testing!
        //         // On the macos CI runner, the certs are not trusted:
        //         // { code: -67843, message: "The certificate was not trusted." }
        //         .danger_accept_invalid_certs(true)
        //         .build()
        //         .unwrap()
        // } else {
        //     let cert = Certificate::from_pem(cert.as_bytes()).unwrap();
        //     TlsConnector::builder()
        //         .add_root_certificate(cert)
        //         .build()
        //         .unwrap()
        // };
        let mut store = RootCertStore::empty();
        store.add(cert.clone().into()).unwrap();
        let conf = ClientConfig::builder()
            .with_root_certificates(store)
            .with_no_client_auth();
        TlsStream::with_client_config(conf, Some(&env::var("FAKTORY_URL_SECURE").unwrap()))
            .await
            .unwrap()
    };
    let mut c = c.connect_with(tls().await, None).await.unwrap();
    let mut p = Producer::connect_with(tls().await, None).await.unwrap();
    p.enqueue(Job::new(local, vec!["z"]).on_queue(local))
        .await
        .unwrap();
    c.run_one(0, &[local]).await.unwrap();

    let job = rx.recv().unwrap();
    assert_eq!(job.queue, local);
    assert_eq!(job.kind(), local);
    assert_eq!(job.args(), &[Value::from("z")]);
}
