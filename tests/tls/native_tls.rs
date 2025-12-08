use faktory::native_tls::TlsStream;
use faktory::{Client, Job, Worker, WorkerId};
use serde_json::Value;
use std::{env, sync};
use tokio::io::BufStream;
use url::Url;

#[tokio::test(flavor = "multi_thread")]
async fn roundtrip_tls() {
    use tokio_native_tls::native_tls::TlsConnector;

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
    let tls = || async {
        let connector = TlsConnector::builder()
            .danger_accept_invalid_certs(true)
            .build()
            .unwrap();
        let stream =
            TlsStream::with_connector(connector, Some(&env::var("FAKTORY_URL_SECURE").unwrap()))
                .await
                .unwrap();
        BufStream::new(stream)
    };

    let password = Url::parse(&env::var("FAKTORY_URL_SECURE").expect("faktory url to be set..."))
        .expect("...and be valid")
        .password()
        .map(|p| p.to_string());

    let mut worker = Worker::builder()
        .hostname("tester".to_string())
        .wid(WorkerId::new(local))
        .register(local, fixtures::JobHandler::new(tx))
        .connect_with(tls().await, password.clone())
        .await
        .unwrap();

    // "one-shot" client
    Client::connect_with(tls().await, password)
        .await
        .unwrap()
        .enqueue(Job::new(local, vec!["z"]).on_queue(local))
        .await
        .unwrap();

    worker.run_one(0, &[local]).await.unwrap();

    let job = rx.recv().unwrap();
    assert_eq!(job.queue, local);
    assert_eq!(job.kind(), local);
    assert_eq!(job.args(), &[Value::from("z")]);
}

mod fixtures {
    pub use handler::JobHandler;

    mod handler {
        use async_trait::async_trait;
        use faktory::{Job, JobRunner};
        use std::{
            io,
            sync::{mpsc::Sender, Arc, Mutex},
            time::Duration,
        };
        use tokio::time;

        pub struct JobHandler {
            chan: Arc<Mutex<Sender<Job>>>,
        }

        impl JobHandler {
            pub fn new(chan: Sender<Job>) -> Self {
                Self {
                    chan: Arc::new(Mutex::new(chan)),
                }
            }

            async fn process_one(&self, job: Job) -> io::Result<()> {
                time::sleep(Duration::from_millis(100)).await;
                eprintln!("{job:?}");
                self.chan.lock().unwrap().send(job).unwrap();
                Ok(())
            }
        }

        #[async_trait]
        impl JobRunner for JobHandler {
            type Error = io::Error;

            async fn run(&self, job: Job) -> Result<(), Self::Error> {
                self.process_one(job).await.unwrap();
                Ok(())
            }
        }
    }
}
