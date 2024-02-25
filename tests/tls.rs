#![cfg(feature = "tls")]

use faktory::*;
use serde_json::Value;
use std::{
    env,
    sync::{self, Arc},
};
use tokio_rustls::rustls::ClientConfig;
use tokio_rustls::rustls::SignatureScheme;

#[tokio::test(flavor = "multi_thread")]
async fn roundtrip_tls() {
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
    let mut c = ConsumerBuilder::default();

    c.hostname("tester".to_string()).wid(local.to_string());
    c.register_runner(local, fixtures::JobHandler::new(tx));

    let tls = || async {
        let verifier = fixtures::TestServerCertVerifier::new(
            SignatureScheme::RSA_PSS_SHA512,
            env::current_dir()
                .unwrap()
                .join("docker")
                .join("certs")
                .join("faktory.local.crt"),
        );
        let client_config = ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(verifier))
            .with_no_client_auth();

        TlsStream::with_client_config(
            client_config,
            Some(&env::var("FAKTORY_URL_SECURE").unwrap()),
        )
        .await
        .unwrap()
    };

    let mut c = c.connect_with(tls().await, None).await.unwrap();
    let mut p = Client::connect_with(tls().await, None).await.unwrap();
    p.enqueue(Job::new(local, vec!["z"]).on_queue(local))
        .await
        .unwrap();
    c.run_one(0, &[local]).await.unwrap();

    let job = rx.recv().unwrap();
    assert_eq!(job.queue, local);
    assert_eq!(job.kind(), local);
    assert_eq!(job.args(), &[Value::from("z")]);
}

mod fixtures {
    pub use handler::JobHandler;
    pub use tls::TestServerCertVerifier;

    mod handler {
        use faktory::*;

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
                eprintln!("{:?}", job);
                self.chan.lock().unwrap().send(job).unwrap();
                Ok(())
            }
        }

        #[async_trait::async_trait]
        impl JobRunner for JobHandler {
            type Error = io::Error;

            async fn run(&self, job: Job) -> Result<(), Self::Error> {
                self.process_one(job).await.unwrap();
                Ok(())
            }
        }
    }

    mod tls {
        #![allow(unused_variables)]

        use std::fs;
        use std::path::PathBuf;

        use tokio_rustls::rustls::client::danger::{
            HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier,
        };
        use tokio_rustls::rustls::pki_types::{CertificateDer, ServerName, UnixTime};
        use tokio_rustls::rustls::DigitallySignedStruct;
        use tokio_rustls::rustls::Error as RustlsError;
        use tokio_rustls::rustls::SignatureScheme;
        use x509_parser::pem::parse_x509_pem;

        #[derive(Debug)]
        pub struct TestServerCertVerifier<'a> {
            scheme: SignatureScheme,
            cert_der: CertificateDer<'a>,
        }

        impl TestServerCertVerifier<'_> {
            pub fn new(scheme: SignatureScheme, cert_path: PathBuf) -> Self {
                let cert = fs::read(&cert_path).unwrap();
                let (_, pem) = parse_x509_pem(&cert).unwrap();
                let cert_der = CertificateDer::try_from(pem.contents).unwrap();
                Self { scheme, cert_der }
            }
        }

        impl ServerCertVerifier for TestServerCertVerifier<'_> {
            fn verify_server_cert(
                &self,
                end_entity: &CertificateDer<'_>,
                intermediates: &[CertificateDer<'_>],
                server_name: &ServerName<'_>,
                ocsp_response: &[u8],
                now: UnixTime,
            ) -> Result<ServerCertVerified, RustlsError> {
                assert_eq!(&self.cert_der, end_entity);
                Ok(ServerCertVerified::assertion())
            }

            fn verify_tls12_signature(
                &self,
                message: &[u8],
                cert: &CertificateDer<'_>,
                dss: &DigitallySignedStruct,
            ) -> Result<HandshakeSignatureValid, RustlsError> {
                Ok(HandshakeSignatureValid::assertion())
            }

            fn verify_tls13_signature(
                &self,
                message: &[u8],
                cert: &CertificateDer<'_>,
                dss: &DigitallySignedStruct,
            ) -> Result<HandshakeSignatureValid, RustlsError> {
                Ok(HandshakeSignatureValid::assertion())
            }

            fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
                vec![self.scheme]
            }
        }
    }
}
