use std::io;
use proto::{Client, ClientOptions, Info, Job, Push, StreamConnector};
use serde_json;

/// `Producer` is used to enqueue new jobs that will in turn be processed by Faktory workers.
///
/// # Connecting to Faktory
///
/// To issue jobs, the `Producer` must first be connected to the Faktory server. Exactly how you do
/// that depends on your setup. In particular, you must provide a connection *type*; that is,
/// something that implements [`StreamConnector`](trait.StreamConnector.html), likely
/// [`TcpEstablisher`](struct.TcpEstablisher.html) or
/// [`TlsEstablisher`](struct.TlsEstablisher.html) (for unencrypted and encrypted connections
/// respectively).
///
/// You must then tell the `Producer` *where* to connect. This is done by supplying a connection
/// URL of the form:
///
/// ```text
/// protocol://[:password@]hostname[:port]
/// ```
///
/// Faktory suggests using the `FAKTORY_PROVIDER` and `FAKTORY_URL` environment variables (see
/// their docs for more information) with `localhost:7419` as the fallback default. If you want
/// this behavior, use [`Producer::connect_env`](struct.Producer.html#method.connect_env). If not,
/// you can supply the URL directly to [`Producer::connect`](struct.Producer.html#method.connect).
/// Both methods take a connection type as described above.
///
/// # Issuing jobs
///
/// Most of the lifetime of a `Producer` will be spent creating and enqueueing jobs for Faktory
/// workers. This is done by passing a [`Job`](struct.Job.html) to
/// [`Producer::enqueue`](struct.Producer.html#method.enqueue). The most important part of a `Job`
/// is its `kind`; this field dictates how workers will execute the job when they receive it. The
/// string provided here must match a handler registered on the worker using
/// [`ConsumerBuilder::register`](struct.ConsumerBuilder.html#method.register) (or the equivalent
/// handler registration method in workers written in other languages).
///
/// Since Faktory workers do not all need to be the same (you could have some written in Rust for
/// performance-critical tasks, some in Ruby for more webby tasks, etc.), it may be the case that a
/// given job can only be executed by some workers (e.g., if they job type is not registered at
/// others). To allow for this, Faktory includes a `labels` field with each job. Jobs will only be
/// sent to workers whose labels (see
/// [`ConsumerBuilder::labels`](struct.ConsumerBuilder.html#method.labels)) match those set in
/// `Job::labels`.
///
/// # Examples
///
/// Connecting to an unsecured Faktory server using environment variables
///
/// ```no_run
/// use faktory::{Producer, TcpEstablisher};
/// let p = Producer::connect_env(TcpEstablisher).unwrap();
/// ```
///
/// Connecting to a secured Faktory server using environment variables and default TLS options
///
/// ```no_run
/// use faktory::{Producer, TlsEstablisher, TcpEstablisher};
/// let p = Producer::connect_env(TlsEstablisher::<TcpEstablisher>::default()).unwrap();
/// ```
///
/// Issuing a job using a `Producer`
///
/// ```no_run
/// # use faktory::{Producer, TcpEstablisher};
/// # let mut p = Producer::connect_env(TcpEstablisher).unwrap();
/// use faktory::Job;
/// p.enqueue(Job::new("foobar", vec!["z"])).unwrap();
/// ```
///
/// Connecting to a secured Faktory server using a URL and custom TLS options
///
/// ```no_run
/// # extern crate native_tls;
/// # extern crate faktory;
/// # const CA_CERT: &'static [u8] = &[];
/// # fn main() {
/// use native_tls::{TlsConnector, Certificate};
/// use faktory::{Producer, TlsEstablisher, TcpEstablisher};
/// let mut tls = TlsConnector::builder().unwrap();
/// tls.add_root_certificate(Certificate::from_der(CA_CERT).unwrap()).unwrap();
/// let p = Producer::connect(
///     TlsEstablisher::<TcpEstablisher>::from(tls.build().unwrap()),
///     "tcp://:hunter2@example.com:42"
/// ).unwrap();
/// # }
/// ```
///
// TODO: provide way of inspecting status of job.
pub struct Producer<C: StreamConnector> {
    c: Client<C>,
}

impl<C: StreamConnector> Producer<C> {
    /// Connect to a Faktory server at the given URL.
    ///
    /// Port defaults to 7419 if not given.
    pub fn connect<U>(connector: C, url: U) -> io::Result<Producer<C>>
    where
        U: AsRef<str>,
    {
        Ok(Producer {
            c: Client::connect(connector, ClientOptions::default(), url.as_ref())?,
        })
    }

    /// Connect to a Faktory server using the standard environment variables.
    ///
    /// Will first read `FAKTORY_PROVIDER` to get the name of the environment variable to get the
    /// address from (defaults to `FAKTORY_URL`), and then read that environment variable to get
    /// the server address. If the latter environment variable is not defined, the connection will
    /// be made to
    ///
    /// ```text
    /// tcp://localhost:7419
    /// ```
    pub fn connect_env(connector: C) -> io::Result<Producer<C>> {
        Ok(Producer {
            c: Client::connect_env(connector, ClientOptions::default())?,
        })
    }

    /// Enqueue the given job on the Faktory server.
    ///
    /// Returns `Ok` if the job was successfully queued by the Faktory server.
    pub fn enqueue(&mut self, job: Job) -> io::Result<()> {
        self.c.issue(Push::from(job))?.await_ok()
    }

    /// Retrieve information about the running server.
    ///
    /// The returned value is the result of running the `INFO` command on the server.
    pub fn info(&mut self) -> io::Result<serde_json::Value> {
        self.c
            .issue(Info)
            .map_err(serde_json::Error::io)?
            .read_json()
            .map(|v| v.expect("info command cannot give empty response"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proto::TcpEstablisher;

    #[test]
    #[ignore]
    fn it_works() {
        let mut p = Producer::connect_env(TcpEstablisher).unwrap();
        p.enqueue(Job::new("foobar", vec!["z"])).unwrap();
    }
}
