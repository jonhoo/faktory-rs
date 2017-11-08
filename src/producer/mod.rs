use std::io::prelude::*;
use std::io;
use std::net::TcpStream;
use proto::{self, Client, Info, Job, Push};
use serde_json;

/// `Producer` is used to enqueue new jobs that will in turn be processed by Faktory workers.
///
/// # Connecting to Faktory
///
/// To issue jobs, the `Producer` must first be connected to the Faktory server. Exactly how you do
/// that depends on your setup. In most cases, you'll want to use `Producer::connect`, and provide
/// a connection URL (`None` will use the Faktory environment variables). If you supply a URL, it
/// must be of the form:
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
/// use faktory::Producer;
/// let p = Producer::connect(None).unwrap();
/// ```
///
/// Connecting to a secured Faktory server using an explicit URL
///
/// ```no_run
/// use faktory::Producer;
/// let p = Producer::connect(Some("tcp://:hunter2@localhost:7439")).unwrap();
/// ```
///
/// Issuing a job using a `Producer`
///
/// ```no_run
/// # use faktory::Producer;
/// # let mut p = Producer::connect(None).unwrap();
/// use faktory::Job;
/// p.enqueue(Job::new("foobar", vec!["z"])).unwrap();
/// ```
///
// TODO: provide way of inspecting status of job.
pub struct Producer<S: Read + Write> {
    c: Client<S>,
}

impl Producer<TcpStream> {
    /// Connect to a Faktory server.
    ///
    /// If `url` is not given, will use the standard Faktory environment variables. Specifically,
    /// `FAKTORY_PROVIDER` is read to get the name of the environment variable to get the address
    /// from (defaults to `FAKTORY_URL`), and then that environment variable is read to get the
    /// server address. If the latter environment variable is not defined, the connection will be
    /// made to
    ///
    /// ```text
    /// tcp://localhost:7419
    /// ```
    ///
    /// If `url` is given, but does not specify a port, it defaults to 7419.
    pub fn connect(url: Option<&str>) -> io::Result<Self> {
        let url = match url {
            Some(url) => proto::url_parse(url),
            None => proto::url_parse(&proto::get_env_url()),
        }?;
        let stream = TcpStream::connect(proto::host_from_url(&url))?;
        Self::connect_with(stream, url.password().map(|p| p.to_string()))
    }
}

impl<S: Read + Write> Producer<S> {
    /// Connect to a Faktory server with a non-standard stream.
    pub fn connect_with(stream: S, pwd: Option<String>) -> io::Result<Producer<S>> {
        Ok(Producer {
            c: Client::new_producer(stream, pwd)?,
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

    #[test]
    #[ignore]
    fn it_works() {
        let mut p = Producer::connect(None).unwrap();
        p.enqueue(Job::new("foobar", vec!["z"])).unwrap();
    }
}
