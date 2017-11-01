use std::io::prelude::*;
use std::io;
use proto::{Client, ClientOptions, Info, Job, Push, StreamConnector};
use serde_json;

/// A `Producer` provides an interface to a Faktory work server that allows enqueuing new jobs.
///
/// ```no_run
/// use faktory::{Producer, Job, TcpEstablisher};
/// let mut p = Producer::default::<TcpEstablisher>().unwrap();
/// p.enqueue(Job::new("foobar", vec!["z"])).unwrap();
/// ```
// TODO: provide way of inspecting status of job.
pub struct Producer<S: Read + Write> {
    c: Client<S>,
}

impl<S: Read + Write + 'static> Producer<S> {
    /// Construct a new consumer with default consumer options and the url fetched from environment
    /// variables.
    ///
    /// This will construct a worker where:
    ///
    ///  - `hostname` is this machine's hostname.
    ///  - `wid` is a randomly generated string.
    ///  - `pid` is the OS PID of this process.
    ///  - `labels` is `["rust"]`.
    ///
    pub fn default<C: StreamConnector<Stream = S> + Default>() -> io::Result<Producer<S>> {
        Self::connect_env(C::default())
    }

    /// Connect to an unsecured Faktory server.
    ///
    /// The url is in standard URL form:
    ///
    /// ```text
    /// tcp://[:password@]hostname[:port]
    /// ```
    ///
    /// Port defaults to 7419 if not given.
    pub fn connect<U, C>(connector: C, url: U) -> io::Result<Producer<S>>
    where
        U: AsRef<str>,
        C: StreamConnector<Stream = S>,
    {
        Ok(Producer {
            c: Client::connect(connector, ClientOptions::default(), url.as_ref())?,
        })
    }

    /// Connect to an unsecured Faktory server using the standard environment variables.
    ///
    /// Will first read `FAKTORY_PROVIDER` to get the name of the environment variable to get the
    /// address from (defaults to `FAKTORY_URL`), and then read that environment variable to get
    /// the server address. If the latter environment variable is not defined, the url defaults to:
    ///
    /// ```text
    /// tcp://localhost:7419
    /// ```
    pub fn connect_env<C>(connector: C) -> io::Result<Producer<S>>
    where
        C: StreamConnector<Stream = S>,
    {
        Ok(Producer {
            c: Client::connect_env(connector, ClientOptions::default())?,
        })
    }
}

impl<S: Read + Write> Producer<S> {
    /// Enqueue the given job on the Faktory server.
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proto::TcpEstablisher;

    #[test]
    #[ignore]
    fn it_works() {
        let mut p = Producer::default::<TcpEstablisher>().unwrap();
        p.enqueue(Job::new("foobar", vec!["z"])).unwrap();
    }
}
