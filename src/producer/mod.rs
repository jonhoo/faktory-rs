use std::io::prelude::*;
use std::io;
use proto::{Client, ClientOptions, Info, Job, Push, StreamConnector};
use serde_json;

/// A `Producer` provides an interface to a Faktory work server that allows enqueuing new jobs.
///
/// ```no_run
/// # use faktory::{Producer, Job};
/// use std::net::TcpStream;
/// let mut p = Producer::connect_env::<TcpStream>().unwrap();
/// p.enqueue(Job::new("foobar", vec!["z"])).unwrap();
/// ```
// TODO: provide way of inspecting status of job.
pub struct Producer<S: Read + Write> {
    c: Client<S>,
}

impl<S: Read + Write + 'static> Producer<S> {
    /// Connect to an unsecured Faktory server.
    ///
    /// The url is in standard URL form:
    ///
    /// ```text
    /// tcp://[:password@]hostname[:port]
    /// ```
    ///
    /// Port defaults to 7419 if not given.
    pub fn connect<U, C>(url: U) -> io::Result<Producer<S>>
    where
        U: AsRef<str>,
        C: StreamConnector<Stream = S>,
    {
        Ok(Producer {
            c: Client::connect::<C>(ClientOptions::default(), url.as_ref())?,
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
    pub fn connect_env<C>() -> io::Result<Producer<S>>
    where
        C: StreamConnector<Stream = S>,
    {
        Ok(Producer {
            c: Client::connect_env::<C>(ClientOptions::default())?,
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

    #[test]
    #[ignore]
    fn it_works() {
        use std::net::TcpStream;

        let mut p = Producer::connect_env::<TcpStream>().unwrap();
        p.enqueue(Job::new("foobar", vec!["z"])).unwrap();
    }
}
