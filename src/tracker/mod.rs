use std::io::{Read, Write};
use std::net::TcpStream;

use crate::proto::{host_from_url, parse_provided_or_from_env, Client, GetBatchStatus, Track};
use crate::{BatchStatus, Error, Progress, ProgressUpdate};

/// Used for retrieving and updating information on a job's execution progress
/// (see [`Progress`] and [`ProgressUpdate`]), as well for retrieving a batch's status
/// from the Faktory server (see [`BatchStatus`]).
///
/// Fetching a job's execution progress:
/// ```no_run
/// use faktory::Tracker;
/// let job_id = String::from("W8qyVle9vXzUWQOf");
/// let mut tracker = Tracker::connect(None)?;
/// if let Some(progress) = tracker.get_progress(job_id)? {
///     if progress.state == "success" {
///       # /*
///         ...
///       # */
///     }
/// }
/// # Ok::<(), faktory::Error>(())
/// ```
/// Sending an update on a job's execution progress:
/// ```no_run
/// use faktory::{Tracker, ProgressUpdateBuilder};
/// let jid = String::from("W8qyVle9vXzUWQOf");
/// let mut tracker = Tracker::connect(None)?;
/// let progress = ProgressUpdateBuilder::new(&jid)
///     .desc("Almost done...".to_owned())
///     .percent(99)
///     .build();
/// tracker.set_progress(progress)?;
/// # Ok::<(), faktory::Error>(())
///````
/// Fetching a batch's status:
/// ```no_run
/// use faktory::Tracker;
/// let bid = String::from("W8qyVle9vXzUWQOg");
/// let mut tracker = Tracker::connect(None)?;
/// if let Some(status) = tracker.get_batch_status(bid)? {
///     println!("This batch created at {}", status.created_at);
/// }
/// # Ok::<(), faktory::Error>(())
/// ```
pub struct Tracker<S: Read + Write> {
    c: Client<S>,
}

impl Tracker<TcpStream> {
    /// Create new tracker and connect to a Faktory server.
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
    pub fn connect(url: Option<&str>) -> Result<Tracker<TcpStream>, Error> {
        let url = parse_provided_or_from_env(url)?;
        let addr = host_from_url(&url);
        let pwd = url.password().map(Into::into);
        let stream = TcpStream::connect(addr)?;
        Ok(Tracker {
            c: Client::new_tracker(stream, pwd)?,
        })
    }
}

impl<S: Read + Write> Tracker<S> {
    /// Connect to a Faktory server with a non-standard stream.
    pub fn connect_with(stream: S, pwd: Option<String>) -> Result<Tracker<S>, Error> {
        Ok(Tracker {
            c: Client::new_tracker(stream, pwd)?,
        })
    }

    /// Send information on a job's execution progress to Faktory.
    pub fn set_progress(&mut self, upd: ProgressUpdate) -> Result<(), Error> {
        let cmd = Track::Set(upd);
        self.c.issue(&cmd)?.await_ok()
    }

    /// Fetch information on a job's execution progress from Faktory.
    pub fn get_progress(&mut self, jid: String) -> Result<Option<Progress>, Error> {
        let cmd = Track::Get(jid);
        self.c.issue(&cmd)?.read_json()
    }

    /// Fetch information on a batch of jobs execution progress.
    pub fn get_batch_status(&mut self, bid: String) -> Result<Option<BatchStatus>, Error> {
        let cmd = GetBatchStatus::from(bid);
        self.c.issue(&cmd)?.read_json()
    }
}

#[cfg(test)]
mod test {
    use crate::proto::{host_from_url, parse_provided_or_from_env};

    use super::Tracker;
    use std::{env, net::TcpStream};

    #[test]
    fn test_trackers_created_ok() {
        if env::var_os("FAKTORY_URL").is_none() || env::var_os("FAKTORY_ENT").is_none() {
            return;
        }
        let _ = Tracker::connect(None).expect("tracker successfully instantiated and connected");

        let url = parse_provided_or_from_env(None).expect("valid url");
        let host = host_from_url(&url);
        let stream = TcpStream::connect(host).expect("connected");
        let pwd = url.password().map(String::from);
        let _ = Tracker::connect_with(stream, pwd)
            .expect("tracker successfully instantiated and connected");
    }
}
