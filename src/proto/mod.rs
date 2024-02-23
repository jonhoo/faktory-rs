#[cfg(doc)]
use crate::{Consumer, Producer};

use crate::error::{self, Error};
use std::io;
use tokio::io::BufStream;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream as TokioStream;

mod single;
pub use single::{Ack, Fail, Info, Job, JobBuilder, Push, PushBulk, QueueAction, QueueControl};
pub(crate) mod utils;

#[cfg(feature = "ent")]
pub use self::single::ent::{JobState, Progress, ProgressUpdate, ProgressUpdateBuilder, Track};
use self::single::Heartbeat;

#[cfg(feature = "ent")]
mod batch;
#[cfg(feature = "ent")]
pub use batch::{
    Batch, BatchBuilder, BatchHandle, BatchStatus, CallbackState, CommitBatch, GetBatchStatus,
    OpenBatch,
};

pub(crate) const EXPECTED_PROTOCOL_VERSION: usize = 2;

fn check_protocols_match(ver: usize) -> Result<(), Error> {
    if ver != EXPECTED_PROTOCOL_VERSION {
        return Err(error::Connect::VersionMismatch {
            ours: EXPECTED_PROTOCOL_VERSION,
            theirs: ver,
        }
        .into());
    }
    Ok(())
}

#[derive(Clone)]
pub(crate) struct ClientOptions {
    /// Hostname to advertise to server.
    /// Defaults to machine hostname.
    pub(crate) hostname: Option<String>,

    /// PID to advertise to server.
    /// Defaults to process ID.
    pub(crate) pid: Option<usize>,

    /// Worker ID to advertise to server.
    /// Defaults to a GUID.
    pub(crate) wid: Option<String>,

    /// Labels to advertise to se/// A stream that can be re-established after failing.rver.
    /// Defaults to ["rust"].
    pub(crate) labels: Vec<String>,

    /// Password to authenticate with
    /// Defaults to None.
    pub(crate) password: Option<String>,

    /// Whether this client is instatianted for
    /// a consumer ("worker" in Faktory terms).
    pub(crate) is_worker: bool,
}

impl Default for ClientOptions {
    fn default() -> Self {
        ClientOptions {
            hostname: None,
            pid: None,
            wid: None,
            labels: vec!["rust".to_string()],
            password: None,
            is_worker: false,
        }
    }
}

/// A stream that can be re-established after failing.
#[async_trait::async_trait]
pub trait Reconnect: Sized {
    /// Re-establish the stream.
    async fn reconnect(&mut self) -> io::Result<Self>;
}

#[async_trait::async_trait]
impl Reconnect for TokioStream {
    async fn reconnect(&mut self) -> io::Result<Self> {
        let addr = &self.peer_addr().expect("socket address");
        TokioStream::connect(addr).await
    }
}

#[async_trait::async_trait]
impl<S> Reconnect for BufStream<S>
where
    S: AsyncRead + AsyncWrite + Reconnect + Send + Sync,
{
    async fn reconnect(&mut self) -> io::Result<Self> {
        // let addr = &self.get_ref().peer_addr().expect("socket address");
        let stream = self.get_mut().reconnect().await?;
        Ok(Self::new(stream))
    }
}

/// A Faktory connection that represents neither a [`Producer`] nor a [`Consumer`].
///
/// Useful for retrieving and updating information on a job's execution progress
/// (see [`Progress`] and [`ProgressUpdate`]), as well for retrieving a batch's status
/// from the Faktory server (see [`BatchStatus`]).
///
/// Fetching a job's execution progress:
/// ```no_run
/// # tokio_test::block_on(async {
/// use faktory::{Client, ent::JobState};
/// let job_id = String::from("W8qyVle9vXzUWQOf");
/// let mut cl = Client::connect(None).await?;
/// if let Some(progress) = cl.get_progress(job_id).await? {
///     if let JobState::Success = progress.state {
///         # /*
///         ...
///         # */
///     }
/// }
/// # Ok::<(), faktory::Error>(())
/// });
/// ```
///
/// Sending an update on a job's execution progress:
///
/// ```no_run
/// # tokio_test::block_on(async {
/// use faktory::{Client, ent::ProgressUpdateBuilder};
/// let jid = String::from("W8qyVle9vXzUWQOf");
/// let mut cl = Client::connect(None).await?;
/// let progress = ProgressUpdateBuilder::new(&jid)
///     .desc("Almost done...".to_owned())
///     .percent(99)
///     .build();
/// cl.set_progress(progress).await?;
/// # Ok::<(), faktory::Error>(())
/// });
///````
///
/// Fetching a batch's status:
///
/// ```no_run
/// # tokio_test::block_on(async {
/// use faktory::Client;
/// let bid = String::from("W8qyVle9vXzUWQOg");
/// let mut cl = Client::connect(None).await?;
/// if let Some(status) = cl.get_batch_status(bid).await? {
///     println!("This batch created at {}", status.created_at);
/// }
/// # Ok::<(), faktory::Error>(())
/// });
/// ```
pub struct Client<S: AsyncBufReadExt + AsyncWriteExt + Send + Unpin> {
    stream: S,
    opts: ClientOptions,
}

impl<S> Client<S>
where
    S: AsyncBufReadExt + AsyncWriteExt + Unpin + Send + Reconnect,
{
    pub(crate) async fn connect_again(&mut self) -> Result<Self, Error> {
        let s = self.stream.reconnect().await?;
        Client::new(s, self.opts.clone()).await
    }

    pub(crate) async fn reconnect(&mut self) -> Result<(), Error> {
        self.stream = self.stream.reconnect().await?;
        self.init().await
    }
}

impl<S> Drop for Client<S>
where
    S: AsyncBufReadExt + AsyncWriteExt + Unpin + Send,
{
    fn drop(&mut self) {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                single::write_command(&mut self.stream, &single::End)
                    .await
                    .unwrap();
            })
        });
    }
}

pub(crate) enum HeartbeatStatus {
    Ok,
    Terminate,
    Quiet,
}

impl Client<BufStream<TokioStream>> {
    /// Create new [`Client`] and connect to a Faktory server.
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
    pub async fn connect(url: Option<&str>) -> Result<Client<BufStream<TokioStream>>, Error> {
        let url = utils::parse_provided_or_from_env(url)?;
        let stream = TokioStream::connect(utils::host_from_url(&url)).await?;
        let buffered = BufStream::new(stream);
        Self::connect_with(buffered, url.password().map(|p| p.to_string())).await
    }
}

impl<S> Client<S>
where
    S: AsyncBufReadExt + AsyncWriteExt + Unpin + Send,
{
    async fn init(&mut self) -> Result<(), Error> {
        let hi = single::read_hi(&mut self.stream).await?;
        check_protocols_match(hi.version)?;

        let mut hello = single::Hello::default();

        // prepare password hash, if one expected by 'Faktory'
        if hi.salt.is_some() {
            if let Some(ref pwd) = self.opts.password {
                hello.set_password(&hi, pwd);
            } else {
                return Err(error::Connect::AuthenticationNeeded.into());
            }
        }

        // fill in any missing options, and remember them for re-connect
        let mut hello = single::Hello::default();

        if self.opts.is_worker {
            // fill in any missing options, and remember them for re-connect

            let hostname = self
                .opts
                .hostname
                .clone()
                .or_else(|| hostname::get().ok()?.into_string().ok())
                .unwrap_or_else(|| "local".to_string());
            self.opts.hostname = Some(hostname);
            let pid = self.opts.pid.unwrap_or_else(|| std::process::id() as usize);
            self.opts.pid = Some(pid);
            let wid = self.opts.wid.clone().unwrap_or_else(single::gen_random_wid);
            self.opts.wid = Some(wid);

            hello.hostname = Some(self.opts.hostname.clone().unwrap());
            hello.wid = Some(self.opts.wid.clone().unwrap());
            hello.pid = Some(self.opts.pid.unwrap());
            hello.labels = self.opts.labels.clone();
        }

        if hi.salt.is_some() {
            if let Some(ref pwd) = self.opts.password {
                hello.set_password(&hi, pwd);
            } else {
                return Err(error::Connect::AuthenticationNeeded.into());
            }
        }

        single::write_command_and_await_ok(&mut self.stream, &hello).await?;
        Ok(())
    }

    pub(crate) async fn new(stream: S, opts: ClientOptions) -> Result<Client<S>, Error> {
        let mut c = Client { stream, opts };
        c.init().await?;
        Ok(c)
    }

    /// Create new [`Client`] and connect to a Faktory server with a non-standard stream.
    pub async fn connect_with(stream: S, pwd: Option<String>) -> Result<Client<S>, Error> {
        let opts = ClientOptions {
            password: pwd,
            ..Default::default()
        };
        Client::new(stream, opts).await
    }

    pub(crate) async fn issue<FC: single::FaktoryCommand>(
        &mut self,
        c: &FC,
    ) -> Result<ReadToken<'_, S>, Error> {
        single::write_command(&mut self.stream, c).await?;
        Ok(ReadToken(self))
    }

    pub(crate) async fn fetch<Q>(&mut self, queues: &[Q]) -> Result<Option<Job>, Error>
    where
        Q: AsRef<str> + Sync,
    {
        self.issue(&single::Fetch::from(queues))
            .await?
            .read_json()
            .await
    }

    pub(crate) async fn heartbeat(&mut self) -> Result<HeartbeatStatus, Error> {
        single::write_command(
            &mut self.stream,
            &Heartbeat::new(&**self.opts.wid.as_ref().unwrap()),
        )
        .await?;

        match single::read_json::<_, serde_json::Value>(&mut self.stream).await? {
            None => Ok(HeartbeatStatus::Ok),
            Some(s) => match s
                .as_object()
                .and_then(|m| m.get("state"))
                .and_then(|s| s.as_str())
            {
                Some("terminate") => Ok(HeartbeatStatus::Terminate),
                Some("quiet") => Ok(HeartbeatStatus::Quiet),
                _ => Err(error::Protocol::BadType {
                    expected: "heartbeat response",
                    received: format!("{}", s),
                }
                .into()),
            },
        }
    }
}

#[cfg(feature = "ent")]
#[cfg_attr(docsrs, doc(cfg(feature = "ent")))]
impl<S: AsyncBufReadExt + AsyncWriteExt + Unpin + Send> Client<S> {
    /// Send information on a job's execution progress to Faktory.
    pub async fn set_progress(&mut self, upd: ProgressUpdate) -> Result<(), Error> {
        let cmd = Track::Set(upd);
        self.issue(&cmd).await?.read_ok().await
    }

    /// Fetch information on a job's execution progress from Faktory.
    pub async fn get_progress(&mut self, jid: String) -> Result<Option<Progress>, Error> {
        let cmd = Track::Get(jid);
        self.issue(&cmd).await?.read_json().await
    }

    /// Fetch information on a batch of jobs execution progress.
    pub async fn get_batch_status(&mut self, bid: String) -> Result<Option<BatchStatus>, Error> {
        let cmd = GetBatchStatus::from(bid);
        self.issue(&cmd).await?.read_json().await
    }
}

pub struct ReadToken<'a, S: AsyncBufReadExt + AsyncWriteExt + Unpin + Send>(&'a mut Client<S>);

impl<'a, S: AsyncBufReadExt + AsyncWriteExt + Unpin + Send> ReadToken<'a, S> {
    pub(crate) async fn read_ok(self) -> Result<(), Error> {
        single::read_ok(&mut self.0.stream).await
    }

    pub(crate) async fn read_json<T>(self) -> Result<Option<T>, Error>
    where
        T: serde::de::DeserializeOwned,
    {
        single::read_json(&mut self.0.stream).await
    }

    #[cfg(feature = "ent")]
    pub(crate) async fn read_bid(self) -> Result<String, Error> {
        single::read_bid(&mut self.0.stream).await
    }

    #[cfg(feature = "ent")]
    pub(crate) async fn maybe_bid(self) -> Result<Option<String>, Error> {
        let bid_read_res = single::read_bid(&mut self.0.stream).await;
        if bid_read_res.is_ok() {
            return Ok(Some(bid_read_res.unwrap()));
        }
        match bid_read_res.unwrap_err() {
            Error::Protocol(error::Protocol::Internal { msg }) => {
                if msg.starts_with("No such batch") {
                    return Ok(None);
                }
                return Err(error::Protocol::Internal { msg }.into());
            }
            another => Err(another),
        }
    }
}
