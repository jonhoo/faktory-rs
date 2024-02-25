#[cfg(doc)]
use crate::{Consumer, Producer};

#[cfg(feature = "ent")]
#[cfg_attr(docsrs, doc(cfg(feature = "ent")))]
mod ent;

use super::utils;
use super::{single, Reconnect};
use crate::error::{self, Error};
use crate::Job;
use tokio::io::BufStream;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tokio::net::TcpStream as TokioStream;

mod options;
pub(crate) use options::ClientOptions;

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

/// `Client` is used to enqueue new jobs that will in turn be processed by Faktory workers.
///
/// # Connecting to Faktory
///
/// To issue jobs, the `Client` must first be connected to the Faktory server. Exactly how you do
/// that depends on your setup. Faktory suggests using the `FAKTORY_PROVIDER` and `FAKTORY_URL`
/// environment variables (see their docs for more information) with `localhost:7419` as the
/// fallback default. If you want this behavior, pass `None` to [`Client::connect`](Client::connect).
/// If not, you can supply the URL directly  in the form:
///
/// ```text
/// protocol://[:password@]hostname[:port]
/// ```
///
///
/// # Issuing jobs
///
/// Most of the lifetime of a `Client` will be spent creating and enqueueing jobs for Faktory
/// workers. This is done by passing a [`Job`](struct.Job.html) to
/// [`Client::enqueue`](Client::enqueue). The most important part of a `Job`
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
/// Connecting to an unsecured Faktory server using environment variables:
///
/// ```no_run
/// # tokio_test::block_on(async {
/// use faktory::Client;
/// let p = Client::connect(None).await.unwrap();
/// # });
/// ```
///
/// Connecting to a secured Faktory server using an explicit URL:
///
/// ```no_run
/// # tokio_test::block_on(async {
/// use faktory::Client;
/// let p = Client::connect(Some("tcp://:hunter2@localhost:7439")).await.unwrap();
/// # })
/// ```
///
/// Issuing a job using a `Client`:
///
/// ```no_run
/// # tokio_test::block_on(async {
/// # use faktory::Client;
/// # let mut client = Client::connect(None).await.unwrap();
/// use faktory::Job;
/// client.enqueue(Job::new("foobar", vec!["z"])).await.unwrap();
/// # });
/// ```
///
/// `Client` is also useful for retrieving and updating information on a job's execution progress
/// (see [`Progress`] and [`ProgressUpdate`]), as well for retrieving a batch's status
/// from the Faktory server (see [`BatchStatus`]).
///
/// Fetching a job's execution progress:
///
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
            &single::Heartbeat::new(&**self.opts.wid.as_ref().unwrap()),
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

pub struct ReadToken<'a, S>(pub(crate) &'a mut Client<S>)
where
    S: AsyncBufReadExt + AsyncWriteExt + Unpin + Send;

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
}
