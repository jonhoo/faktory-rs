#[cfg(feature = "ent")]
#[cfg_attr(docsrs, doc(cfg(feature = "ent")))]
mod ent;

#[cfg(doc)]
use crate::proto::{BatchStatus, Progress, ProgressUpdate};

use super::utils::{get_env_url, url_parse};
use super::{single, Info, Push, QueueAction, QueueControl};
use super::{utils, PushBulk};
use crate::error::{self, Error};
use crate::{Job, Reconnect, WorkerId};
use semver::Op;
use std::collections::HashMap;
use tokio::io::{AsyncBufRead, AsyncWrite, BufStream};
use tokio::net::TcpStream as TokioStream;

mod options;
pub(crate) use options::ClientOptions;

mod conn;
pub(crate) use conn::BoxedConnection;
pub use conn::Connection;

mod mutation;

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
/// [`WorkerBuilder::register`](struct.WorkerBuilder.html#method.register) (or the equivalent
/// handler registration method in workers written in other languages).
///
/// Since Faktory workers do not all need to be the same (you could have some written in Rust for
/// performance-critical tasks, some in Ruby for more webby tasks, etc.), it may be the case that a
/// given job can only be executed by some workers (e.g., if they job type is not registered at
/// others). To allow for this, Faktory includes a `labels` field with each job. Jobs will only be
/// sent to workers whose labels (see
/// [`WorkerBuilder::labels`](struct.WorkerBuilder.html#method.labels)) match those set in
/// `Job::labels`.
///
/// # Examples
///
/// Connecting to an unsecured Faktory server using environment variables:
///
/// ```no_run
/// # tokio_test::block_on(async {
/// use faktory::Client;
/// let p = Client::connect().await.unwrap();
/// # });
/// ```
///
/// Connecting to a secured Faktory server using an explicit URL:
///
/// ```no_run
/// # tokio_test::block_on(async {
/// use faktory::Client;
/// let p = Client::connect_to("tcp://:hunter2@localhost:7439").await.unwrap();
/// # })
/// ```
///
/// Issuing a job using a `Client`:
///
/// ```no_run
/// # tokio_test::block_on(async {
/// # use faktory::Client;
/// # let mut client = Client::connect().await.unwrap();
/// use faktory::Job;
/// client.enqueue(Job::new("foobar", vec!["z"])).await.unwrap();
/// # });
/// ```
///
/// `Client` is also useful for retrieving and updating information on a job's execution progress
/// (see [`Progress`] and [`ProgressUpdate`]), as well for retrieving a batch's status
/// from the Faktory server (see [`BatchStatus`]). But these constructs are only available under `ent` feature
/// and are only supported by Enterprise Faktory.
///
/// Fetching a job's execution progress:
///
/// ```no_run
/// # tokio_test::block_on(async {
/// use faktory::{Client, JobId, ent::JobState};
/// let job_id = JobId::new("W8qyVle9vXzUWQOf");
/// let mut cl = Client::connect().await?;
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
/// use faktory::{Client, JobId, ent::ProgressUpdate};
/// let jid = JobId::new("W8qyVle9vXzUWQOf");
/// let mut cl = Client::connect().await?;
/// let progress = ProgressUpdate::builder(jid)
///     .desc("Almost done...".to_owned())
///     .percent(99)
///     .build();
/// cl.set_progress(&progress).await?;
/// # Ok::<(), faktory::Error>(())
/// });
///````
///
/// Fetching a batch's status:
///
/// ```no_run
/// # tokio_test::block_on(async {
/// use faktory::{Client, ent::BatchId};
/// let bid = BatchId::new("W8qyVle9vXzUWQOg");
/// let mut cl = Client::connect().await?;
/// if let Some(status) = cl.get_batch_status(bid).await? {
///     println!("This batch created at {}", status.created_at);
/// }
/// # Ok::<(), faktory::Error>(())
/// });
/// ```
pub struct Client {
    stream: BoxedConnection,
    pub(crate) opts: ClientOptions,
}
impl Client {
    pub(crate) async fn connect_again(&mut self) -> Result<Self, Error> {
        let s = self.stream.reconnect().await?;
        Client::new(s, self.opts.clone()).await
    }

    pub(crate) async fn reconnect(&mut self) -> Result<(), Error> {
        self.stream = self.stream.reconnect().await?;
        self.init().await
    }
}

impl Drop for Client {
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

impl Client {
    /// Create new [`Client`] and connect to a Faktory server with a non-standard stream.
    ///
    /// In case you've got a `stream` that doesn't already implement `AsyncBufRead`, you will
    /// want to wrap it in `tokio::io::BufStream`.
    pub async fn connect_with<S>(stream: S, pwd: Option<String>) -> Result<Client, Error>
    where
        S: AsyncBufRead + AsyncWrite + Reconnect + Send + Sync + Unpin + 'static,
    {
        let opts = ClientOptions {
            password: pwd,
            ..Default::default()
        };
        Client::new(Box::new(stream), opts).await
    }
}

impl Client {
    /// Create new [`Client`] and connect to a Faktory server.
    ///
    /// Will use the standard Faktory environment variables. Specifically,
    /// `FAKTORY_PROVIDER` is read to get the name of the environment variable to get the address
    /// from (defaults to `FAKTORY_URL`), and then that environment variable is read to get the
    /// server address. If the latter environment variable is not defined, the connection will be
    /// made to
    ///
    /// ```text
    /// tcp://localhost:7419
    /// ```
    pub async fn connect() -> Result<Client, Error> {
        let url = get_env_url();
        Self::connect_to(&url).await
    }

    /// Create new [`Client`] and connect to a Faktory server using specified address.
    ///
    /// If the address of the Faktory server is present in the environment,
    /// you may want to simply use [`Client::connect`].
    pub async fn connect_to(addr: &str) -> Result<Client, Error> {
        let url = url_parse(addr)?;
        let stream = TokioStream::connect(utils::host_from_url(&url)).await?;
        let buffered_stream = BufStream::new(stream);
        Self::connect_with(buffered_stream, url.password().map(|p| p.to_string())).await
    }
}

impl Client {
    async fn init(&mut self) -> Result<(), Error> {
        let hi = single::read_hi(&mut self.stream).await?;
        check_protocols_match(hi.version)?;

        // fill in any missing options, and remember them for re-connect
        let mut hello = single::Hello::default();

        // prepare password hash, if one expected by 'Faktory'
        if hi.salt.is_some() {
            if let Some(ref pwd) = self.opts.password {
                hello.set_password(&hi, pwd);
            } else {
                return Err(error::Connect::AuthenticationNeeded.into());
            }
        }

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
            let wid = self.opts.wid.clone().unwrap_or_else(WorkerId::random);
            self.opts.wid = Some(wid);

            hello.hostname = Some(self.opts.hostname.clone().unwrap());
            hello.wid = Some(self.opts.wid.clone().unwrap());
            hello.pid = Some(self.opts.pid.unwrap());
            hello.labels.clone_from(&self.opts.labels);
        }

        single::write_command_and_await_ok(&mut self.stream, &hello).await?;
        Ok(())
    }

    pub(crate) async fn new(stream: BoxedConnection, opts: ClientOptions) -> Result<Client, Error> {
        let mut c = Client { stream, opts };
        c.init().await?;
        Ok(c)
    }

    pub(crate) async fn issue<FC: single::FaktoryCommand>(
        &mut self,
        c: &FC,
    ) -> Result<ReadToken<'_>, Error> {
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

    pub(crate) async fn heartbeat(
        &mut self,
        rss_kb: Option<u64>,
    ) -> Result<HeartbeatStatus, Error> {
        let wid = self
            .opts
            .wid
            .as_ref()
            .expect("every worker to have wid")
            .clone();
        single::write_command(&mut self.stream, &single::Heartbeat::new(wid, rss_kb)).await?;

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
                    received: s.to_string(),
                }
                .into()),
            },
        }
    }

    pub(crate) async fn perform_queue_action<Q>(
        &mut self,
        queues: &[Q],
        action: QueueAction,
    ) -> Result<(), Error>
    where
        Q: AsRef<str> + Sync,
    {
        self.issue(&QueueControl::new(action, queues))
            .await?
            .read_ok()
            .await
    }
}

impl Client {
    /// Enqueue the given job on the Faktory server.
    ///
    /// Returns `Ok` if the job was successfully queued by the Faktory server.
    pub async fn enqueue(&mut self, job: Job) -> Result<(), Error> {
        self.issue(&Push::from(job)).await?.read_ok().await
    }

    /// Enqueue numerous jobs on the Faktory server.
    ///
    /// Provided you have numerous jobs to submit, using this method will be more efficient as compared
    /// to calling [`enqueue`](Client::enqueue) multiple times.
    ///
    /// The returned `Ok` result will contain a tuple of enqueued jobs count and an option of a hash map
    /// with job ids mapped onto error messages. Therefore `Ok(n, None)` will indicate that all n jobs
    /// have been enqueued without errors.
    ///
    /// Note that this is not an all-or-nothing operation: jobs that contain errors will not be enqueued,
    /// while those that are error-free _will_ be enqueued by the Faktory server.
    pub async fn enqueue_many<J>(
        &mut self,
        jobs: J,
    ) -> Result<(usize, Option<HashMap<String, String>>), Error>
    where
        J: IntoIterator<Item = Job>,
        J::IntoIter: ExactSizeIterator,
    {
        let jobs = jobs.into_iter();
        let jobs_count = jobs.len();
        let errors: HashMap<String, String> = self
            .issue(&PushBulk::from(jobs.collect::<Vec<_>>()))
            .await?
            .read_json()
            .await?
            .expect("Faktory server sends {} literal when there are no errors");
        if errors.is_empty() {
            return Ok((jobs_count, None));
        }
        Ok((jobs_count - errors.len(), Some(errors)))
    }

    /// Retrieve [information](crate::ServerSnapshot) about the running server.
    ///
    /// The returned value is the result of running the `INFO` command on the server.
    pub async fn current_info(&mut self) -> Result<single::FaktoryState, Error> {
        self.issue(&Info)
            .await?
            .read_json()
            .await
            .map(|v| v.expect("info command cannot give empty response"))
    }

    /// Pause the given queues.
    ///
    /// Passing a wildcard `&["*"]` as the value of the `queues` parameter
    /// will pause all the queues. To be more explicit, you may want to call [`Client::queue_pause_all`]
    /// shortcut method to pause all the queues.
    pub async fn queue_pause<Q>(&mut self, queues: &[Q]) -> Result<(), Error>
    where
        Q: AsRef<str> + Sync,
    {
        self.perform_queue_action(queues, QueueAction::Pause).await
    }

    /// Pause all queues.
    pub async fn queue_pause_all(&mut self) -> Result<(), Error> {
        self.perform_queue_action(&["*"], QueueAction::Pause).await
    }

    /// Resume the given queues.
    ///
    /// Passing a wildcard `&["*"]` as the value of the `queues` parameter
    /// will resume all the queues. To be more explicit, you may want to call [`Client::queue_resume_all`]
    /// shortcut method to resume all the queues.
    pub async fn queue_resume<Q>(&mut self, queues: &[Q]) -> Result<(), Error>
    where
        Q: AsRef<str> + Sync,
    {
        self.perform_queue_action(queues, QueueAction::Resume).await
    }

    /// Resume all queues.
    pub async fn queue_resume_all(&mut self) -> Result<(), Error> {
        self.perform_queue_action(&["*"], QueueAction::Resume).await
    }

    /// Remove the given queues.
    ///
    /// Beware, passing a wildcard `&["*"]` as the value of the `queues` parameter
    /// will **remove** all the queues. To be more explicit, you may want to call [`Client::queue_remove_all`]
    /// shortcut method to remove all the queues.
    pub async fn queue_remove<Q>(&mut self, queues: &[Q]) -> Result<(), Error>
    where
        Q: AsRef<str> + Sync,
    {
        self.perform_queue_action(queues, QueueAction::Remove).await
    }

    /// Remove all queues.
    pub async fn queue_remove_all(&mut self) -> Result<(), Error> {
        self.perform_queue_action(&["*"], QueueAction::Remove).await
    }
}

pub struct ReadToken<'a>(pub(crate) &'a mut Client);

impl ReadToken<'_> {
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
