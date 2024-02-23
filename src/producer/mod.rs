use std::collections::HashMap;

use crate::proto::{Client, Info, Job, Push, PushBulk, QueueAction, QueueControl};
use crate::Error;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufStream};
use tokio::net::TcpStream as TokioStream;

#[cfg(feature = "ent")]
use crate::proto::{Batch, BatchHandle, CommitBatch, OpenBatch};

/// `Producer` is used to enqueue new jobs that will in turn be processed by Faktory workers.
///
/// # Connecting to Faktory
///
/// To issue jobs, the `Producer` must first be connected to the Faktory server. Exactly how you do
/// that depends on your setup. Faktory suggests using the `FAKTORY_PROVIDER` and `FAKTORY_URL`
/// environment variables (see their docs for more information) with `localhost:7419` as the
/// fallback default. If you want this behavior, pass `None` to
/// [`Producer::connect`](struct.Producer.html#method.connect). If not, you can supply the URL
/// directly to [`Producer::connect`](struct.Producer.html#method.connect) in the form:
///
/// ```text
/// protocol://[:password@]hostname[:port]
/// ```
///
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
/// # tokio_test::block_on(async {
/// use faktory::Producer;
/// let p = Producer::connect(None).await.unwrap();
/// # });
/// ```
///
/// Connecting to a secured Faktory server using an explicit URL
///
/// ```no_run
/// # tokio_test::block_on(async {
/// use faktory::Producer;
/// let p = Producer::connect(Some("tcp://:hunter2@localhost:7439")).await.unwrap();
/// # })
/// ```
///
/// Issuing a job using a `Producer`
///
/// ```no_run
/// # tokio_test::block_on(async {
/// # use faktory::Producer;
/// # let mut p = Producer::connect(None).await.unwrap();
/// use faktory::Job;
/// p.enqueue(Job::new("foobar", vec!["z"])).await.unwrap();
/// # });
/// ```
///
pub struct Producer<S: AsyncBufReadExt + AsyncWriteExt + Send + Unpin> {
    c: Client<S>,
}

impl<S: AsyncBufReadExt + AsyncWriteExt + Send + Unpin> Producer<S> {
    /// Asynchronously enqueue the given job on the Faktory server.
    ///
    /// Returns `Ok` if the job was successfully queued by the Faktory server.
    pub async fn enqueue(&mut self, job: Job) -> Result<(), Error> {
        self.c.issue(&Push::from(job)).await?.read_ok().await
    }

    /// Enqueue numerous jobs on the Faktory server.
    ///
    /// Provided you have numerous jobs to submit, using this method will be more efficient as compared
    /// to calling [`enqueue`](Producer::enqueue) multiple times.
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
            .c
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

    /// Retrieve information about the running server.
    ///
    /// The returned value is the result of running the `INFO` command on the server.
    pub async fn info(&mut self) -> Result<serde_json::Value, Error> {
        self.c
            .issue(&Info)
            .await?
            .read_json()
            .await
            .map(|v| v.expect("info command cannot give empty response"))
    }

    /// Pause the given queues.
    pub async fn queue_pause<Q>(&mut self, queues: &[Q]) -> Result<(), Error>
    where
        Q: AsRef<str> + Sync,
    {
        self.c
            .issue(&QueueControl::new(QueueAction::Pause, queues))
            .await?
            .read_ok()
            .await
    }

    /// Resume the given queues.
    pub async fn queue_resume<Q: AsRef<str>>(&mut self, queues: &[Q]) -> Result<(), Error>
    where
        Q: AsRef<str> + Sync,
    {
        self.c
            .issue(&QueueControl::new(QueueAction::Resume, queues))
            .await?
            .read_ok()
            .await
    }

    /// Initiate a new batch of jobs.
    #[cfg(feature = "ent")]
    #[cfg_attr(docsrs, doc(cfg(feature = "ent")))]
    pub async fn start_batch(&mut self, batch: Batch) -> Result<BatchHandle<'_, S>, Error> {
        let bid = self.c.issue(&batch).await?.read_bid().await?;
        Ok(BatchHandle::new(bid, self))
    }

    /// Open an already existing batch of jobs.
    ///
    /// This will not error if a batch with the provided `bid` does not exist,
    /// rather `Ok(None)` will be returned.
    #[cfg(feature = "ent")]
    #[cfg_attr(docsrs, doc(cfg(feature = "ent")))]
    pub async fn open_batch(&mut self, bid: String) -> Result<Option<BatchHandle<'_, S>>, Error> {
        let bid = self
            .c
            .issue(&OpenBatch::from(bid))
            .await?
            .maybe_bid()
            .await?;
        Ok(bid.map(|bid| BatchHandle::new(bid, self)))
    }

    #[cfg(feature = "ent")]
    pub(crate) async fn commit_batch(&mut self, bid: String) -> Result<(), Error> {
        self.c.issue(&CommitBatch::from(bid)).await?.read_ok().await
    }
}

impl<S: AsyncRead + AsyncWrite + Send + Unpin> Producer<BufStream<S>> {
    /// Connect to a Faktory server with a non-standard stream.
    pub async fn connect_with(
        stream: S,
        pwd: Option<String>,
    ) -> Result<Producer<BufStream<S>>, Error> {
        let buffered = BufStream::new(stream);
        let c = Client::connect_with(buffered, pwd).await?;
        Ok(Producer { c })
    }
}

impl Producer<BufStream<TokioStream>> {
    /// Create a producer and asynchronously connect to a Faktory server.
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
    pub async fn connect(url: Option<&str>) -> Result<Self, Error> {
        let c = Client::connect(url).await?;
        Ok(Producer { c })
    }
}
