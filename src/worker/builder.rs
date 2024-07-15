use super::{runner::Closure, CallbacksRegistry, Client, ShutdownSignal, Worker};
use crate::{
    proto::{utils, ClientOptions},
    Error, Job, JobRunner, WorkerId,
};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, BufStream};
use tokio::net::TcpStream as TokioStream;

/// Convenience wrapper for building a Faktory worker.
///
/// See the [`Worker`] documentation for details.
pub struct WorkerBuilder<E> {
    opts: ClientOptions,
    workers_count: usize,
    callbacks: CallbacksRegistry<E>,
    shutdown_timeout: Option<Duration>,
    shutdown_signal: Option<ShutdownSignal>,
}

impl<E> Default for WorkerBuilder<E> {
    /// Construct a new [`WorkerBuilder`](struct.WorkerBuilder.html) with default worker options and the url fetched from environment
    /// variables.
    ///
    /// This will construct a worker where:
    ///
    ///  - `hostname` is this machine's hostname.
    ///  - `wid` is a randomly generated string.
    ///  - `pid` is the OS PID of this process.
    ///  - `labels` is `["rust"]`.
    ///
    fn default() -> Self {
        WorkerBuilder {
            opts: ClientOptions::default(),
            workers_count: 1,
            callbacks: CallbacksRegistry::default(),
            shutdown_timeout: None,
            shutdown_signal: None,
        }
    }
}

impl<E: 'static> WorkerBuilder<E> {
    /// Set the hostname to use for this worker.
    ///
    /// Defaults to the machine's hostname as reported by the operating system.
    pub fn hostname(mut self, hn: String) -> Self {
        self.opts.hostname = Some(hn);
        self
    }

    /// Set a unique identifier for this worker.
    ///
    /// Defaults to a randomly generated 32-char ASCII string.
    pub fn wid(mut self, wid: WorkerId) -> Self {
        self.opts.wid = Some(wid);
        self
    }

    /// Set the labels to use for this worker.
    ///
    /// Defaults to `["rust"]`.
    ///
    /// Note that calling this overrides the labels set previously.
    ///
    /// If you need to extend the labels already set, use [`WorkerBuilder::add_to_labels`] instead.
    pub fn labels<I>(mut self, labels: I) -> Self
    where
        I: IntoIterator<Item = String>,
    {
        self.opts.labels = labels.into_iter().collect();
        self
    }

    /// Extend the worker's labels.
    ///
    /// Note that calling this will add the provided labels to those that are already there or -
    /// if no labels have been explicitly set before - to the default `"rust"` label.
    ///
    /// If you need to override the labels set previously, use [`WorkerBuilder::labels`] instead.
    pub fn add_to_labels<I>(mut self, labels: I) -> Self
    where
        I: IntoIterator<Item = String>,
    {
        self.opts.labels.extend(labels);
        self
    }

    /// Set the number of workers to use `run` and `run_to_completion`.
    ///
    /// Defaults to 1.
    pub fn workers(mut self, w: usize) -> Self {
        self.workers_count = w;
        self
    }

    /// Set a graceful shutdown signal.
    ///
    /// As soon as the provided future resolves, the graceful shutdown will step in
    /// making the long-running operation (see [`Worker::run`]) return control to the calling code.
    ///
    /// The graceful shutdown itself is a race between the clean up needed to be performed
    /// (e.g. report on the currently processed to the Faktory server) and a shutdown deadline.
    /// The latter can be customized via [`WorkerBuilder::shutdown_timeout`].
    ///
    /// ```no_run
    /// # tokio_test::block_on(async {
    /// use faktory::{Client, Job, Worker};
    /// use tokio_util::sync::CancellationToken;
    ///
    /// Client::connect(None)
    ///     .await
    ///     .unwrap()
    ///     .enqueue(Job::new("foobar", vec!["z"]))
    ///     .await
    ///     .unwrap();
    ///
    /// // create a signalling future (we are using a utility from the `tokio_util` crate)
    /// let token = CancellationToken::new();
    /// let child_token = token.child_token();
    /// let signal = async move { child_token.cancelled().await };
    ///
    /// // get a connected worker
    /// let mut w = Worker::builder()
    ///     .with_graceful_shutdown(signal)
    ///     .register_fn("job_type", move |_| async { Ok::<(), std::io::Error>(()) })
    ///     .connect(None)
    ///     .await
    ///     .unwrap();
    ///
    /// // start consuming
    /// let jh = tokio::spawn(async move { w.run(&["default"]).await });
    ///
    /// // send a signal to eventually return control (upon graceful shutdown)
    /// token.cancel();
    /// # });
    /// ```
    pub fn with_graceful_shutdown<F>(mut self, signal: F) -> Self
    where
        F: Future<Output = ()> + 'static + Send,
    {
        self.shutdown_signal = Some(Box::pin(signal));
        self
    }

    /// Set a shutdown timeout.
    ///
    /// This will be used once the worker is sent a termination signal whether it is at the application
    /// (via a signalling future, see [`WorkerBuilder::with_graceful_shutdown`]) or OS level (via Ctrl-C signal,
    /// see [`Worker::run_to_completion`]).
    ///
    /// Defaults to `None`, i.e. no shoutdown abortion due to a timeout.
    pub fn shutdown_timeout(mut self, dur: Duration) -> Self {
        self.shutdown_timeout = Some(dur);
        self
    }

    /// Register a handler function for the given job type (`kind`).
    ///
    /// Whenever a job whose type matches `kind` is fetched from the Faktory, the given handler
    /// function is called with that job as its argument.
    ///
    /// Note that only one single handler per job kind is supported. Registering another handler
    /// for the same job kind will silently override the handler registered previously.
    pub fn register_fn<K, H, Fut>(self, kind: K, handler: H) -> Self
    where
        K: Into<String>,
        H: Fn(Job) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), E>> + Send,
    {
        self.register(kind, Closure(handler))
    }

    /// Register a _blocking_ (synchronous) handler function for the given job type (`kind`).
    ///
    /// This is an analogue of [`register_fn`](WorkerBuilder::register_fn) for compute heavy tasks.
    /// Internally, `tokio`'s `spawn_blocking` is used when a job arrives whose type matches `kind`,
    /// and so the `handler` is executed in a dedicated pool for blocking operations. See `Tokio`'s
    /// [docs](https://docs.rs/tokio/latest/tokio/index.html#cpu-bound-tasks-and-blocking-code) for
    /// how to set the upper limit on the number of threads in the mentioned pool and other details.
    ///
    /// You can mix and match async and blocking handlers in a single `Worker`. However, note that
    /// there is no active management of the blocking tasks in `tokio`, and so if you end up with more
    /// CPU-intensive blocking handlers executing at the same time than you have cores, the asynchronous
    /// handler tasks (and indeed, all tasks) will suffer as a result. If you have a lot of blocking tasks,
    /// consider using the standard async job handler (which you can register with [`WorkerBuilder::register`]
    /// or [`WorkerBuilder::register_fn`]) and add explicit code to manage the blocking tasks appropriately.
    ///
    /// Also note that only one single handler per job kind is supported. Registering another handler
    /// for the same job kind will silently override the handler registered previously.
    pub fn register_blocking_fn<K, H>(mut self, kind: K, handler: H) -> Self
    where
        K: Into<String>,
        H: Fn(Job) -> Result<(), E> + Send + Sync + 'static,
    {
        self.callbacks
            .insert(kind.into(), super::Callback::Sync(Arc::new(handler)));
        self
    }

    /// Register a handler for the given job type (`kind`).
    ///
    /// Whenever a job whose type matches `kind` is fetched from the Faktory, the given handler
    /// object is called with that job as its argument.
    ///
    /// Note that only one single handler per job kind is supported. Registering another handler
    /// for the same job kind will silently override the handler registered previously.
    pub fn register<K, H>(mut self, kind: K, runner: H) -> Self
    where
        K: Into<String>,
        H: JobRunner<Error = E> + 'static,
    {
        self.callbacks
            .insert(kind.into(), super::Callback::Async(Box::new(runner)));
        self
    }

    /// Connect to a Faktory server with a non-standard stream.
    pub async fn connect_with<S: AsyncRead + AsyncWrite + Send + Unpin>(
        mut self,
        stream: S,
        pwd: Option<String>,
    ) -> Result<Worker<BufStream<S>, E>, Error> {
        self.opts.password = pwd;
        self.opts.is_worker = true;
        let buffered = BufStream::new(stream);
        let client = Client::new(buffered, self.opts).await?;
        Ok(Worker::new(
            client,
            self.workers_count,
            self.callbacks,
            self.shutdown_timeout,
            self.shutdown_signal,
        )
        .await)
    }

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
    pub async fn connect(
        self,
        url: Option<&str>,
    ) -> Result<Worker<BufStream<TokioStream>, E>, Error> {
        let url = utils::parse_provided_or_from_env(url)?;
        let stream = TokioStream::connect(utils::host_from_url(&url)).await?;
        self.connect_with(stream, None).await
    }
}
