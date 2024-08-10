use super::{runner::Closure, CallbacksRegistry, Client, ShutdownSignal, Worker};
use crate::{
    proto::{utils, ClientOptions},
    Error, Job, JobRunner, Reconnect, TlsKind, WorkerId,
};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufRead, AsyncRead, AsyncWrite, BufStream};
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
    tls_kind: Option<TlsKind>,
    skip_verify_server_certs: bool,
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
            tls_kind: None,
            skip_verify_server_certs: true,
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
    /// Note that once the `signal` resolves, the [`Worker`] will be marked as terminated and calling
    /// [`Worker::run`] will cause a panic. You will need to build and run a new worker instead.
    ///
    /// ```no_run
    /// # tokio_test::block_on(async {
    /// use faktory::{Client, Job, StopReason, Worker};
    /// use std::time::Duration;
    /// use tokio_util::sync::CancellationToken;
    /// use tokio::time::sleep;
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
    /// // verify the consumer thread has not finished
    /// sleep(Duration::from_secs(2)).await;
    /// assert!(!jh.is_finished());
    ///  
    /// // send a signal to eventually return control (upon graceful shutdown)
    /// token.cancel();
    ///
    /// // learn the stop reason and the number of workers that were still running
    /// let stop_details = jh.await.expect("joined ok").unwrap();
    /// assert_eq!(stop_details.reason, StopReason::GracefulShutdown);
    /// let _nrunning = stop_details.workers_still_running;
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

    /// Make the traffic between this worker and Faktory encrypted with native TLS.
    ///
    /// The underlying crate (`native-tls`) will use _SChannel_ on Windows,
    /// _SecureTransport_ on OSX, and _OpenSSL_ on other platforms.
    ///
    /// Note that if you use this method on the builder, but eventually use [`WorkerBuilder::connect_with`]
    /// (rather than [`WorkerBuilder::connect`]) to create an instance of [`Worker`], this worker
    /// will be connected to the Faktory server with the stream you've provided to `connect_with`.
    #[cfg(feature = "native_tls")]
    #[cfg_attr(docsrs, doc(cfg(feature = "native_tls")))]
    pub fn use_native_tls(mut self) -> Self {
        self.tls_kind = Some(TlsKind::Native);
        self
    }

    /// Make the traffic between this worker and Faktory encrypted with [`rustls`](https://github.com/rustls/rustls).
    ///
    /// Note that if you use this method on the builder, but eventually use [`WorkerBuilder::connect_with`]
    /// (rather than [`WorkerBuilder::connect`]) to create an instance of [`Worker`], this worker
    /// will be connected to the Faktory server with the stream you've provided to `connect_with`.
    #[cfg(feature = "rustls")]
    #[cfg_attr(docsrs, doc(cfg(feature = "rustls")))]
    pub fn use_rustls(mut self) -> Self {
        self.tls_kind = Some(TlsKind::Rust);
        self
    }

    /// Connect to a Faktory server with a non-standard stream.
    ///
    /// Iternally, the `stream` will be buffered. In case you've got a `stream` that is _already_
    /// buffered (and so it is `AsyncBufRead`), you will want to use [`WorkerBuilder::connect_with_buffered`]
    /// in order to avoid buffering the stream twice.
    pub async fn connect_with<S>(self, stream: S, pwd: Option<String>) -> Result<Worker<E>, Error>
    where
        S: AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static,
        BufStream<S>: Reconnect,
    {
        let stream = BufStream::new(stream);
        WorkerBuilder::connect_with_buffered(self, stream, pwd).await
    }

    /// Connect to a Faktory server with a non-standard buffered stream.
    ///
    /// In case you've got a `stream` that is _not_ buffered just yet, you may want to use
    /// [`WorkerBuilder::connect_with`] that will do this buffering for you.
    pub async fn connect_with_buffered<S>(
        mut self,
        stream: S,
        pwd: Option<String>,
    ) -> Result<Worker<E>, Error>
    where
        S: AsyncBufRead + AsyncWrite + Reconnect + Send + Sync + Unpin + 'static,
    {
        self.opts.password = pwd;
        self.opts.is_worker = true;
        let client = Client::new(Box::new(stream), self.opts).await?;
        let worker = Worker::new(
            client,
            self.workers_count,
            self.callbacks,
            self.shutdown_timeout,
            self.shutdown_signal,
        );
        Ok(worker)
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
    pub async fn connect(self, url: Option<&str>) -> Result<Worker<E>, Error> {
        let url = utils::parse_provided_or_from_env(url)?;
        let addr = utils::host_from_url(&url);
        let stream = TokioStream::connect(addr).await?;
        match self.tls_kind {
            None => {
                self.connect_with(stream, url.password().map(|p| p.to_string()))
                    .await
            }
            #[cfg(feature = "rustls")]
            Some(TlsKind::Rust) => {
                let hostname = url.host_str().unwrap().to_string();
                let tls_tream = crate::rustls::TlsStream::with_native_certs(
                    stream,
                    hostname,
                    self.skip_verify_server_certs,
                )
                .await?;
                self.connect_with(tls_tream, url.password().map(|p| p.to_string()))
                    .await
            }
            _ => unimplemented!(),
        }
    }
}
