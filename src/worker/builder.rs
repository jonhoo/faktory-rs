use super::{runner::Closure, CallbacksRegistry, Client, Worker};
use crate::{
    proto::{utils, ClientOptions},
    Error, Job, JobRunner, WorkerId,
};
use std::future::Future;
use tokio::io::{AsyncRead, AsyncWrite, BufStream};
use tokio::net::TcpStream as TokioStream;

/// Convenience wrapper for building a Faktory worker.
///
/// See the [`Worker`] documentation for details.
pub struct WorkerBuilder<E> {
    opts: ClientOptions,
    workers_count: usize,
    callbacks: CallbacksRegistry<E>,
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
        }
    }
}

impl<E: 'static> WorkerBuilder<E> {
    /// Set the hostname to use for this worker.
    ///
    /// Defaults to the machine's hostname as reported by the operating system.
    pub fn hostname(&mut self, hn: String) -> &mut Self {
        self.opts.hostname = Some(hn);
        self
    }

    /// Set a unique identifier for this worker.
    ///
    /// Defaults to a randomly generated 32-char ASCII string.
    pub fn wid(&mut self, wid: WorkerId) -> &mut Self {
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
    pub fn labels<I>(&mut self, labels: I) -> &mut Self
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
    pub fn add_to_labels<I>(&mut self, labels: I) -> &mut Self
    where
        I: IntoIterator<Item = String>,
    {
        self.opts.labels.extend(labels);
        self
    }

    /// Set the number of workers to use `run` and `run_to_completion`.
    ///
    /// Defaults to 1.
    pub fn workers(&mut self, w: usize) -> &mut Self {
        self.workers_count = w;
        self
    }

    /// Register a handler function for the given job type (`kind`).
    ///
    /// Whenever a job whose type matches `kind` is fetched from the Faktory, the given handler
    /// function is called with that job as its argument.
    ///
    /// Note that only one single handler per job kind is supported. Registering another handler
    /// for the same job kind will silently override the handler registered previously.
    pub fn register_fn<K, H, Fut>(&mut self, kind: K, handler: H) -> &mut Self
    where
        K: Into<String>,
        H: Fn(Job) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), E>> + Send,
    {
        self.register(kind, Closure(handler));
        self
    }

    /// Register a handler for the given job type (`kind`).
    ///
    /// Whenever a job whose type matches `kind` is fetched from the Faktory, the given handler
    /// object is called with that job as its argument.
    ///
    /// Note that only one single handler per job kind is supported. Registering another handler
    /// for the same job kind will silently override the handler registered previously.
    pub fn register<K, H>(&mut self, kind: K, runner: H) -> &mut Self
    where
        K: Into<String>,
        H: JobRunner<Error = E> + 'static,
    {
        self.callbacks.insert(kind.into(), Box::new(runner));
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
        Ok(Worker::new(client, self.workers_count, self.callbacks).await)
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
        mut self,
        url: Option<&str>,
    ) -> Result<Worker<BufStream<TokioStream>, E>, Error> {
        let url = utils::parse_provided_or_from_env(url)?;
        let stream = TokioStream::connect(utils::host_from_url(&url)).await?;
        self.opts.is_worker = true;
        let buffered = BufStream::new(stream);
        let client = Client::new(buffered, self.opts).await?;
        Ok(Worker::new(client, self.workers_count, self.callbacks).await)
    }
}
