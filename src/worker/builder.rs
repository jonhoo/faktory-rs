use super::{runner::Closure, CallbacksRegistry, Client, Worker};
use crate::{
    proto::{utils, ClientOptions},
    Error, Job, JobRunner,
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

impl<E> WorkerBuilder<E> {
    /// Create a builder for asynchronous version of `Worker`.
    pub fn default_async() -> WorkerBuilder<E> {
        WorkerBuilder::default()
    }
}

impl<E> Default for WorkerBuilder<E> {
    /// Create a builder for asynchronous version of `ConWorkersumer`.
    ///
    /// See [`WorkerBuilder`](struct.WorkerBuilder.html)
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
    pub fn wid(&mut self, wid: String) -> &mut Self {
        self.opts.wid = Some(wid);
        self
    }

    /// Set the labels to use for this worker.
    ///
    /// Defaults to `["rust"]`.
    pub fn labels(&mut self, labels: Vec<String>) -> &mut Self {
        self.opts.labels = labels;
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
    pub fn register<K, H, Fut>(&mut self, kind: K, handler: H) -> &mut Self
    where
        K: Into<String>,

        H: Fn(Job) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), E>> + Send,
    {
        self.register_runner(kind, Closure(handler));
        self
    }

    /// Register a handler for the given job type (`kind`).
    ///
    /// Whenever a job whose type matches `kind` is fetched from the Faktory, the given handler
    /// object is called with that job as its argument.
    pub fn register_runner<K, H>(&mut self, kind: K, runner: H) -> &mut Self
    where
        K: Into<String>,
        H: JobRunner<Error = E> + 'static,
    {
        self.callbacks.insert(kind.into(), Box::new(runner));
        self
    }

    /// Asynchronously connect to a Faktory server with a non-standard stream.
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

    /// Asynchronously connect to a Faktory server.
    ///
    /// See [`connect`](WorkerBuilder::connect).
    pub async fn connect(
        self,
        url: Option<&str>,
    ) -> Result<Worker<BufStream<TokioStream>, E>, Error> {
        let url = utils::parse_provided_or_from_env(url)?;
        let stream = TokioStream::connect(utils::host_from_url(&url)).await?;
        let buffered = BufStream::new(stream);
        let client = Client::new(buffered, self.opts).await?;
        Ok(Worker::new(client, self.workers_count, self.callbacks).await)
    }
}
