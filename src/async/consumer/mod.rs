use std::{
    future::Future,
    sync::{Arc, Mutex},
};

use fnv::FnvHashMap;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufStream};
use tokio::net::TcpStream as TokioStream;

use super::Client;
use crate::{
    consumer::WorkerState,
    proto::{get_env_url, host_from_url, url_parse, ClientOptions},
    Error, Job,
};

/// A convenience wrapper type for a [`Job`] handler.
pub struct AsyncJobRunner<E>(Box<dyn Fn(Job) -> Box<dyn Future<Output = Result<(), E>>>>);

impl<E> AsyncJobRunner<E> {
    /// Creates a new `JobRunner`.
    pub fn new(
        runner: impl Fn(Job) -> Box<dyn Future<Output = Result<(), E>>> + 'static,
    ) -> AsyncJobRunner<E> {
        Self(Box::new(runner))
    }
}

struct WorkerStatesRegistry(Vec<Mutex<WorkerState>>);

impl WorkerStatesRegistry {
    fn new(workers_count: usize) -> Self {
        Self((0..workers_count).map(|_| Default::default()).collect())
    }
}

struct CallbacksRegistry<E>(FnvHashMap<String, AsyncJobRunner<E>>);

impl<E> Default for CallbacksRegistry<E> {
    fn default() -> CallbacksRegistry<E> {
        Self(FnvHashMap::default())
    }
}

impl<E> CallbacksRegistry<E> {
    fn register(&mut self, kind: String, runner: AsyncJobRunner<E>) {
        self.0.insert(kind, runner);
    }
}

/// Asynchronous version of the [`Consumer`](struct.Consumer.html).
pub struct AsyncConsumer<S: AsyncBufReadExt + AsyncWriteExt + Send, E> {
    c: Client<S>,
    worker_states: Arc<WorkerStatesRegistry>,
    callbacks: Arc<CallbacksRegistry<E>>,
    terminated: bool,
}

/// Convenience wrapper for building a Faktory worker.
///
/// See the [`AsyncConsumer`] documentation for details.
pub struct AsyncConsumerBuilder<E> {
    opts: ClientOptions,
    workers_count: usize,
    callbacks: CallbacksRegistry<E>,
}

impl<E> Default for AsyncConsumerBuilder<E> {
    /// See [`ConsumerBuilder`](struct.ConsumerBuilder.html)
    fn default() -> Self {
        AsyncConsumerBuilder {
            opts: ClientOptions::default(),
            workers_count: 1,
            callbacks: CallbacksRegistry::default(),
        }
    }
}

impl<E> AsyncConsumerBuilder<E> {
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
    pub fn register<K, H>(&mut self, kind: K, handler: H) -> &mut Self
    where
        K: Into<String>,
        H: Fn(Job) -> Box<dyn Future<Output = Result<(), E>>> + 'static,
    {
        let runner = AsyncJobRunner::new(handler);
        self.callbacks.register(kind.into(), runner);
        self
    }

    /// Register a job runner for the given job type (`kind`).
    ///
    /// Serving same purpose as [`register`](struct.AsyncConsumerBuilder.html#structmethod.register),
    /// but accepting an instance of [`JobRunnner`] as the second argument.
    pub fn register_runner(
        &mut self,
        kind: impl Into<String>,
        runner: AsyncJobRunner<E>,
    ) -> &mut Self {
        self.callbacks.register(kind.into(), runner);
        self
    }

    /// Asynchronously connect to a Faktory server with a non-standard stream.
    pub async fn connect_with<S: AsyncBufReadExt + AsyncWriteExt + Send + Unpin>(
        mut self,
        stream: S,
        pwd: Option<String>,
    ) -> Result<AsyncConsumer<S, E>, Error> {
        self.opts.password = pwd;
        Ok(AsyncConsumer::new(
            Client::new(stream, self.opts).await?,
            self.workers_count,
            self.callbacks,
        )
        .await)
    }

    /// Asynchronously connect to a Faktory server.
    ///
    /// See [`connect`](struct.ConsumerBuilder.html#structmethod.connect).
    pub async fn connect(
        self,
        url: Option<&str>,
    ) -> Result<AsyncConsumer<BufStream<TokioStream>, E>, Error> {
        let url = match url {
            Some(url) => url_parse(url),
            None => url_parse(&get_env_url()),
        }?;
        let stream = TokioStream::connect(host_from_url(&url)).await?;
        let buffered = BufStream::new(stream);
        let client = Client::new(buffered, self.opts).await?;
        Ok(AsyncConsumer::new(client, self.workers_count, self.callbacks).await)
    }
}

impl<S: AsyncBufReadExt + AsyncWriteExt + Send + Unpin, E> AsyncConsumer<S, E> {
    async fn new(c: Client<S>, workers_count: usize, callbacks: CallbacksRegistry<E>) -> Self {
        AsyncConsumer {
            c,
            callbacks: Arc::new(callbacks),
            worker_states: Arc::new(WorkerStatesRegistry::new(workers_count)),
            terminated: false,
        }
    }
}
