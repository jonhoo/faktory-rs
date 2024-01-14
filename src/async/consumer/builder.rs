use std::future::Future;
use std::pin::Pin;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufStream};
use tokio::net::TcpStream as TokioStream;

use crate::{
    proto::{get_env_url, host_from_url, url_parse, ClientOptions},
    Error, Job,
};

use super::{AsyncConsumer, AsyncJobRunner, CallbacksRegistry, Client};

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

impl<E: 'static> AsyncConsumerBuilder<E> {
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
        H: Fn(Job) -> Pin<Box<dyn Future<Output = Result<(), E>>>> + 'static,
    {
        let runner = AsyncJobRunner::new(handler);
        self.callbacks.insert(kind.into(), runner);
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
        self.callbacks.insert(kind.into(), runner);
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
