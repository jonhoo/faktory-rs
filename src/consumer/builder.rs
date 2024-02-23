use super::{runner::Closure, CallbacksRegistry, Client, Consumer};
use crate::{
    proto::{
        utils::{get_env_url, host_from_url, url_parse},
        ClientOptions,
    },
    Error, Job, JobRunner,
};
use std::future::Future;
use std::pin::Pin;
use tokio::io::{AsyncRead, AsyncWrite, BufStream};
use tokio::net::TcpStream as TokioStream;

/// Convenience wrapper for building a Faktory worker.
///
/// See the [`Consumer`] documentation for details.
pub struct ConsumerBuilder<E> {
    opts: ClientOptions,
    workers_count: usize,
    callbacks: CallbacksRegistry<E>,
}

impl<E> ConsumerBuilder<E> {
    /// Create a builder for asynchronous version of `Consumer`.
    pub fn default_async() -> ConsumerBuilder<E> {
        ConsumerBuilder::default()
    }
}

impl<E> Default for ConsumerBuilder<E> {
    /// Create a builder for asynchronous version of `Consumer`.
    ///
    /// See [`ConsumerBuilder`](struct.ConsumerBuilder.html)
    fn default() -> Self {
        ConsumerBuilder {
            opts: ClientOptions::default(),
            workers_count: 1,
            callbacks: CallbacksRegistry::default(),
        }
    }
}

impl<E: 'static> ConsumerBuilder<E> {
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
        H: Send + Sync + Fn(Job) -> Pin<Box<dyn Future<Output = Result<(), E>> + Send>> + 'static,
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
    ) -> Result<Consumer<BufStream<S>, E>, Error> {
        self.opts.password = pwd;
        self.opts.is_worker = true;
        let buffered = BufStream::new(stream);
        let client = Client::new(buffered, self.opts).await?;
        Ok(Consumer::new(client, self.workers_count, self.callbacks).await)
    }

    /// Asynchronously connect to a Faktory server.
    ///
    /// See [`connect`](struct.ConsumerBuilder.html#structmethod.connect).
    pub async fn connect(
        self,
        url: Option<&str>,
    ) -> Result<Consumer<BufStream<TokioStream>, E>, Error> {
        let url = match url {
            Some(url) => url_parse(url),
            None => url_parse(&get_env_url()),
        }?;
        let stream = TokioStream::connect(host_from_url(&url)).await?;
        let buffered = BufStream::new(stream);
        let client = Client::new(buffered, self.opts).await?;
        Ok(Consumer::new(client, self.workers_count, self.callbacks).await)
    }
}
