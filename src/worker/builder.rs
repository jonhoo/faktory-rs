use super::{runner::Closure, CallbacksRegistry, Client, Worker};
use crate::{
    proto::{utils, ClientOptions},
    Error, Job, JobRunner, Reconnect, TlsKind, WorkerId,
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
        self.callbacks.insert(kind.into(), Box::new(runner));
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
    pub async fn connect_with<S>(
        mut self,
        stream: S,
        pwd: Option<String>,
    ) -> Result<Worker<E>, Error>
    where
        S: AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static,
        BufStream<S>: Reconnect,
    {
        self.opts.password = pwd;
        self.opts.is_worker = true;
        let buffered = BufStream::new(stream);
        let client = Client::new(Box::new(buffered), self.opts).await?;
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
    pub async fn connect(self, url: Option<&str>) -> Result<Worker<E>, Error> {
        let url = utils::parse_provided_or_from_env(url)?;
        let addr = utils::host_from_url(&url);
        let stream = TokioStream::connect(addr).await?;
        match self.tls_kind {
            None => self.connect_with(stream, None).await,

            #[cfg(feature = "rustls")]
            Some(TlsKind::Rust) => {
                let hostname = url.host_str().unwrap().to_string();
                let tls_tream = crate::rustls::TlsStream::with_native_certs(
                    stream,
                    hostname,
                    self.skip_verify_server_certs,
                )
                .await?;
                self.connect_with(tls_tream, None).await
            }
            _ => unimplemented!(),
        }
    }
}
