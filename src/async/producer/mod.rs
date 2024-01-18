use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufStream};
use tokio::net::TcpStream as TokioStream;

use crate::proto::{Info, Push, QueueAction, QueueControl};
use crate::Job;
use crate::{
    proto::{get_env_url, host_from_url, url_parse},
    Error,
};

use super::proto::AsyncClient;

/// `Producer` is used to enqueue new jobs that will in turn be processed by Faktory workers.
pub struct AsyncProducer<S: AsyncBufReadExt + AsyncWriteExt + Send + Unpin> {
    c: AsyncClient<S>,
}

impl<S: AsyncBufReadExt + AsyncWriteExt + Send + Unpin> AsyncProducer<S> {
    /// Connect to a Faktory server with a non-standard stream.
    pub async fn connect_with(stream: S, pwd: Option<String>) -> Result<AsyncProducer<S>, Error> {
        Ok(AsyncProducer {
            c: AsyncClient::new_producer(stream, pwd).await?,
        })
    }

    /// Asynchronously enqueue the given job on the Faktory server.
    ///
    /// Returns `Ok` if the job was successfully queued by the Faktory server.
    pub async fn enqueue(&mut self, job: Job) -> Result<(), Error> {
        self.c.issue(&Push::from(job)).await?.read_ok().await
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
}

impl AsyncProducer<BufStream<TokioStream>> {
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
        let url = match url {
            Some(url) => url_parse(url),
            None => url_parse(&get_env_url()),
        }?;
        let stream = TokioStream::connect(host_from_url(&url)).await?;
        let buffered = BufStream::new(stream);
        Self::connect_with(buffered, url.password().map(|p| p.to_string())).await
    }
}
