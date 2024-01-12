use crate::proto::ClientOptions;
use crate::Error;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, Result as TokioIOResult};
use tokio::net::TcpStream as TokioStream;

pub struct Client<S: AsyncBufReadExt + AsyncWriteExt> {
    stream: S,
    opts: ClientOptions,
}

/// A stream that can be re-established after failing.
#[async_trait::async_trait]
pub trait Reconnect: Sized {
    /// Re-establish the stream.
    async fn reconnect(&self) -> TokioIOResult<Self>;
}

#[async_trait::async_trait]
impl Reconnect for TokioStream {
    async fn reconnect(&self) -> TokioIOResult<Self> {
        TokioStream::connect(self.peer_addr().expect("socket address")).await
    }
}

impl<S> Client<S>
where
    S: AsyncBufReadExt + AsyncWriteExt + Reconnect,
{
    pub(crate) async fn connect_again(&mut self) -> Result<Self, Error> {
        let s = self.stream.reconnect().await?;
        Client::new(s, self.opts.clone()).await
    }

    pub(crate) async fn reconnect(&mut self) -> Result<(), Error> {
        self.stream = self.stream.reconnect().await?;
        self.init().await
    }
}

impl<S> Client<S>
where
    S: AsyncBufReadExt + AsyncWriteExt,
{
    async fn init(&mut self) -> Result<(), Error> {
        Ok(())
    }

    pub(crate) async fn new(stream: S, opts: ClientOptions) -> Result<Client<S>, Error> {
        let mut c = Client { stream, opts };
        c.init().await?;
        Ok(c)
    }

    pub(crate) async fn new_producer(stream: S, pwd: Option<String>) -> Result<Client<S>, Error> {
        let mut opts = ClientOptions::default_for_producer();
        opts.password = pwd;
        Client::new(stream, opts).await
    }
}
