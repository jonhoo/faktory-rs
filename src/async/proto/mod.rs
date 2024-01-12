use crate::proto::{ClientOptions, Hello, EXPECTED_PROTOCOL_VERSION};
use crate::{error, Error};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, Result as TokioIOResult};
use tokio::net::TcpStream as TokioStream;

mod single;

pub struct Client<S: AsyncBufReadExt + AsyncWriteExt + Send> {
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
    S: AsyncBufReadExt + AsyncWriteExt + Unpin + Send + Reconnect,
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
    S: AsyncBufReadExt + AsyncWriteExt + Unpin + Send,
{
    async fn init(&mut self) -> Result<(), Error> {
        let hi = single::read_hi(&mut self.stream).await?;
        if hi.version != EXPECTED_PROTOCOL_VERSION {
            return Err(error::Connect::VersionMismatch {
                ours: EXPECTED_PROTOCOL_VERSION,
                theirs: hi.version,
            }
            .into());
        }

        // fill in any missing options, and remember them for re-connect
        let mut hello = Hello::default();
        if !self.opts.is_producer() {
            let hostname = self
                .opts
                .hostname
                .clone()
                .or_else(|| hostname::get().ok()?.into_string().ok())
                .unwrap_or_else(|| "local".to_string());
            self.opts.hostname = Some(hostname);
            let pid = self.opts.pid.unwrap_or_else(|| std::process::id() as usize);
            self.opts.pid = Some(pid);
            let wid = self.opts.wid.clone().unwrap_or_else(|| {
                use rand::{thread_rng, Rng};
                thread_rng()
                    .sample_iter(&rand::distributions::Alphanumeric)
                    .map(char::from)
                    .take(32)
                    .collect()
            });
            self.opts.wid = Some(wid);

            hello.hostname = Some(self.opts.hostname.clone().unwrap());
            hello.wid = Some(self.opts.wid.clone().unwrap());
            hello.pid = Some(self.opts.pid.unwrap());
            hello.labels = self.opts.labels.clone();
        }

        if hi.salt.is_some() {
            if let Some(ref pwd) = self.opts.password {
                hello.set_password(&hi, pwd);
            } else {
                return Err(error::Connect::AuthenticationNeeded.into());
            }
        }

        single::write_command_and_await_ok(&mut self.stream, &hello).await?;
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
