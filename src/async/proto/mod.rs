use crate::proto::{
    ClientOptions, End, Fetch, Heartbeat, HeartbeatStatus, Hello, EXPECTED_PROTOCOL_VERSION,
};
use crate::{error, Error, Job};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tokio::net::TcpStream as TokioStream;

mod single;

pub struct AsyncClient<S: AsyncBufReadExt + AsyncWriteExt + Send + Unpin> {
    stream: S,
    opts: ClientOptions,
}

/// A stream that can be re-established after failing.
#[async_trait::async_trait]
pub trait AsyncReconnect: Sized {
    /// Re-establish the stream.
    async fn reconnect(&self) -> Result<Self, Error>;
}

#[async_trait::async_trait]
impl AsyncReconnect for TokioStream {
    async fn reconnect(&self) -> Result<Self, Error> {
        Ok(TokioStream::connect(self.peer_addr().expect("socket address")).await?)
    }
}

impl<S> AsyncClient<S>
where
    S: AsyncBufReadExt + AsyncWriteExt + Unpin + Send + AsyncReconnect,
{
    pub(crate) async fn connect_again(&mut self) -> Result<Self, Error> {
        let s = self.stream.reconnect().await?;
        AsyncClient::new(s, self.opts.clone()).await
    }

    pub(crate) async fn reconnect(&mut self) -> Result<(), Error> {
        self.stream = self.stream.reconnect().await?;
        self.init().await
    }
}

impl<S> Drop for AsyncClient<S>
where
    S: AsyncBufReadExt + AsyncWriteExt + Unpin + Send,
{
    fn drop(&mut self) {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                single::write_command(&mut self.stream, &End).await.unwrap();
            })
        });
    }
}

impl<S> AsyncClient<S>
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

    pub(crate) async fn new(stream: S, opts: ClientOptions) -> Result<AsyncClient<S>, Error> {
        let mut c = AsyncClient { stream, opts };
        c.init().await?;
        Ok(c)
    }

    pub(crate) async fn new_producer(
        stream: S,
        pwd: Option<String>,
    ) -> Result<AsyncClient<S>, Error> {
        let mut opts = ClientOptions::default_for_producer();
        opts.password = pwd;
        AsyncClient::new(stream, opts).await
    }

    pub(crate) async fn issue<FC: single::AsyncFaktoryCommand>(
        &mut self,
        c: &FC,
    ) -> Result<ReadToken<'_, S>, Error> {
        single::write_command(&mut self.stream, c).await?;
        Ok(ReadToken(self))
    }

    pub(crate) async fn fetch<Q>(&mut self, queues: &[Q]) -> Result<Option<Job>, Error>
    where
        Q: AsRef<str> + Sync,
    {
        self.issue(&Fetch::from(queues)).await?.read_json().await
    }

    pub(crate) async fn heartbeat(&mut self) -> Result<HeartbeatStatus, Error> {
        single::write_command(
            &mut self.stream,
            &Heartbeat::new(&**self.opts.wid.as_ref().unwrap()),
        )
        .await?;

        match single::read_json::<_, serde_json::Value>(&mut self.stream).await? {
            None => Ok(HeartbeatStatus::Ok),
            Some(s) => match s
                .as_object()
                .and_then(|m| m.get("state"))
                .and_then(|s| s.as_str())
            {
                Some("terminate") => Ok(HeartbeatStatus::Terminate),
                Some("quiet") => Ok(HeartbeatStatus::Quiet),
                _ => Err(error::Protocol::BadType {
                    expected: "heartbeat response",
                    received: format!("{}", s),
                }
                .into()),
            },
        }
    }
}

pub struct ReadToken<'a, S: AsyncBufReadExt + AsyncWriteExt + Unpin + Send>(&'a mut AsyncClient<S>);

impl<'a, S: AsyncBufReadExt + AsyncWriteExt + Unpin + Send> ReadToken<'a, S> {
    pub(crate) async fn read_ok(self) -> Result<(), Error> {
        single::read_ok(&mut self.0.stream).await
    }

    pub(crate) async fn read_json<T>(self) -> Result<Option<T>, Error>
    where
        T: serde::de::DeserializeOwned,
    {
        single::read_json(&mut self.0.stream).await
    }
}
