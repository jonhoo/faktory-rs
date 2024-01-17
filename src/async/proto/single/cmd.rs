use tokio::io::AsyncWriteExt;

use crate::{
    proto::{Ack, Fail, Fetch, Heartbeat, Hello, Info, Push, QueueAction, QueueControl},
    Error,
};

#[async_trait::async_trait]
pub trait AsyncFaktoryCommand {
    async fn issue<W: AsyncWriteExt + Unpin + Send>(&self, w: &mut W) -> Result<(), Error>;
}

#[async_trait::async_trait]
impl AsyncFaktoryCommand for Info {
    async fn issue<W: AsyncWriteExt + Unpin + Send>(&self, w: &mut W) -> Result<(), Error> {
        Ok(w.write_all(b"INFO\r\n").await?)
    }
}

#[async_trait::async_trait]
impl AsyncFaktoryCommand for Push {
    async fn issue<W: AsyncWriteExt + Unpin + Send>(&self, w: &mut W) -> Result<(), Error> {
        w.write_all(b"PUSH ").await?;
        let r = serde_json::to_vec(&**self).map_err(Error::Serialization)?;
        w.write_all(&r).await?;
        Ok(w.write_all(b"\r\n").await?)
    }
}

#[async_trait::async_trait]
impl<'a, Q> AsyncFaktoryCommand for Fetch<'a, Q>
where
    Q: AsRef<str> + Sync,
{
    async fn issue<W: AsyncWriteExt + Unpin + Send>(&self, w: &mut W) -> Result<(), Error> {
        w.write_all(b"FETCH").await?;
        for q in self.queues {
            w.write_all(b" ").await?;
            w.write_all(q.as_ref().as_bytes()).await?;
        }
        Ok(w.write_all(b"\r\n").await?)
    }
}

#[async_trait::async_trait]
impl<Q> AsyncFaktoryCommand for QueueControl<'_, Q>
where
    Q: AsRef<str> + Sync,
{
    async fn issue<W: AsyncWriteExt + Unpin + Send>(&self, w: &mut W) -> Result<(), Error> {
        let command = match self.action {
            QueueAction::Pause => b"QUEUE PAUSE".as_ref(),
            QueueAction::Resume => b"QUEUE RESUME".as_ref(),
        };
        w.write_all(command).await?;
        for q in self.queues {
            w.write_all(b" ").await?;
            w.write_all(q.as_ref().as_bytes()).await?;
        }
        Ok(w.write_all(b"\r\n").await?)
    }
}

macro_rules! self_to_cmd {
    ($struct:ident) => {
        #[async_trait::async_trait]
        impl AsyncFaktoryCommand for $struct {
            async fn issue<W: AsyncWriteExt + Unpin + Send>(&self, w: &mut W) -> Result<(), Error> {
                let c = format!("{} ", stringify!($struct).to_uppercase());
                w.write_all(c.as_bytes()).await?;
                let r = serde_json::to_vec(self).map_err(Error::Serialization)?;
                w.write_all(&r).await?;
                Ok(w.write_all(b"\r\n").await?)
            }
        }
    };
}

self_to_cmd!(Hello);
self_to_cmd!(Ack);
self_to_cmd!(Fail);
self_to_cmd!(Heartbeat);
