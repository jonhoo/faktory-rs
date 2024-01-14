use tokio::io::AsyncWriteExt;

use crate::{
    proto::{Fetch, Hello, Push},
    Error,
};

#[async_trait::async_trait]
pub trait FaktoryCommand {
    async fn issue<W: AsyncWriteExt + Unpin + Send>(&self, w: &mut W) -> Result<(), Error>;
}

#[async_trait::async_trait]
impl FaktoryCommand for Hello {
    async fn issue<W: AsyncWriteExt + Unpin + Send>(&self, w: &mut W) -> Result<(), Error> {
        w.write_all(b"HELLO ").await?;
        let r = serde_json::to_vec(self).map_err(Error::Serialization)?;
        w.write(&r).await?;
        Ok(w.write_all(b"\r\n").await?)
    }
}

#[async_trait::async_trait]
impl FaktoryCommand for Push {
    async fn issue<W: AsyncWriteExt + Unpin + Send>(&self, w: &mut W) -> Result<(), Error> {
        w.write_all(b"PUSH ").await?;
        let r = serde_json::to_vec(&**self).map_err(Error::Serialization)?;
        w.write(&r).await?;
        Ok(w.write_all(b"\r\n").await?)
    }
}

#[async_trait::async_trait]
impl<'a, Q> FaktoryCommand for Fetch<'a, Q>
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
