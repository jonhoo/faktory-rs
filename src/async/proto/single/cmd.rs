use tokio::io::AsyncWriteExt;

use crate::{proto::Hello, Error};

#[async_trait::async_trait]
pub trait FaktoryCommand {
    async fn issue<W: AsyncWriteExt + Unpin + Send>(&self, w: &mut W) -> Result<(), Error>;
}

#[async_trait::async_trait]
impl FaktoryCommand for Hello {
    async fn issue<W: AsyncWriteExt + Unpin + Send>(&self, w: &mut W) -> Result<(), Error> {
        w.write_all(b"HELLO ").await?;

        let r = serde_json::to_vec(self).map_err(Error::Serialization)?;
        w.write(&r).await;
        Ok(w.write_all(b"\r\n").await?)
    }
}
