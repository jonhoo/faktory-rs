use super::ProgressUpdate;
use crate::error::Error;
use crate::proto::{single::FaktoryCommand, JobId};
use tokio::io::{AsyncWrite, AsyncWriteExt};

#[derive(Debug, Clone)]
pub enum Track {
    Set(ProgressUpdate),
    Get(JobId),
}

#[async_trait::async_trait]
impl FaktoryCommand for Track {
    async fn issue<W: AsyncWrite + Unpin + Send>(&self, w: &mut W) -> Result<(), Error> {
        match self {
            Self::Set(upd) => {
                w.write_all(b"TRACK SET ").await?;
                let r = serde_json::to_vec(upd).map_err(Error::Serialization)?;
                w.write_all(&r).await?;
                Ok(w.write_all(b"\r\n").await?)
            }
            Self::Get(jid) => {
                w.write_all(b"TRACK GET ").await?;
                w.write_all(jid.as_bytes()).await?;
                Ok(w.write_all(b"\r\n").await?)
            }
        }
    }
}
