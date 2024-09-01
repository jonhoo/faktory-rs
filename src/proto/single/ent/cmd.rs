use super::ProgressUpdate;
use crate::error::Error;
use crate::proto::{single::FaktoryCommand, JobId};
use tokio::io::{AsyncWrite, AsyncWriteExt};

#[async_trait::async_trait]
impl FaktoryCommand for ProgressUpdate {
    async fn issue<W: AsyncWrite + Unpin + Send>(&self, w: &mut W) -> Result<(), Error> {
        w.write_all(b"TRACK SET ").await?;
        let r = serde_json::to_vec(self).map_err(Error::Serialization)?;
        w.write_all(&r).await?;
        Ok(w.write_all(b"\r\n").await?)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct FetchProgress<J>(J);

impl<J> FetchProgress<J> {
    pub fn new(j: J) -> Self {
        Self(j)
    }
}

#[async_trait::async_trait]
impl<J> FaktoryCommand for FetchProgress<J>
where
    J: AsRef<JobId> + Sync,
{
    async fn issue<W: AsyncWrite + Unpin + Send>(&self, w: &mut W) -> Result<(), Error> {
        w.write_all(b"TRACK GET ").await?;
        w.write_all(self.0.as_ref().as_bytes()).await?;
        Ok(w.write_all(b"\r\n").await?)
    }
}
