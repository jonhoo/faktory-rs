use crate::error::Error;
use crate::proto::{single::FaktoryCommand, Batch, BatchId};
use tokio::io::AsyncWriteExt;

#[async_trait::async_trait]
impl FaktoryCommand for Batch {
    async fn issue<W: AsyncWriteExt + Unpin + Send>(&self, w: &mut W) -> Result<(), Error> {
        w.write_all(b"BATCH NEW ").await?;
        let r = serde_json::to_vec(self).map_err(Error::Serialization)?;
        w.write_all(&r).await?;
        Ok(w.write_all(b"\r\n").await?)
    }
}

macro_rules! batch_cmd {
    ($structure:ident, $cmd:expr) => {
        impl From<BatchId> for $structure {
            fn from(value: BatchId) -> Self {
                $structure(value)
            }
        }

        #[async_trait::async_trait]
        impl FaktoryCommand for $structure {
            async fn issue<W: AsyncWriteExt + Unpin + Send>(&self, w: &mut W) -> Result<(), Error> {
                w.write_all(b"BATCH ").await?;
                w.write_all($cmd.as_bytes()).await?;
                w.write_all(b" ").await?;
                w.write_all(self.0.as_bytes()).await?;
                Ok(w.write_all(b"\r\n").await?)
            }
        }
    };
}

pub(crate) struct CommitBatch(BatchId);
batch_cmd!(CommitBatch, "COMMIT");

pub(crate) struct GetBatchStatus(BatchId);
batch_cmd!(GetBatchStatus, "STATUS");

pub struct OpenBatch(BatchId);
batch_cmd!(OpenBatch, "OPEN");
