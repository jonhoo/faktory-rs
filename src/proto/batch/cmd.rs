use crate::error::Error;
use crate::proto::{single::FaktoryCommand, Batch, BatchId};
use tokio::io::{AsyncWrite, AsyncWriteExt};

#[async_trait::async_trait]
impl FaktoryCommand for Batch {
    async fn issue<W: AsyncWrite + Unpin + Send>(&self, w: &mut W) -> Result<(), Error> {
        w.write_all(b"BATCH NEW ").await?;
        let r = serde_json::to_vec(self).map_err(Error::Serialization)?;
        w.write_all(&r).await?;
        Ok(w.write_all(b"\r\n").await?)
    }
}

macro_rules! batch_cmd {
    ($structure:ident, $cmd:expr) => {
        impl<B: AsRef<BatchId>> From<B> for $structure<B> {
            fn from(value: B) -> Self {
                $structure(value)
            }
        }

        #[async_trait::async_trait]
        impl<B> FaktoryCommand for $structure<B>
        where
            B: AsRef<BatchId> + Sync,
        {
            async fn issue<W: AsyncWrite + Unpin + Send>(&self, w: &mut W) -> Result<(), Error> {
                w.write_all(b"BATCH ").await?;
                w.write_all($cmd.as_bytes()).await?;
                w.write_all(b" ").await?;
                w.write_all(self.0.as_ref().as_bytes()).await?;
                Ok(w.write_all(b"\r\n").await?)
            }
        }
    };
}

pub(crate) struct CommitBatch<B>(B)
where
    B: AsRef<BatchId>;
batch_cmd!(CommitBatch, "COMMIT");

pub(crate) struct GetBatchStatus<B>(B)
where
    B: AsRef<BatchId>;
batch_cmd!(GetBatchStatus, "STATUS");

pub(crate) struct OpenBatch<B>(B)
where
    B: AsRef<BatchId>;
batch_cmd!(OpenBatch, "OPEN");
