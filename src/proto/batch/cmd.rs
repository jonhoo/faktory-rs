use crate::ent::Batch;
use crate::proto::single::FaktoryCommand;
use crate::Error;
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
        impl From<String> for $structure {
            fn from(value: String) -> Self {
                $structure(value)
            }
        }

        #[async_trait::async_trait]
        impl FaktoryCommand for $structure {
            async fn issue<W: AsyncWriteExt + Unpin + Send>(&self, w: &mut W) -> Result<(), Error> {
                let c = format!("BATCH {} ", $cmd);
                w.write_all(c.as_bytes()).await?;
                w.write_all(self.0.as_bytes()).await?;
                Ok(w.write_all(b"\r\n").await?)
            }
        }
    };
}

pub struct CommitBatch(String);
batch_cmd!(CommitBatch, "COMMIT");

pub struct GetBatchStatus(String);
batch_cmd!(GetBatchStatus, "STATUS");

pub struct OpenBatch(String);
batch_cmd!(OpenBatch, "OPEN");
