use super::super::{single, BatchStatus, GetBatchStatus, Progress, ProgressUpdate, Track};
use super::{Client, ReadToken};
use crate::error::Error;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};

impl<S: AsyncBufReadExt + AsyncWriteExt + Unpin + Send> Client<S> {
    /// Send information on a job's execution progress to Faktory.
    pub async fn set_progress(&mut self, upd: ProgressUpdate) -> Result<(), Error> {
        let cmd = Track::Set(upd);
        self.issue(&cmd).await?.read_ok().await
    }

    /// Fetch information on a job's execution progress from Faktory.
    pub async fn get_progress(&mut self, jid: String) -> Result<Option<Progress>, Error> {
        let cmd = Track::Get(jid);
        self.issue(&cmd).await?.read_json().await
    }

    /// Fetch information on a batch of jobs execution progress.
    pub async fn get_batch_status(&mut self, bid: String) -> Result<Option<BatchStatus>, Error> {
        let cmd = GetBatchStatus::from(bid);
        self.issue(&cmd).await?.read_json().await
    }
}

impl<'a, S: AsyncBufReadExt + AsyncWriteExt + Unpin + Send> ReadToken<'a, S> {
    pub(crate) async fn read_bid(self) -> Result<String, Error> {
        single::read_bid(&mut self.0.stream).await
    }

    pub(crate) async fn maybe_bid(self) -> Result<Option<String>, Error> {
        use crate::error;

        let bid_read_res = single::read_bid(&mut self.0.stream).await;
        if bid_read_res.is_ok() {
            return Ok(Some(bid_read_res.unwrap()));
        }
        match bid_read_res.unwrap_err() {
            Error::Protocol(error::Protocol::Internal { msg }) => {
                if msg.starts_with("No such batch") {
                    return Ok(None);
                }
                return Err(error::Protocol::Internal { msg }.into());
            }
            another => Err(another),
        }
    }
}
