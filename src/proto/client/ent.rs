use super::super::batch::{CommitBatch, GetBatchStatus, OpenBatch};
use super::super::{single, BatchStatus, JobId, Progress, ProgressUpdate};
use super::{Client, ReadToken};
use crate::ent::{Batch, BatchHandle, BatchId};
use crate::error::{self, Error};
use crate::proto::FetchProgress;

impl Client {
    /// Send information on a job's execution progress to Faktory.
    pub async fn set_progress<P>(&mut self, upd: P) -> Result<(), Error>
    where
        P: AsRef<ProgressUpdate> + Sync,
    {
        self.issue(upd.as_ref()).await?.read_ok().await
    }

    /// Fetch information on a job's execution progress from Faktory.
    pub async fn get_progress<J>(&mut self, jid: J) -> Result<Option<Progress>, Error>
    where
        J: AsRef<JobId> + Sync,
    {
        let cmd = FetchProgress::new(jid);
        self.issue(&cmd).await?.read_json().await
    }

    /// Fetch information on a batch of jobs execution progress.
    pub async fn get_batch_status<B>(&mut self, bid: B) -> Result<Option<BatchStatus>, Error>
    where
        B: AsRef<BatchId> + Sync,
    {
        let cmd = GetBatchStatus::from(&bid);
        self.issue(&cmd).await?.read_json().await
    }

    /// Initiate a new batch of jobs.
    pub async fn start_batch(&mut self, batch: Batch) -> Result<BatchHandle<'_>, Error> {
        let bid = self.issue(&batch).await?.read_bid().await?;
        Ok(BatchHandle::new(bid, self))
    }

    /// Open an already existing batch of jobs.
    ///
    /// This will not error if a batch with the provided `bid` does not exist,
    /// rather `Ok(None)` will be returned.
    pub async fn open_batch<B>(&mut self, bid: B) -> Result<Option<BatchHandle<'_>>, Error>
    where
        B: AsRef<BatchId> + Sync,
    {
        let bid = self.issue(&OpenBatch::from(bid)).await?.maybe_bid().await?;
        Ok(bid.map(|bid| BatchHandle::new(bid, self)))
    }

    pub(crate) async fn commit_batch<B>(&mut self, bid: B) -> Result<(), Error>
    where
        B: AsRef<BatchId> + Sync,
    {
        self.issue(&CommitBatch::from(bid)).await?.read_ok().await
    }
}

impl ReadToken<'_> {
    pub(crate) async fn read_bid(self) -> Result<BatchId, Error> {
        single::read_bid(&mut self.0.stream).await
    }

    pub(crate) async fn maybe_bid(self) -> Result<Option<BatchId>, Error> {
        match single::read_bid(&mut self.0.stream).await {
            Ok(bid) => Ok(Some(bid)),
            Err(Error::Protocol(error::Protocol::Internal { msg })) => {
                if msg.starts_with("No such batch") {
                    return Ok(None);
                }
                Err(error::Protocol::Internal { msg }.into())
            }
            Err(another) => Err(another),
        }
    }
}
