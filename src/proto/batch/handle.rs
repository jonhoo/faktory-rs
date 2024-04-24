use crate::error::Error;
use crate::proto::{Batch, BatchId, Client, Job};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};

/// Represents a newly started or re-opened batch of jobs.
pub struct BatchHandle<'a, S: AsyncBufReadExt + AsyncWriteExt + Unpin + Send> {
    bid: BatchId,
    c: &'a mut Client<S>,
}

impl<'a, S: AsyncBufReadExt + AsyncWriteExt + Unpin + Send> BatchHandle<'a, S> {
    /// ID issued by the Faktory server to this batch.
    pub fn id(&self) -> &BatchId {
        &self.bid
    }

    pub(crate) fn new(bid: BatchId, c: &mut Client<S>) -> BatchHandle<'_, S> {
        BatchHandle { bid, c }
    }

    /// Add the given job to the batch.
    ///
    /// Should the submitted job - for whatever reason - already have a `bid` key present in its custom hash,
    /// this value will be overwritten by the ID of the batch this job is being added to with the old value
    /// returned as `Some(<old value here>)`.
    pub async fn add(&mut self, mut job: Job) -> Result<Option<serde_json::Value>, Error> {
        let bid = job.custom.insert("bid".into(), self.bid.clone().into());
        self.c.enqueue(job).await.map(|_| bid)
    }

    /// Initiate a child batch of jobs.
    pub async fn start_batch(&mut self, mut batch: Batch) -> Result<BatchHandle<'_, S>, Error> {
        batch.parent_bid = Some(self.bid.clone());
        self.c.start_batch(batch).await
    }

    /// Commit this batch.
    ///
    /// The Faktory server will not queue any callbacks, unless the batch is committed.
    /// Committing an empty batch will make the server queue the callback(s) right away.
    /// Once committed, the batch can still be re-opened with [open_batch](Client::open_batch),
    /// and extra jobs can be added to it.
    pub async fn commit(self) -> Result<(), Error> {
        self.c.commit_batch(&self.bid).await
    }
}
