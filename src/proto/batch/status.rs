#[cfg(doc)]
use super::Batch;

use super::BatchHandle;
use crate::{Client, Error};
use chrono::{DateTime, Utc};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};

// Not documented, but existing de fakto and also mentioned in the official client
// https://github.com/contribsys/faktory/blob/main/client/batch.go#L17-L19
/// State of a `callback` job of a [`Batch`].
#[derive(Copy, Clone, Debug, Deserialize, Eq, PartialEq)]
#[non_exhaustive]
pub enum CallbackState {
    /// Not enqueued yet.
    #[serde(rename = "")]
    Pending,
    /// Enqueued by the server, because the jobs belonging to this batch have finished executing.
    /// If a callback has been consumed, it's status is still `Enqueued`.
    /// If a callback has finished with failure, it's status remains `Enqueued`.
    #[serde(rename = "1")]
    Enqueued,
    /// The enqueued callback job has been consumed and successfully executed.
    #[serde(rename = "2")]
    FinishedOk,
}

impl std::fmt::Display for CallbackState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use CallbackState::*;
        let s = match self {
            Pending => "Pending",
            Enqueued => "Enqueued",
            FinishedOk => "FinishedOk",
        };
        write!(f, "{}", s)
    }
}

/// Batch status retrieved from Faktory server.
#[derive(Deserialize, Debug)]
pub struct BatchStatus {
    // Fields "bid", "created_at", "description", "total", "pending", and "failed"
    // are described in the docs: https://github.com/contribsys/faktory/wiki/Ent-Batches#status
    /// Id of this batch.
    pub bid: String,

    /// Batch creation date and time.
    pub created_at: DateTime<Utc>,

    /// Batch description, if any.
    pub description: Option<String>,

    /// Number of jobs in this batch.
    pub total: usize,

    /// Number of pending jobs.
    pub pending: usize,

    /// Number of failed jobs.
    pub failed: usize,

    // The official golang client also mentions "parent_bid', "complete_st", and "success_st":
    // https://github.com/contribsys/faktory/blob/main/client/batch.go#L8-L22
    /// Id of the parent batch, provided this batch is a child ("nested") batch.
    pub parent_bid: Option<String>,

    /// State of the `complete` callback.
    ///
    /// See [with_complete_callback](struct.BatchBuilder.html#method.with_complete_callback).
    #[serde(rename = "complete_st")]
    pub complete_callback_state: CallbackState,

    /// State of the `success` callback.
    ///
    /// See [with_success_callback](struct.BatchBuilder.html#method.with_success_callback).
    #[serde(rename = "success_st")]
    pub success_callback_state: CallbackState,
}

impl<'a> BatchStatus {
    /// Open the batch for which this `BatchStatus` has been retrieved.
    ///
    /// See [`open_batch`](Client::open_batch).
    pub async fn open<S: AsyncBufReadExt + AsyncWriteExt + Unpin + Send>(
        &self,
        prod: &'a mut Client<S>,
    ) -> Result<Option<BatchHandle<'a, S>>, Error> {
        prod.open_batch(self.bid.clone()).await
    }
}
