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
                if is_batch_not_found_error(&msg) {
                    return Ok(None);
                }
                Err(error::Protocol::Internal { msg }.into())
            }
            Err(another) => Err(another),
        }
    }
}

pub(crate) fn is_batch_not_found_error(msg: &str) -> bool {
    msg.get(..13)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("no such batch"))
}

#[cfg(test)]
mod test {
    use super::is_batch_not_found_error;

    #[test]
    fn batch_does_not_exist_message_identified_correctly() {
        // in Ent Faktory 1.9.1 the error signalling that batch does not exist
        // started with an upper-cased "N": "No such batch <batch_id>", and in
        // 1.9.2 it was changed to be a lowercase one; in order to identify this
        // message correctly, we are comparing case-insensitively with the
        // "well-known" prefix (observed rather, since the source code of Ent
        // Faktory is currently not available, and this message is not mentioned
        // in the docs, neither in the official Go bindings)

        // 'non-existent-batch-id' - is ID of a batch we are
        // using in one of our end-to-end tests
        assert!(is_batch_not_found_error(
            "No such batch non-existent-batch-id"
        ));
        assert!(is_batch_not_found_error("no such batch"));
        assert!(is_batch_not_found_error("NO SUCH BATCH"));

        assert!(!is_batch_not_found_error("not found"));
        assert!(!is_batch_not_found_error("invalid"));
        assert!(!is_batch_not_found_error("any other error"));
    }
}
