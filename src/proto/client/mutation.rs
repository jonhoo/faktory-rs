use crate::{
    proto::single::{MutationAction, MutationType},
    Client, Error, MutationFilter, MutationTarget,
};
use std::borrow::Borrow;

impl Client {
    /// Re-enqueue the jobs.
    ///
    /// This will immediately move the jobs from the targeted set (see [`MutationTarget`])
    /// to their queues. This will apply to the jobs satisfying the [`filter`](crate::MutationFilter).
    ///
    /// ```no_run
    /// # tokio_test::block_on(async {
    /// # use faktory::{JobId, Client, MutationTarget, MutationFilter};
    /// # let mut client = Client::connect().await.unwrap();
    /// let job_id1 = JobId::new("3sgE_qwtqw1501");
    /// let job_id2 = JobId::new("3sgE_qwtqw1502");
    /// let failed_ids = [&job_id1, &job_id2];
    /// let filter = MutationFilter::builder().jids(failed_ids.as_slice()).build();
    /// client.requeue(MutationTarget::Retries, &filter).await.unwrap();
    /// # });
    /// ```
    pub async fn requeue<'a, F>(&mut self, target: MutationTarget, filter: F) -> Result<(), Error>
    where
        F: Borrow<MutationFilter<'a>>,
    {
        self.issue(&MutationAction {
            cmd: MutationType::Requeue,
            target,
            filter: Some(filter.borrow()),
        })
        .await?
        .read_ok()
        .await
    }

    /*
    From Go bindings:

    // Move the given jobs from structure to the Dead set.
    // Faktory will not touch them anymore but you can still see them in the Web UI.
    //
    // Kill(Retries, OfType("DataSyncJob").WithJids("abc", "123"))
    Kill(name Structure, filter JobFilter) error

    // Move the given jobs to their associated queue so they can be immediately
    // picked up and processed.
    Requeue(name Structure, filter JobFilter) error

    // Throw away the given jobs, e.g. if you want to delete all jobs named "QuickbooksSyncJob"
    //
    //   Discard(Dead, OfType("QuickbooksSyncJob"))
    Discard(name Structure, filter JobFilter) error

    // Empty the entire given structure, e.g. if you want to clear all retries.
    // This is very fast as it is special cased by Faktory.
    Clear(name Structure) error
    */
}
