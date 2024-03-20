#[cfg(doc)]
use crate::Client;

use crate::proto::{BatchId, Job};
use derive_builder::Builder;

mod cmd;
mod handle;
mod status;

pub use cmd::{CommitBatch, GetBatchStatus, OpenBatch};
pub use handle::BatchHandle;
pub use status::{BatchStatus, CallbackState};

/// Batch of jobs.
///
/// Faktory guarantees a callback (`success` and/or `failure`) will be triggered after the execution
/// of all the jobs belonging to the same batch has finished (successfully or with errors accordingly).
/// The 'complete' callback will always be queued first.
///
/// Batches can be nested. They can also be re-opened, but - once a batch is committed - only those jobs
/// that belong to this batch can re-open it.
///
/// An empty batch can be committed just fine. That will make Faktory immediately fire a callback (i.e. put
/// the job specified in `complete` and/or the one specified in `success` onto the queues).
///
/// If you open a batch, but - for some reason - do not commit it within _30 minutes_, it will simply expire
/// on the Faktory server (which means no callbackes will be fired).
///
/// Here is how you can create a simple batch:
/// ```no_run
/// # tokio_test::block_on(async {
/// # use faktory::Error;
/// use faktory::{Client, Job, ent::Batch};
///
/// let mut cl = Client::connect(None).await?;
/// let job1 = Job::builder("job_type").build();
/// let job2 = Job::builder("job_type").build();
/// let job_cb = Job::builder("callback_job_type").build();
///
/// let batch = Batch::builder()
///     .description("Batch description")
///     .with_complete_callback(job_cb);
///
/// let mut batch = cl.start_batch(batch).await?;
/// batch.add(job1).await?;
/// batch.add(job2).await?;
/// batch.commit().await?;
///
/// # Ok::<(), Error>(())
/// # });
/// ```
///
/// Nested batches are also supported:
/// ```no_run
/// # tokio_test::block_on(async {
/// # use faktory::{Client, Job, Error};
/// # use faktory::ent::Batch;
/// # let mut cl = Client::connect(None).await?;
/// let parent_job1 = Job::builder("job_type").build();
/// let parent_job2 = Job::builder("another_job_type").build();
/// let parent_cb = Job::builder("callback_job_type").build();
///
/// let child_job1 = Job::builder("job_type").build();
/// let child_job2 = Job::builder("yet_another_job_type").build();
/// let child_cb = Job::builder("callback_job_type").build();
///
/// let parent_batch = Batch::builder()
///     .description("Batch description")
///     .with_complete_callback(parent_cb);
/// let child_batch = Batch::builder()
///     .description("Child batch description")
///     .with_success_callback(child_cb);
///
/// let mut parent = cl.start_batch(parent_batch).await?;
/// parent.add(parent_job1).await?;
/// parent.add(parent_job2).await?;
/// let mut child = parent.start_batch(child_batch).await?;
/// child.add(child_job1).await?;
/// child.add(child_job2).await?;
///
/// child.commit().await?;
/// parent.commit().await?;
///
/// # Ok::<(), Error>(())
/// });
/// ```
///
/// In the example above, there is a single level nesting, but you can nest those batches as deep as you wish,
/// effectively building a pipeline this way, since the Faktory guarantees that callback jobs will not be queued unless
/// the batch gets committed.
///
/// You can retieve the batch status using a [`Client`]:
/// ```no_run
/// # use faktory::Error;
/// # use faktory::{Job, Client};
/// # use faktory::ent::{Batch, CallbackState};
/// # tokio_test::block_on(async {
/// let mut cl = Client::connect(None).await?;
/// let job = Job::builder("job_type").build();
/// let cb_job = Job::builder("callback_job_type").build();
/// let b = Batch::builder()
///     .description("Batch description")
///     .with_complete_callback(cb_job);
///
/// let mut b = cl.start_batch(b).await?;
/// let bid = b.id().to_owned();
/// b.add(job).await?;
/// b.commit().await?;
///
/// let mut t = Client::connect(None).await?;
/// let s = t.get_batch_status(bid).await?.unwrap();
/// assert_eq!(s.total, 1);
/// assert_eq!(s.pending, 1);
/// assert_eq!(s.description, Some("Batch description".into()));
///
/// match s.complete_callback_state {
///     CallbackState::Pending => {},
///     _ => panic!("The jobs of this batch have not executed, so the callback job is expected to _not_ have fired"),
/// }
/// # Ok::<(), Error>(())
/// });
/// ```
#[derive(Builder, Debug, Serialize)]
#[builder(
    custom_constructor,
    pattern = "owned",
    setter(into),
    build_fn(name = "try_build", private)
)]
pub struct Batch {
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(skip))]
    parent_bid: Option<BatchId>,

    /// Batch description for Faktory WEB UI.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(custom), default = "None")]
    pub description: Option<String>,

    /// On success callback.
    ///
    /// This job will be queued by the Faktory server provided
    /// all the jobs belonging to this batch have been executed successfully.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(skip))]
    pub(crate) success: Option<Job>,

    /// On complete callback.
    ///
    /// This job will be queue by the Faktory server after all the jobs
    /// belonging to this batch have been executed, even if one/some/all
    /// of the workers have failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(skip))]
    pub(crate) complete: Option<Job>,
}

impl Batch {
    /// Create a new `BatchBuilder`.
    pub fn builder() -> BatchBuilder {
        BatchBuilder::new()
    }
}

impl BatchBuilder {
    fn build(self) -> Batch {
        self.try_build().expect("There are no required fields.")
    }

    /// Create a new `BatchBuilder` with optional description of the batch.
    pub fn new() -> BatchBuilder {
        Self::create_empty()
    }

    /// Batch description for Faktory WEB UI.
    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(Some(description.into()));
        self
    }

    /// Create a `Batch` with only `success` callback specified.
    pub fn with_success_callback(self, success_cb: Job) -> Batch {
        let mut b = self.build();
        b.success = Some(success_cb);
        b
    }

    /// Create a `Batch` with only `complete` callback specified.
    pub fn with_complete_callback(self, complete_cb: Job) -> Batch {
        let mut b = self.build();
        b.complete = Some(complete_cb);
        b
    }

    /// Create a `Batch` with both `success` and `complete` callbacks specified.
    pub fn with_callbacks(self, success_cb: Job, complete_cb: Job) -> Batch {
        let mut b = self.build();
        b.success = Some(success_cb);
        b.complete = Some(complete_cb);
        b
    }
}

impl Clone for BatchBuilder {
    fn clone(&self) -> Self {
        BatchBuilder {
            parent_bid: self.parent_bid.clone(),
            description: self.description.clone(),
            success: self.success.clone(),
            complete: self.complete.clone(),
        }
    }
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use chrono::{DateTime, Utc};

    use super::*;

    #[test]
    fn test_batch_creation() {
        let b = BatchBuilder::new()
            .description("Image processing batch")
            .with_success_callback(Job::builder("thumbnail").build());

        assert!(b.complete.is_none());
        assert!(b.parent_bid.is_none());
        assert!(b.success.is_some());
        assert_eq!(b.description, Some("Image processing batch".into()));

        let b = BatchBuilder::new()
            .description("Image processing batch")
            .with_complete_callback(Job::builder("thumbnail").build());
        assert!(b.complete.is_some());
        assert!(b.success.is_none());

        let b = BatchBuilder::new().with_callbacks(
            Job::builder("thumbnail").build(),
            Job::builder("thumbnail").build(),
        );
        assert!(b.description.is_none());
        assert!(b.complete.is_some());
        assert!(b.success.is_some());

        let b = BatchBuilder::new().description("Batch description");
        let _batch_with_complete_cb = b.clone().with_complete_callback(Job::builder("jt").build());
        let _batch_with_success_cb = b.with_success_callback(Job::builder("jt").build());
    }

    #[test]
    fn test_batch_serialized_correctly() {
        let prepare_test_job = |jobtype: String| {
            let jid = "LFluKy1Baak83p54";
            let dt = "2023-12-22T07:00:52.546258624Z";
            let created_at = DateTime::<Utc>::from_str(dt).unwrap();
            Job::builder(jobtype)
                .jid(jid)
                .created_at(created_at)
                .build()
        };

        // with description and on success callback:
        let got = serde_json::to_string(
            &BatchBuilder::new()
                .description("Image processing workload")
                .with_success_callback(prepare_test_job("thumbnail_clean_up".into())),
        )
        .unwrap();
        let expected = if cfg!(feature = "ent") {
            r#"{"description":"Image processing workload","success":{"jid":"LFluKy1Baak83p54","queue":"default","jobtype":"thumbnail_clean_up","args":[],"created_at":"2023-12-22T07:00:52.546258624Z","reserve_for":600,"retry":25,"priority":5,"backtrace":0,"custom":{"track":1}}}"#
        } else {
            r#"{"description":"Image processing workload","success":{"jid":"LFluKy1Baak83p54","queue":"default","jobtype":"thumbnail_clean_up","args":[],"created_at":"2023-12-22T07:00:52.546258624Z","reserve_for":600,"retry":25,"priority":5,"backtrace":0}}"#
        };
        assert_eq!(got, expected);

        // without description and with on complete callback:
        let got = serde_json::to_string(
            &BatchBuilder::new().with_complete_callback(prepare_test_job("thumbnail_info".into())),
        )
        .unwrap();
        let expected = if cfg!(feature = "ent") {
            r#"{"complete":{"jid":"LFluKy1Baak83p54","queue":"default","jobtype":"thumbnail_info","args":[],"created_at":"2023-12-22T07:00:52.546258624Z","reserve_for":600,"retry":25,"priority":5,"backtrace":0,"custom":{"track":1}}}"#
        } else {
            r#"{"complete":{"jid":"LFluKy1Baak83p54","queue":"default","jobtype":"thumbnail_info","args":[],"created_at":"2023-12-22T07:00:52.546258624Z","reserve_for":600,"retry":25,"priority":5,"backtrace":0}}"#
        };
        assert_eq!(got, expected);

        // with description and both callbacks:
        let got = serde_json::to_string(
            &BatchBuilder::new()
                .description("Image processing workload")
                .with_callbacks(
                    prepare_test_job("thumbnail_clean_up".into()),
                    prepare_test_job("thumbnail_info".into()),
                ),
        )
        .unwrap();
        let expected = if cfg!(feature = "ent") {
            r#"{"description":"Image processing workload","success":{"jid":"LFluKy1Baak83p54","queue":"default","jobtype":"thumbnail_clean_up","args":[],"created_at":"2023-12-22T07:00:52.546258624Z","reserve_for":600,"retry":25,"priority":5,"backtrace":0,"custom":{"track":1}},"complete":{"jid":"LFluKy1Baak83p54","queue":"default","jobtype":"thumbnail_info","args":[],"created_at":"2023-12-22T07:00:52.546258624Z","reserve_for":600,"retry":25,"priority":5,"backtrace":0,"custom":{"track":1}}}"#
        } else {
            r#"{"description":"Image processing workload","success":{"jid":"LFluKy1Baak83p54","queue":"default","jobtype":"thumbnail_clean_up","args":[],"created_at":"2023-12-22T07:00:52.546258624Z","reserve_for":600,"retry":25,"priority":5,"backtrace":0},"complete":{"jid":"LFluKy1Baak83p54","queue":"default","jobtype":"thumbnail_info","args":[],"created_at":"2023-12-22T07:00:52.546258624Z","reserve_for":600,"retry":25,"priority":5,"backtrace":0}}"#
        };
        assert_eq!(got, expected);
    }
}
