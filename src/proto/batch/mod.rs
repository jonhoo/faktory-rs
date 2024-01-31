use crate::{Error, Job, Producer};
use chrono::{DateTime, Utc};
use derive_builder::Builder;
use std::io::{Read, Write};

mod cmd;

pub use cmd::{CommitBatch, GetBatchStatus, OpenBatch};

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
/// # use faktory::Error;
/// use faktory::{Producer, Job, Batch};
///
/// let mut prod = Producer::connect(None)?;
/// let job1 = Job::builder("job_type").build();
/// let job2 = Job::builder("job_type").build();
/// let job_cb = Job::builder("callback_job_type").build();
///
/// let batch = Batch::builder()
///     .description("Batch description".to_string())
///     .with_complete_callback(job_cb);
///
/// let mut batch = prod.start_batch(batch)?;
/// batch.add(job1)?;
/// batch.add(job2)?;
/// batch.commit()?;
///
/// # Ok::<(), Error>(())
/// ```
///
/// Nested batches are also supported:
/// ```no_run
/// # use faktory::{Producer, Job, Batch, Error};
/// # let mut prod = Producer::connect(None)?;
/// let parent_job1 = Job::builder("job_type").build();
/// let parent_job2 = Job::builder("another_job_type").build();
/// let parent_cb = Job::builder("callback_job_type").build();
///
/// let child_job1 = Job::builder("job_type").build();
/// let child_job2 = Job::builder("yet_another_job_type").build();
/// let child_cb = Job::builder("callback_job_type").build();
///
/// let parent_batch = Batch::builder()
///     .description("Batch description".to_string())
///     .with_complete_callback(parent_cb);
/// let child_batch = Batch::builder()
///     .description("Child batch description".to_string())
///     .with_success_callback(child_cb);
///
/// let mut parent = prod.start_batch(parent_batch)?;
/// parent.add(parent_job1)?;
/// parent.add(parent_job2)?;
/// let mut child = parent.start_batch(child_batch)?;
/// child.add(child_job1)?;
/// child.add(child_job2)?;
///
/// child.commit()?;
/// parent.commit()?;
///
/// # Ok::<(), Error>(())
/// ```
///
/// In the example above, there is a single level nesting, but you can nest those batches as deep as you wish,
/// effectively building a pipeline this way, since the Faktory guarantees that callback jobs will not be queued unless
/// the batch gets committed.
///
/// You can retieve the batch status using a [`Tracker`](struct.Tracker.html):
/// ```no_run
/// # use faktory::Error;
/// # use faktory::{Producer, Job, Batch, Tracker};
/// let mut prod = Producer::connect(None)?;
/// let job = Job::builder("job_type").build();
/// let cb_job = Job::builder("callback_job_type").build();
/// let b = Batch::builder().description("Batch description".to_string()).with_complete_callback(cb_job);
///
/// let mut b = prod.start_batch(b)?;
/// let bid = b.id().to_string();
/// b.add(job)?;
/// b.commit()?;
///
/// let mut t = Tracker::connect(None)?;
/// let s = t.get_batch_status(bid)?.unwrap();
/// assert_eq!(s.total, 1);
/// assert_eq!(s.pending, 1);
/// assert_eq!(s.description, Some("Batch description".into()));
/// assert_eq!(s.complete_callback_state, ""); // has not been queued;
/// # Ok::<(), Error>(())
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
    parent_bid: Option<String>,

    /// Batch description for Faktory WEB UI.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// On success callback.
    ///
    /// This job will be queued by the Faktory server provided
    /// all the jobs belonging to this batch have been executed successfully.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(custom))]
    pub(crate) success: Option<Job>,

    /// On complete callback.
    ///
    /// This job will be queue by the Faktory server after all the jobs
    /// belonging to this batch have been executed, even if one/some/all
    /// of the workers have failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(custom))]
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

    fn success(&mut self, success_cb: impl Into<Option<Job>>) -> &mut Self {
        self.success = Some(success_cb.into());
        self
    }

    fn complete(&mut self, complete_cb: impl Into<Option<Job>>) -> &mut Self {
        self.complete = Some(complete_cb.into());
        self
    }

    /// Create a `Batch` with only `success` callback specified.
    pub fn with_success_callback(mut self, success_cb: Job) -> Batch {
        self.success(success_cb);
        self.complete(None);
        self.build()
    }

    /// Create a `Batch` with only `complete` callback specified.
    pub fn with_complete_callback(mut self, complete_cb: Job) -> Batch {
        self.complete(complete_cb);
        self.success(None);
        self.build()
    }

    /// Create a `Batch` with both `success` and `complete` callbacks specified.
    pub fn with_callbacks(mut self, success_cb: Job, complete_cb: Job) -> Batch {
        self.success(success_cb);
        self.complete(complete_cb);
        self.build()
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

pub struct BatchHandle<'a, S: Read + Write> {
    bid: String,
    prod: &'a mut Producer<S>,
}

impl<'a, S: Read + Write> BatchHandle<'a, S> {
    /// ID issued by the Faktory server to this batch.
    pub fn id(&self) -> &str {
        self.bid.as_ref()
    }

    pub(crate) fn new(bid: String, prod: &mut Producer<S>) -> BatchHandle<'_, S> {
        BatchHandle { bid, prod }
    }

    /// Add the given job to the batch.
    pub fn add(&mut self, mut job: Job) -> Result<(), Error> {
        job.custom.insert("bid".into(), self.bid.clone().into());
        self.prod.enqueue(job)
    }

    /// Initiate a child batch of jobs.
    pub fn start_batch(&mut self, mut batch: Batch) -> Result<BatchHandle<'_, S>, Error> {
        batch.parent_bid = Some(self.bid.clone());
        self.prod.start_batch(batch)
    }

    /// Commit this batch.
    ///
    /// The Faktory server will not queue any callbacks, unless the batch is committed.
    /// Committing an empty batch will make the server queue the callback(s) right away.
    /// Once committed, the batch can still be re-opened with [open_batch](struct.Producer.html#method.open_batch),
    /// and extra jobs can be added to it.
    pub fn commit(self) -> Result<(), Error> {
        self.prod.commit_batch(self.bid)
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
    /// See [complete](struct.Batch.html#structfield.complete).
    #[serde(rename = "complete_st")]
    pub complete_callback_state: String,

    /// State of the `success` callback.
    ///
    /// See [success](struct.Batch.html#structfield.success).
    #[serde(rename = "success_st")]
    pub success_callback_state: String,
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use chrono::{DateTime, Utc};

    use super::*;

    #[test]
    fn test_batch_creation() {
        let b = BatchBuilder::new()
            .description("Image processing batch".to_string())
            .with_success_callback(Job::builder("thumbnail").build());

        assert!(b.complete.is_none());
        assert!(b.parent_bid.is_none());
        assert!(b.success.is_some());
        assert_eq!(b.description, Some("Image processing batch".into()));

        let b = BatchBuilder::new()
            .description("Image processing batch".to_string())
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

        let b = BatchBuilder::new().description("Batch description".to_string());
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
                .description("Image processing workload".to_string())
                .with_success_callback(prepare_test_job("thumbnail_clean_up".into())),
        )
        .unwrap();
        let expected = r#"{"description":"Image processing workload","success":{"jid":"LFluKy1Baak83p54","queue":"default","jobtype":"thumbnail_clean_up","args":[],"created_at":"2023-12-22T07:00:52.546258624Z","reserve_for":600,"retry":25,"priority":5,"backtrace":0}}"#;
        assert_eq!(got, expected);

        // without description and with on complete callback:
        let got = serde_json::to_string(
            &BatchBuilder::new().with_complete_callback(prepare_test_job("thumbnail_info".into())),
        )
        .unwrap();
        let expected = r#"{"complete":{"jid":"LFluKy1Baak83p54","queue":"default","jobtype":"thumbnail_info","args":[],"created_at":"2023-12-22T07:00:52.546258624Z","reserve_for":600,"retry":25,"priority":5,"backtrace":0}}"#;
        assert_eq!(got, expected);

        // with description and both callbacks:
        let got = serde_json::to_string(
            &BatchBuilder::new()
                .description("Image processing workload".to_string())
                .with_callbacks(
                    prepare_test_job("thumbnail_clean_up".into()),
                    prepare_test_job("thumbnail_info".into()),
                ),
        )
        .unwrap();
        let expected = r#"{"description":"Image processing workload","success":{"jid":"LFluKy1Baak83p54","queue":"default","jobtype":"thumbnail_clean_up","args":[],"created_at":"2023-12-22T07:00:52.546258624Z","reserve_for":600,"retry":25,"priority":5,"backtrace":0},"complete":{"jid":"LFluKy1Baak83p54","queue":"default","jobtype":"thumbnail_info","args":[],"created_at":"2023-12-22T07:00:52.546258624Z","reserve_for":600,"retry":25,"priority":5,"backtrace":0}}"#;
        assert_eq!(got, expected);
    }
}
