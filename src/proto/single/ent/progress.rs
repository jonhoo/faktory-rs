use super::utils;
use crate::proto::single::JobId;
use chrono::{DateTime, Utc};
use derive_builder::Builder;

/// Info on job execution progress (sent).
///
/// In Enterprise Faktory, a client executing a job can report on the execution
/// progress, provided the job is trackable. A trackable job is one with "track":0
/// specified in the custom data hash.
#[derive(Debug, Clone, Serialize, Builder)]
#[builder(
    custom_constructor,
    setter(into),
    build_fn(name = "try_build", private)
)]
pub struct ProgressUpdate {
    /// Id of the tracked job.
    #[builder(setter(custom))]
    pub jid: JobId,

    /// Percentage of the job's completion.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default = "None")]
    pub percent: Option<u8>,

    /// Arbitrary description that may be useful to whoever is tracking the job's progress.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default = "None")]
    pub desc: Option<String>,

    /// Allows to extend the job's reservation, if more time is needed to execute it.
    ///
    /// Note that you cannot shorten the initial [reservation](struct.Job.html#structfield.reserve_for) via
    /// specifying an instant that is sooner than the job's initial reservation deadline.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default = "None")]
    pub reserve_until: Option<DateTime<Utc>>,
}

impl AsRef<Self> for ProgressUpdate {
    fn as_ref(&self) -> &Self {
        self
    }
}

impl ProgressUpdate {
    /// Create an instance of `ProgressUpdate` for the job with this ID specifying its completion percentage.
    pub fn set(jid: JobId, percent: u8) -> ProgressUpdate {
        ProgressUpdate::builder(jid).percent(percent).build()
    }

    /// Create a new instance of `ProgressUpdateBuilder` with job ID already set.
    ///
    /// Equivalent to creating a [new](struct.ProgressUpdateBuilder.html#method.new)
    /// `ProgressUpdateBuilder`.
    pub fn builder(jid: JobId) -> ProgressUpdateBuilder {
        ProgressUpdateBuilder::new(jid)
    }
}

impl ProgressUpdateBuilder {
    /// Builds an instance of ProgressUpdate.
    pub fn build(&self) -> ProgressUpdate {
        self.try_build()
            .expect("Only jid is required, and it is set by all constructors.")
    }

    /// Create a new instance of `JobBuilder`
    pub fn new(jid: JobId) -> ProgressUpdateBuilder {
        ProgressUpdateBuilder {
            jid: Some(jid),
            ..ProgressUpdateBuilder::create_empty()
        }
    }
}

// Ref: https://github.com/contribsys/faktory/wiki/Ent-Tracking#notes
/// Job's state as last known by the Faktory server.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum JobState {
    /// The server can't tell a job's state.
    ///
    /// This happens when a job with the specified ID has never been enqueued, or the job has
    /// not been marked as trackable via "track":0 in its custom hash, or the tracking info on this
    /// job has simply expired on the server (normally, after 29 min).
    Unknown,

    /// The job has been enqueued.
    Enqueued,

    /// The job has been consumed by a worker and is now being executed.
    Working,

    /// The job has been executed with success.
    Success,

    /// The job has finished with an error.
    Failed,

    /// The jobs has been consumed but its status has never been updated.
    Dead,
}

impl std::fmt::Display for JobState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use JobState::*;
        let s = match self {
            Unknown => "unknown",
            Enqueued => "enqueued",
            Working => "working",
            Success => "success",
            Failed => "failed",
            Dead => "dead",
        };
        write!(f, "{}", s)
    }
}

/// Info on job execution progress (retrieved).
///
/// The tracker is guaranteed to get the following details: the job's id (though they should
/// know it beforehand in order to be ably to track the job), its last known [`state`](JobState), and
/// the date and time the job was last updated. Additionally, arbitrary information on what's going
/// on with the job ([`desc`](ProgressUpdate::desc)) and the job's completion percentage
/// ([`percent`](ProgressUpdate::percent)) may be available, if the worker has provided those details.
#[derive(Debug, Clone, Deserialize)]
pub struct Progress {
    /// Id of the tracked job.
    pub jid: JobId,

    /// Job's state.
    pub state: JobState,

    /// When this job was last updated.
    #[serde(deserialize_with = "utils::parse_datetime")]
    pub updated_at: Option<DateTime<Utc>>,

    /// Percentage of the job's completion.
    pub percent: Option<u8>,

    /// Arbitrary description that may be useful to whoever is tracking the job's progress.
    pub desc: Option<String>,
}

impl Progress {
    /// Create an instance of `ProgressUpdate` for the job updating its completion percentage.
    ///
    /// This will copy the [`desc`](Progress::desc) from the `Progress` (retrieved) over to `ProgressUpdate` (to be sent).
    pub fn update_percent(&self, percent: u8) -> ProgressUpdate {
        ProgressUpdate::builder(self.jid.clone())
            .desc(self.desc.clone())
            .percent(percent)
            .build()
    }

    /// Create an instance of `ProgressUpdateBuilder` for the job.
    pub fn update_builder(&self) -> ProgressUpdateBuilder {
        ProgressUpdateBuilder::new(self.jid.clone())
    }
}
