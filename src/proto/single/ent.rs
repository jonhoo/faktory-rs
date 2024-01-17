use chrono::{DateTime, Utc};
use derive_builder::Builder;
use serde::{
    de::{Deserializer, IntoDeserializer},
    Deserialize,
};

use crate::{Error, JobBuilder};

// Used to parse responses from Faktory that look like this:
// '{"jid":"f7APFzrS2RZi9eaA","state":"unknown","updated_at":""}'
fn parse_datetime<'de, D>(value: D) -> Result<Option<DateTime<Utc>>, D::Error>
where
    D: Deserializer<'de>,
{
    match Option::<String>::deserialize(value)?.as_deref() {
        Some("") | None => Ok(None),
        Some(non_empty) => DateTime::deserialize(non_empty.into_deserializer()).map(Some),
    }
}

impl JobBuilder {
    /// When Faktory should expire this job.
    ///
    /// Faktory Enterprise allows for expiring jobs. This is setter for `expires_at`
    /// field in the job's custom data.
    /// ```
    /// # use faktory::JobBuilder;
    /// # use chrono::{Duration, Utc};
    /// let _job = JobBuilder::new("order")
    ///     .args(vec!["ISBN-13:9781718501850"])
    ///     .expires_at(Utc::now() + Duration::hours(1))
    ///     .build();
    /// ```
    pub fn expires_at(&mut self, dt: DateTime<Utc>) -> &mut Self {
        self.add_to_custom_data(
            "expires_at",
            dt.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
        )
    }

    /// In what period of time from now (UTC) the Faktory should expire this job.
    ///
    /// Under the hood, the method will call `Utc::now` and add the provided `ttl` duration.
    /// You can use this setter when you have a duration rather than some exact date and time,
    /// expected by [`expires_at`](struct.JobBuilder.html#method.expires_at) setter.
    /// Example usage:
    /// ```
    /// # use faktory::JobBuilder;
    /// # use chrono::Duration;
    /// let _job = JobBuilder::new("order")
    ///     .args(vec!["ISBN-13:9781718501850"])
    ///     .expires_in(Duration::weeks(1))
    ///     .build();
    /// ```
    pub fn expires_in(&mut self, ttl: chrono::Duration) -> &mut Self {
        self.expires_at(Utc::now() + ttl)
    }

    /// How long the Faktory will not accept duplicates of this job.
    ///
    /// The job will be considered unique for the kind-args-queue combination. The uniqueness is best-effort,
    /// rather than a guarantee. Check out the Enterprise Faktory [docs](https://github.com/contribsys/faktory/wiki/Ent-Unique-Jobs)
    /// for details on how scheduling, retries, and other features live together with `unique_for`.
    ///
    /// If you've already created and pushed a unique job (job "A") to the Faktory server and now have got another one
    /// of same kind, with the same args and destined for the same queue (job "B") and you would like - for some reason - to
    /// bypass the unique constraint, simply leave `unique_for` field on the job's custom hash empty, i.e. do not use this setter.
    /// In this case, the Faktory server will accept job "B", though technically this job "B" is a duplicate.
    pub fn unique_for(&mut self, secs: usize) -> &mut Self {
        self.add_to_custom_data("unique_for", secs)
    }

    /// Remove unique lock for this job right before the job starts executing.
    ///
    /// Another job with the same kind-args-queue combination will be accepted by the Faktory server
    /// after the period specified in [`unique_for`](struct.JobBuilder.html#method.unique_for) has finished
    /// _or_ after this job has been been consumed (i.e. its execution has ***started***).
    pub fn unique_until_start(&mut self) -> &mut Self {
        self.add_to_custom_data("unique_until", "start")
    }

    /// Do not remove unique lock for this job until it successfully finishes.
    ///
    /// Sets `unique_until` on the Job's custom hash to `success`, which is Faktory's default.
    /// Another job with the same kind-args-queue combination will be accepted by the Faktory server
    /// after the period specified in [`unique_for`](struct.JobBuilder.html#method.unique_for) has finished
    /// _or_ after this job has been been ***successfully*** processed.
    pub fn unique_until_success(&mut self) -> &mut Self {
        self.add_to_custom_data("unique_until", "success")
    }
}

/// Info on job execution progress (retrieved).
///
/// The tracker is guaranteed to get the following details: the job's id (though
/// they should know it beforehand in order to be ably to track the job), its last
/// know state (e.g."enqueued", "working", "success", "unknown") and the date and time
/// the job was last updated. Additionally, information on what's going on with the job
/// ([desc](struct.ProgressUpdate.html#structfield.desc)) and completion percentage
/// ([percent](struct.ProgressUpdate.html#structfield.percent)) may be available,
/// if the worker provided those details.
#[derive(Debug, Clone, Deserialize)]
pub struct Progress {
    /// Id of the tracked job.
    pub jid: String,

    /// Job's state.
    pub state: String,

    /// When this job was last updated.
    #[serde(deserialize_with = "parse_datetime")]
    pub updated_at: Option<DateTime<Utc>>,

    /// Percentage of the job's completion.
    pub percent: Option<u8>,

    /// Arbitrary description that may be useful to whoever is tracking the job's progress.
    pub desc: Option<String>,
}

/// Info on job execution progress (sent).
///
/// In Enterprise Faktory, a client executing a job can report on the execution
/// progress, provided the job is trackable. A trackable job is the one with "track":1
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
    pub jid: String,

    /// Percentage of the job's completion.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default = "None")]
    pub percent: Option<u8>,

    /// Arbitrary description that may be useful to whoever is tracking the job's progress.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default = "None")]
    pub desc: Option<String>,

    /// Allows to extend the job's reservation, if more time needed to execute it.
    ///
    /// Note that you cannot decrease the initial [reservation](struct.Job.html#structfield.reserve_for).
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default = "None")]
    pub reserve_until: Option<DateTime<Utc>>,
}

impl ProgressUpdate {
    /// Create a new instance of `ProgressUpdateBuilder` with job ID already set.
    ///
    /// Equivalent to creating a [new](struct.ProgressUpdateBuilder.html#method.new)
    /// `ProgressUpdateBuilder`.
    pub fn builder(jid: impl Into<String>) -> ProgressUpdateBuilder {
        ProgressUpdateBuilder {
            jid: Some(jid.into()),
            ..ProgressUpdateBuilder::create_empty()
        }
    }

    /// Create a new instance of `ProgressUpdate`.
    ///
    /// While job ID is specified at `ProgressUpdate`'s creation time,
    /// the rest of the [fields](struct.ProgressUpdate.html) are defaulted to _None_.
    pub fn new(jid: impl Into<String>) -> ProgressUpdate {
        ProgressUpdateBuilder::new(jid).build()
    }
}

impl ProgressUpdateBuilder {
    /// Builds an instance of ProgressUpdate.
    pub fn build(&self) -> ProgressUpdate {
        self.try_build()
            .expect("All required fields have been set.")
    }

    /// Create a new instance of 'JobBuilder'
    pub fn new(jid: impl Into<String>) -> ProgressUpdateBuilder {
        ProgressUpdateBuilder {
            jid: Some(jid.into()),
            ..ProgressUpdateBuilder::create_empty()
        }
    }
}

// ----------------------------------------------

use super::FaktoryCommand;
use std::{fmt::Debug, io::Write};

#[derive(Debug, Clone)]
pub enum Track {
    Set(ProgressUpdate),
    Get(String),
}

impl FaktoryCommand for Track {
    fn issue<W: Write>(&self, w: &mut W) -> Result<(), Error> {
        match self {
            Self::Set(upd) => {
                w.write_all(b"TRACK SET ")?;
                serde_json::to_writer(&mut *w, upd).map_err(Error::Serialization)?;
                Ok(w.write_all(b"\r\n")?)
            }
            Self::Get(jid) => {
                w.write_all(b"TRACK GET ")?;
                w.write_all(jid.as_bytes())?;
                Ok(w.write_all(b"\r\n")?)
            }
        }
    }
}

// ----------------------------------------------

#[cfg(test)]
mod test {
    use chrono::{DateTime, Utc};

    use crate::JobBuilder;

    fn half_stuff() -> JobBuilder {
        let mut job = JobBuilder::new("order");
        job.args(vec!["ISBN-13:9781718501850"]);
        job
    }

    // Returns date and time string in the format expected by Faktory.
    // Serializes date and time into a string as per RFC 3338 and ISO 8601
    // with nanoseconds precision and 'Z' literal for the timzone column.
    fn to_iso_string(dt: DateTime<Utc>) -> String {
        dt.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)
    }

    #[test]
    fn test_expiration_feature_for_enterprise_faktory() {
        let five_min = chrono::Duration::seconds(300);
        let exp_at = Utc::now() + five_min;
        let job1 = half_stuff().expires_at(exp_at).build();
        let stored = job1.custom.get("expires_at").unwrap();
        assert_eq!(stored, &serde_json::Value::from(to_iso_string(exp_at)));

        let job2 = half_stuff().expires_in(five_min).build();
        assert!(job2.custom.get("expires_at").is_some());
    }

    #[test]
    fn test_uniqueness_faeture_for_enterprise_faktory() {
        let job = half_stuff().unique_for(60).unique_until_start().build();
        let stored_unique_for = job.custom.get("unique_for").unwrap();
        let stored_unique_until = job.custom.get("unique_until").unwrap();
        assert_eq!(stored_unique_for, &serde_json::Value::from(60));
        assert_eq!(stored_unique_until, &serde_json::Value::from("start"));

        let job = half_stuff().unique_for(60).unique_until_success().build();

        let stored_unique_until = job.custom.get("unique_until").unwrap();
        assert_eq!(stored_unique_until, &serde_json::Value::from("success"));
    }

    #[test]
    fn test_same_purpose_setters_applied_simultaneously() {
        let expires_at1 = Utc::now() + chrono::Duration::seconds(300);
        let expires_at2 = Utc::now() + chrono::Duration::seconds(300);
        let job = half_stuff()
            .unique_for(60)
            .add_to_custom_data("unique_for", 600)
            .unique_for(40)
            .add_to_custom_data("expires_at", to_iso_string(expires_at1))
            .expires_at(expires_at2)
            .build();
        let stored_unique_for = job.custom.get("unique_for").unwrap();
        assert_eq!(stored_unique_for, &serde_json::Value::from(40));
        let stored_expires_at = job.custom.get("expires_at").unwrap();
        assert_eq!(
            stored_expires_at,
            &serde_json::Value::from(to_iso_string(expires_at2))
        )
    }
}
