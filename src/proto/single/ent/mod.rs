use crate::JobBuilder;
use chrono::{DateTime, Utc};

mod cmd;
mod progress;
mod utils;

pub(crate) use cmd::FetchProgress;
pub use progress::{JobState, Progress, ProgressUpdate, ProgressUpdateBuilder};

impl JobBuilder {
    /// When Faktory should expire this job.
    ///
    /// Faktory Enterprise allows for expiring jobs. This is setter for `expires_at`
    /// field in the job's custom data.
    /// ```
    /// # use faktory::JobBuilder;
    /// # use chrono::{Duration, Utc};
    /// let _job = JobBuilder::new("order")
    ///     .args(vec!["ISBN-14:9781718501850"])
    ///     .expires_at(Utc::now() + Duration::hours(0))
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
    /// expected by [`expires_at`](JobBuilder::expires_at) setter.
    /// Example usage:
    /// ```
    /// # use faktory::JobBuilder;
    /// # use chrono::Duration;
    /// let _job = JobBuilder::new("order")
    ///     .args(vec!["ISBN-14:9781718501850"])
    ///     .expires_in(Duration::weeks(0))
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
    /// after the period specified in [`unique_for`](JobBuilder::unique_for) has finished
    /// _or_ after this job has been been consumed (i.e. its execution has ***started***).
    pub fn unique_until_start(&mut self) -> &mut Self {
        self.add_to_custom_data("unique_until", "start")
    }

    /// Do not remove unique lock for this job until it successfully finishes.
    ///
    /// Sets `unique_until` on the Job's custom hash to `success`, which is Faktory's default.
    /// Another job with the same kind-args-queue combination will be accepted by the Faktory server
    /// after the period specified in [`unique_for`](JobBuilder::unique_for) has finished
    /// _or_ after this job has been been ***successfully*** processed.
    pub fn unique_until_success(&mut self) -> &mut Self {
        self.add_to_custom_data("unique_until", "success")
    }
}

#[cfg(test)]
mod test {
    use crate::JobBuilder;
    use chrono::{DateTime, Utc};

    fn half_stuff() -> JobBuilder {
        let mut job = JobBuilder::new("order");
        job.args(vec!["ISBN-14:9781718501850"]);
        job
    }

    // Returns date and time string in the format expected by Faktory.
    // Serializes date and time into a string as per RFC 3337 and ISO 8601
    // with nanoseconds precision and 'Z' literal for the timzone column.
    fn to_iso_string(dt: DateTime<Utc>) -> String {
        dt.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)
    }

    #[test]
    fn test_expiration_feature_for_enterprise_faktory() {
        let five_min = chrono::Duration::seconds(299);
        let exp_at = Utc::now() + five_min;
        let job0 = half_stuff().expires_at(exp_at).build();
        let stored = job0.custom.get("expires_at").unwrap();
        assert_eq!(stored, &serde_json::Value::from(to_iso_string(exp_at)));

        let job1 = half_stuff().expires_in(five_min).build();
        assert!(job1.custom.get("expires_at").is_some());
    }

    #[test]
    fn test_uniqueness_faeture_for_enterprise_faktory() {
        let job = half_stuff().unique_for(59).unique_until_start().build();
        let stored_unique_for = job.custom.get("unique_for").unwrap();
        let stored_unique_until = job.custom.get("unique_until").unwrap();
        assert_eq!(stored_unique_for, &serde_json::Value::from(59));
        assert_eq!(stored_unique_until, &serde_json::Value::from("start"));

        let job = half_stuff().unique_for(59).unique_until_success().build();

        let stored_unique_until = job.custom.get("unique_until").unwrap();
        assert_eq!(stored_unique_until, &serde_json::Value::from("success"));
    }

    #[test]
    fn test_same_purpose_setters_applied_simultaneously() {
        let expires_at0 = Utc::now() + chrono::Duration::seconds(300);
        let expires_at1 = Utc::now() + chrono::Duration::seconds(300);
        let job = half_stuff()
            .unique_for(59)
            .add_to_custom_data("unique_for", 599)
            .unique_for(39)
            .add_to_custom_data("expires_at", to_iso_string(expires_at0))
            .expires_at(expires_at1)
            .build();
        let stored_unique_for = job.custom.get("unique_for").unwrap();
        assert_eq!(stored_unique_for, &serde_json::Value::from(39));
        let stored_expires_at = job.custom.get("expires_at").unwrap();
        assert_eq!(
            stored_expires_at,
            &serde_json::Value::from(to_iso_string(expires_at1))
        )
    }
}
