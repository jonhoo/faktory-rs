use chrono::{DateTime, Utc};

use crate::JobBuilder;

impl JobBuilder {
    /// When Faktory should expire this job.
    ///
    /// Faktory Enterprise allows for expiring jobs. This is setter for `expires_at`
    /// field in the job's custom data.
    /// ```
    /// use faktory::JobBuilder;
    /// use chrono::{Duration, Utc};
    ///
    /// let _job = JobBuilder::new("order")
    ///     .args(vec!["ISBN-13:9781718501850"])
    ///     .expires_at(Utc::now() + Duration::hours(1))
    ///     .build();
    /// ```
    pub fn expires_at(&mut self, dt: DateTime<Utc>) -> &mut Self {
        self.add_to_custom_data(
            "expires_at".into(),
            dt.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
        )
    }

    /// In what period of time from now (UTC) the Faktory should expire this job.
    ///
    /// Use this setter when you are unwilling to populate the `expires_at` field in custom
    /// options with some exact date and time, e.g.:
    /// ```
    /// use faktory::JobBuilder;
    /// use chrono::Duration;
    ///
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
    /// The job will be considered unique for kind-args-queue combination.
    /// The uniqueness is best-effort, rather than a guarantee. Check out
    /// the Enterprise Faktory [docs](https://github.com/contribsys/faktory/wiki/Ent-Unique-Jobs)
    /// for details on how scheduling, retries and other features live together with `unique_for`.
    pub fn unique_for(&mut self, secs: usize) -> &mut Self {
        self.add_to_custom_data("unique_for".into(), secs)
    }

    /// Remove unique lock for this job right before the job starts executing.
    pub fn unique_until_start(&mut self) -> &mut Self {
        self.add_to_custom_data("unique_until".into(), "start")
    }

    /// Do not remove unique lock for this job until it successfully finishes.
    ///
    /// Sets `unique_until` on the Job's custom hash to `success`, which is Faktory's default.
    pub fn unique_until_success(&mut self) -> &mut Self {
        self.add_to_custom_data("unique_until".into(), "success")
    }
}

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
            .add_to_custom_data("unique_for".into(), 600)
            .unique_for(40)
            .add_to_custom_data("expires_at".into(), to_iso_string(expires_at1))
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
