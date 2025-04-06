use crate::JobId;

#[cfg(doc)]
use crate::{Client, Job};

use super::utils::Empty;

/// Mutation target set.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum JobSet {
    /// A set of currently enqueued jobs.
    #[default]
    Scheduled,

    /// A set of jobs that should be retried.
    Retries,

    /// A set of failed jobs that will not be retried.
    Dead,
}

// As of Faktory v1.9.2, not all the fields on the filter
// will be taken into account, rather EITHER `jids` OR optional `kind`
// plus optional `pattern`.
// See: https://github.com/contribsys/faktory/issues/489
//
/// A filter to help narrow down the mutation target.
///
/// Example usage:
/// ```no_run
/// # tokio_test::block_on(async {
/// # use faktory::Client;
/// # use faktory::mutate::{JobSet, Filter};
/// # let mut client = Client::connect().await.unwrap();
/// let filter = Filter::from_kind_and_pattern("jobtype_here", r#"*\"args\":\[\"fizz\"\]*"#);
/// client.requeue(JobSet::Retries, &filter).await.unwrap();
/// # })
/// ```
#[derive(Clone, Default, Debug, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct Filter<'a> {
    /// A job's [`kind`](crate::Job::kind).
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "jobtype")]
    pub(crate) kind: Option<&'a str>,

    /// [`JobId`]s to target.
    #[serde(skip_serializing_if = "Empty::is_empty")]
    pub(crate) jids: Option<&'a [&'a JobId]>,

    /// Match pattern to use for filtering.
    ///
    /// Faktory will pass this directly to Redis's `SCAN` command,
    /// so please see the [`SCAN` documentation](https://redis.io/docs/latest/commands/scan/)
    /// for further details.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "regexp")]
    pub(crate) pattern: Option<&'a str>,
}

impl Empty for &Filter<'_> {
    fn is_empty(&self) -> bool {
        self.jids.is_empty() && self.kind.is_none() && self.pattern.is_none()
    }
}

impl<'a> Filter<'a> {
    /// Creates an empty filter.
    ///
    /// Sending a mutation command (e.g. [`Client::discard`]) with an empty
    /// filter effectively means using a wildcard.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Creates a filter from the provided [`JobId`]s.
    pub fn from_ids(ids: &'a [&JobId]) -> Self {
        Self {
            jids: Some(ids),
            ..Default::default()
        }
    }

    /// Creates a filter with the provided job's [`kind`](crate::Job::kind).
    pub fn from_kind(job_kind: &'a str) -> Self {
        Self {
            kind: Some(job_kind),
            ..Default::default()
        }
    }

    /// Creates a filter with the provided search pattern.
    ///
    /// Faktory will pass this directly to Redis's `SCAN` command,
    /// so please see the [`SCAN` documentation](https://redis.io/docs/latest/commands/scan/)
    /// for further details.
    pub fn from_pattern(pattern: &'a str) -> Self {
        Self {
            pattern: Some(pattern),
            ..Default::default()
        }
    }
    /// Creates a filter with the provided job's [`kind`](crate::Job::kind) and search pattern.
    ///
    /// This is essentially [`Filter::from_kind`] and [`Filter::from_pattern`] merged together.
    pub fn from_kind_and_pattern(job_kind: &'a str, pattern: &'a str) -> Self {
        Self {
            kind: Some(job_kind),
            pattern: Some(pattern),
            jids: None,
        }
    }
}

#[cfg(test)]
mod test {
    use super::Filter;

    #[test]
    fn filter_is_serialized_correctly() {
        // every field None
        let filter = Filter::empty();
        let ser = serde_json::to_string(&filter).unwrap();
        assert_eq!(ser, "{}");

        // some but empty jids
        let filter = Filter::from_ids(&[]);
        let ser = serde_json::to_string(&filter).unwrap();
        assert_eq!(ser, "{}")
    }
}
