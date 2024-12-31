use crate::JobId;
use derive_builder::Builder;

#[cfg(doc)]
use crate::{Client, Job};

/// Mutation target set.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum MutationTarget {
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
/// As of Faktory version 1.9.2, if [`MutationFilter::pattern`] and/or [`MutationFilter::kind`]
/// is specified, the values in [`MutationFilter::jids`] will not be taken into account by the
/// server. If you want to filter by `jids`, make sure to leave other fields of the filter empty
/// or use dedicated methods like [`Client::requeue_by_ids`].
///
/// Example usage:
/// ```no_run
/// # tokio_test::block_on(async {
/// # use faktory::{Client, MutationTarget, MutationFilter};
/// # let mut client = Client::connect().await.unwrap();
/// let filter = MutationFilter::builder()
///     .kind("jobtype_here")
///     .pattern(r#"*\"args\":\[\"fizz\"\]*"#)
///     .build();
/// client.requeue(MutationTarget::Retries, &filter).await.unwrap();
/// # })
/// ```
#[derive(Builder, Clone, Debug, PartialEq, Eq, Serialize)]
#[builder(setter(into), build_fn(name = "try_build", private), pattern = "owned")]
#[non_exhaustive]
pub struct MutationFilter<'a> {
    /// A job's [`kind`](crate::Job::kind).
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "jobtype")]
    #[builder(default)]
    pub kind: Option<&'a str>,

    /// [`JobId`]s to target.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(custom))]
    #[builder(default)]
    pub jids: Option<&'a [&'a JobId]>,

    /// Match pattern to use for filtering.
    ///
    /// Faktory will pass this directly to Redis's `SCAN` command,
    /// so please see the [`SCAN` documentation](https://redis.io/docs/latest/commands/scan/)
    /// for further details.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "regexp")]
    #[builder(default)]
    pub pattern: Option<&'a str>,
}

impl MutationFilter<'_> {
    pub(crate) fn is_empty(&self) -> bool {
        self.jids.is_none() && self.kind.is_none() && self.pattern.is_none()
    }
}

impl<'a> MutationFilter<'_> {
    /// Creates an empty filter.
    ///
    /// Sending a mutation command (e.g. [`Client::discard`]) with an empty
    /// filter effectively means performing no filtering at all.
    pub fn empty() -> Self {
        Self {
            kind: None,
            jids: None,
            pattern: None,
        }
    }

    /// Creates a new builder for a [`MutationFilter`].
    pub fn builder() -> MutationFilterBuilder<'a> {
        MutationFilterBuilder::default()
    }
}

impl<'a> MutationFilterBuilder<'a> {
    /// Ids of jobs to target.
    pub fn jids(mut self, value: &'a [&JobId]) -> Self {
        self.jids = Some(value).into();
        self
    }
}

impl<'a> MutationFilterBuilder<'a> {
    /// Builds a new [`MutationFilter`] from the parameters of this builder.
    pub fn build(self) -> MutationFilter<'a> {
        self.try_build().expect("infallible")
    }
}
