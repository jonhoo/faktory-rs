use derive_builder::Builder;

use crate::JobId;

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

/// Filter to help narrow down the mutation target.
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
#[derive(Builder, Clone, Debug, Default, PartialEq, Eq, Serialize)]
#[builder(
    default,
    setter(into),
    build_fn(name = "try_build", private),
    pattern = "owned"
)]
#[non_exhaustive]
pub struct MutationFilter<'a> {
    /// A job's [`kind`](crate::Job::kind).
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "jobtype")]
    pub kind: Option<&'a str>,

    /// Ids of jobs to target.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(custom))]
    pub jids: Option<&'a [&'a JobId]>,

    /// Match attern to use for filtering.
    ///
    /// Faktory will pass this directly to Redis's `SCAN` command,
    /// so please see the [`SCAN` documentation](https://redis.io/docs/latest/commands/scan/)
    /// for further details.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "regexp")]
    pub pattern: Option<&'a str>,
}

impl MutationFilter<'_> {
    pub(crate) fn is_empty(&self) -> bool {
        self.jids.is_none() && self.kind.is_none() && self.pattern.is_none()
    }
}

impl<'a> MutationFilter<'_> {
    /// Creates a new builder for a [`MutationFilter`]
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
