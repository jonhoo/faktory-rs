use derive_builder::Builder;

use crate::JobId;

/// TODO
///
/// Use a [`filter`](crate::MutationFilter) to narrow down the subset of jobs your would
/// like to requeue.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum MutationTarget {
    /// TODO
    #[default]
    Scheduled,

    /// TODO
    Retries,

    /// TODO
    Dead,
}

/// TODO
#[derive(Builder, Clone, Debug, Default, PartialEq, Eq, Serialize)]
#[builder(default, setter(into), build_fn(name = "try_build", private))]
#[non_exhaustive]
pub struct MutationFilter<'a> {
    /// TODO
    ///
    /// TODO
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(custom))]
    pub jids: Option<&'a [&'a JobId]>,

    /// TODO
    ///
    /// TODO
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "regexp")]
    pub pattern: Option<&'a str>,

    /// TODO
    ///
    /// TODO
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "jobtype")]
    pub kind: Option<&'a str>,
}

impl MutationFilter<'_> {
    pub(crate) fn is_empty(&self) -> bool {
        self.jids.is_none() && self.kind.is_none() && self.pattern.is_none()
    }
}

impl<'a> MutationFilter<'_> {
    /// TODO
    pub fn builder() -> MutationFilterBuilder<'a> {
        MutationFilterBuilder::default()
    }
}

impl<'a> MutationFilterBuilder<'a> {
    /// TODO
    pub fn jids(mut self, value: &'a [&JobId]) -> Self {
        self.jids = Some(value).into();
        self
    }
}

impl<'a> MutationFilterBuilder<'a> {
    /// TODO
    pub fn build(self) -> MutationFilter<'a> {
        self.try_build().expect("infallible")
    }
}
