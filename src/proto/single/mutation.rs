use derive_builder::Builder;

use crate::JobId;

/// TODO
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
#[builder(setter(into), build_fn(name = "try_build", private))]
#[non_exhaustive]
pub struct MutationFilter<'a, J> {
    /// TODO
    ///
    /// TODO
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(setter(custom))]
    pub jids: Option<&'a [J]>,

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

impl<'a, J> MutationFilter<'_, J>
where
    J: Clone,
{
    /// TODO
    pub fn builder() -> MutationFilterBuilder<'a, J> {
        MutationFilterBuilder::default()
    }
}

impl<'a, J> MutationFilterBuilder<'a, J>
where
    J: Clone + AsRef<JobId>,
{
    /// TODO
    pub fn jids(mut self, value: &'a [J]) -> Self {
        self.jids = Some(value).into();
        self
    }
}

impl<'a, J> MutationFilterBuilder<'a, J>
where
    J: Clone,
{
    /// TODO
    pub fn build(self) -> MutationFilter<'a, J> {
        self.try_build().expect("infallible")
    }
}
