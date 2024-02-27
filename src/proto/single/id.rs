use super::utils;
use std::ops::{Deref, DerefMut};

/// Job identifier.
///
/// The Faktory server expects a [`jid`](struct.Job.html#structfield.jid) of a reasonable length
/// (at least 8 chars), which you should take into account when creating a new instance of `JobId`.
/// If you do not have any domain, product or organisation specific requirements, you may prefer
/// to have a random job identifier generated for you with [`random`](JobId::random).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobId(String);

impl JobId {
    /// Internally, generates a 16-char long random string.
    pub fn random() -> Self {
        Self(utils::gen_random_jid())
    }
}

impl<S> From<S> for JobId
where
    S: AsRef<str>,
{
    fn from(value: S) -> Self {
        JobId(value.as_ref().to_owned())
    }
}

impl Deref for JobId {
    type Target = String;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for JobId {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

// -----------------------------------------------------
