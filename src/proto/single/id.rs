use super::utils;
use std::ops::{Deref, DerefMut};

macro_rules! string_wrapper_impls {
    ($new_type:ident) => {
        impl<S> From<S> for $new_type
        where
            S: AsRef<str>,
        {
            fn from(value: S) -> Self {
                $new_type(value.as_ref().to_owned())
            }
        }

        impl Deref for $new_type {
            type Target = String;
            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl DerefMut for $new_type {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.0
            }
        }
    };
}

/// Job identifier.
///
/// The Faktory server expects a [`jid`](struct.Job.html#structfield.jid) of a reasonable length
/// (at least 8 chars), which you should take into account when creating a new instance of `JobId`.
///
/// If you do not have any domain, product or organisation specific requirements, you may prefer
/// to have a random job identifier generated for you with [`random`](JobId::random).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobId(String);

impl JobId {
    /// Internally, generates a 16-char long random ASCII string.
    pub fn random() -> Self {
        Self(utils::gen_random_jid())
    }
}

string_wrapper_impls!(JobId);

// -----------------------------------------------------

/// Worker identifier.
///
/// The Faktory server expects a non-empty string as a worker identifier,
/// see [`wid`](struct.WorkerBuilder.html#method.wid).
///
/// If you do not have any domain, product or organisation specific requirements, you may prefer
/// to have a random job identifier generated for you with [`random`](WorkerId::random).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerId(String);

impl WorkerId {
    /// Internally, generates a 32-char long random ASCII string.
    pub fn random() -> Self {
        Self(utils::gen_random_wid())
    }
}

string_wrapper_impls!(WorkerId);

// -----------------------------------------------------
