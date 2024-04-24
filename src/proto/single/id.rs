use super::utils;
use std::fmt::Display;
use std::ops::Deref;

macro_rules! string_wrapper_impls {
    ($new_type:ident) => {
        impl $new_type {
            /// Create a new entity identifier.
            pub fn new<S>(inner: S) -> Self
            where
                S: AsRef<str> + Clone + Display,
            {
                Self(inner.to_string())
            }
        }

        impl Deref for $new_type {
            type Target = String;
            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl AsRef<str> for $new_type {
            fn as_ref(&self) -> &str {
                self.deref().as_ref()
            }
        }

        impl AsRef<$new_type> for $new_type {
            fn as_ref(&self) -> &$new_type {
                &self
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

/// Batch identifier.
///
/// This is a wrapper over the string identifier issued by the Faktory server.
/// Only used for operations with [`Batch`](struct.Batch.html) in Enterprise Faktory.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchId(String);

string_wrapper_impls!(BatchId);

use serde_json::Value;
impl From<BatchId> for Value {
    fn from(value: BatchId) -> Self {
        value.0.into()
    }
}
