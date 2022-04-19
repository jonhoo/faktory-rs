use thiserror::Error;

use crate::proto::ConnectError;

/// The set of observable errors when interacting with a Faktory server.
#[derive(Debug, Error)]
#[allow(clippy::manual_non_exhaustive)]
pub enum Error {
    /// The connection to the server, or one of its prerequisites, failed.
    #[error("connection error: {0}")]
    Connect(#[from] ConnectError),

    /// Underlying io layer errors.
    /// These are overwhelmingly network communication errors on the socket connection to the server.
    #[error("underlying i/o: {0}")]
    GenericIO(#[from] std::io::Error),

    /// Application-level errors.
    /// These generally indicate a mismatch between what the client expects and what the server expects.
    #[error("protocol: {0}")]
    Protocol(#[from] ProtocolError),

    /// Faktory payloads are JSON encoded.
    /// This error is one that was encountered when attempting to deserialize a response from the server.
    /// These generally indicate a mismatch between what the client expects and what the server provided.
    #[error("deserialize payload: {0}")]
    DeserializePayload(#[from] serde_json::Error),

    /// Indicates an error in the underlying TLS stream.
    #[cfg(feature = "tls")]
    #[error("underlying tls stream: {0}")]
    TlsStream(#[from] native_tls::Error),

    // We're going to add more error types in the future
    // https://github.com/rust-lang/rust/issues/44109
    //
    // This forces users to write pattern matches with a catch-all `_` arm.
    #[error("unreachable")]
    #[doc(hidden)]
    __Nonexhaustive,
}

/// The set of observable application-level errors when interacting with a Faktory server.
#[derive(Debug, Error)]
#[allow(clippy::manual_non_exhaustive)]
pub enum ProtocolError {
    /// The server reports that an issued request was malformed.
    #[error("request was malformed: {desc}")]
    Malformed {
        /// Error reported by server
        desc: String,
    },

    /// The server responded with an error.
    #[error("an internal server error occurred: {msg}")]
    Internal {
        /// The error message given by the server.
        msg: String,
    },

    /// The server sent a response that did not match what was expected.
    #[error("expected {expected}, got unexpected response: {received}")]
    BadType {
        /// The expected response type.
        expected: &'static str,

        /// The received response.
        received: String,
    },

    /// The server sent a malformed response.
    #[error("server sent malformed {typed_as} response: {error} in {bytes:?}")]
    BadResponse {
        /// The type of the server response.
        typed_as: &'static str,

        /// A description of what was wrong with the server response.
        error: &'static str,

        /// The relevant bytes sent by the server.
        bytes: Vec<u8>,
    },

    // We're going to add more error types in the future
    // https://github.com/rust-lang/rust/issues/44109
    //
    // This forces users to write pattern matches with a catch-all `_` arm.
    #[error("unreachable")]
    #[doc(hidden)]
    __Nonexhaustive,
}

impl ProtocolError {
    pub(crate) fn new(line: String) -> Self {
        let mut parts = line.splitn(2, ' ');
        let code = parts.next();
        let error = parts.next();
        if error.is_none() {
            return ProtocolError::Internal {
                msg: code.unwrap().to_string(),
            };
        }
        let error = error.unwrap().to_string();

        match code {
            Some("ERR") => ProtocolError::Internal { msg: error },
            Some("MALFORMED") => ProtocolError::Malformed { desc: error },
            Some(c) => ProtocolError::Internal {
                msg: format!("{} {}", c, error),
            },
            None => ProtocolError::Internal {
                msg: "empty error response".to_string(),
            },
        }
    }
}
