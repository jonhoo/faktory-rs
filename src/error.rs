//! Enumerates all errors that this crate may return.
//!
//! [`Error`] is the top level error enum.
//! Most consumers should only need to interact with this type.
//! This is also where more generic errors such as I/O errors are placed,
//! whereas the more specific errors ([`Connection`] and [`Protocol`]) are
//! related to logic.
//!
//! [`Connect`] describes errors specific to the connection logic, for example
//! version mismatches or an invalid URL.
//!
//! [`Protocol`] describes lower-level errors relating to communication
//! with the faktory server. Typically, [`Protocol`] errors are the result
//! of the server sending a response this client did not expect.

use thiserror::Error;

/// The set of observable errors when interacting with a Faktory server.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// The connection to the server, or one of its prerequisites, failed.
    #[error("connection")]
    Connect(#[from] Connect),

    /// Underlying I/O layer errors.
    ///
    /// These are overwhelmingly network communication errors on the socket connection to the server.
    #[error("underlying I/O")]
    IO(#[from] std::io::Error),

    /// Application-level errors.
    ///
    /// These generally indicate a mismatch between what the client expects and what the server expects.
    #[error("protocol")]
    Protocol(#[from] Protocol),

    /// Faktory payloads are JSON encoded.
    ///
    /// This error is one that was encountered when attempting to serialize or deserialize communication with the server.
    /// These generally indicate a mismatch between what the client expects and what the server provided.
    #[error("serialization")]
    Serialization(#[source] serde_json::Error),

    /// Indicates an error in the underlying TLS stream.
    #[cfg(feature = "tls")]
    #[error("underlying tls stream")]
    TlsStream(#[source] native_tls::Error),
}

/// Errors specific to connection logic.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Connect {
    /// The scheme portion of the connection address provided is invalid.
    #[error("unknown scheme: {scheme}")]
    BadScheme {
        /// The scheme that was provided in the connection address.
        scheme: String,
    },

    /// The provided connection address does not contain a hostname.
    #[error("no hostname given")]
    MissingHostname,

    /// The server requires authentication, but none was provided.
    #[error("server requires authentication")]
    AuthenticationNeeded,

    /// The server expects a different protocol version than this library supports.
    #[error("server version mismatch (theirs: {theirs}, ours: {ours})")]
    VersionMismatch {
        /// The protocol version this library supports.
        ours: usize,

        /// The protocol version the server expects.
        theirs: usize,
    },

    /// The connection address provided was not able to be parsed.
    #[error("parse URL")]
    ParseUrl(#[source] url::ParseError),
}

/// The set of observable application-level errors when interacting with a Faktory server.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Protocol {
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
}

impl Protocol {
    pub(crate) fn new(line: String) -> Self {
        let mut parts = line.splitn(2, ' ');
        let code = parts.next();
        let error = parts.next();
        if error.is_none() {
            return Protocol::Internal {
                msg: code.unwrap().to_string(),
            };
        }
        let error = error.unwrap().to_string();

        match code {
            Some("ERR") => Protocol::Internal { msg: error },
            Some("MALFORMED") => Protocol::Malformed { desc: error },
            Some(c) => Protocol::Internal {
                msg: format!("{} {}", c, error),
            },
            None => Protocol::Internal {
                msg: "empty error response".to_string(),
            },
        }
    }
}
