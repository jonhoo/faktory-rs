/// The set of observable application-level errors when interacting with a Faktory server.
#[derive(Debug, Fail)]
pub enum FaktoryError {
    /// The server reports that an issued request was malformed.
    #[fail(display = "request was malformed: {}", desc)]
    Malformed {
        /// Error reported by server
        desc: String,
    },

    /// The server responded with an error.
    #[fail(display = "an internal server error occurred: {}", msg)]
    Internal {
        /// The error message given by the server.
        msg: String,
    },

    /// The server sent a response that did not match what was expected.
    #[fail(display = "expected {}, got unexpected response: {}", expected, received)]
    BadType {
        /// The expected response type.
        expected: &'static str,

        /// The received response.
        received: String,
    },

    /// The server sent a malformed response.
    #[fail(display = "server sent malformed {} response: {} in {:?}", typed_as, error, bytes)]
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
    #[fail(display = "unreachable")]
    #[doc(hidden)]
    __Nonexhaustive,
}

impl FaktoryError {
    pub(crate) fn new(line: String) -> Self {
        let mut parts = line.splitn(2, ' ');
        let code = parts.next();
        let error = parts.next();
        if error.is_none() {
            return FaktoryError::Internal {
                msg: code.unwrap().to_string(),
            };
        }
        let error = error.unwrap().to_string();

        match code {
            Some("ERR") => FaktoryError::Internal { msg: error },
            Some("MALFORMED") => FaktoryError::Malformed { desc: error },
            Some(c) => FaktoryError::Internal {
                msg: format!("{} {}", c, error),
            },
            None => FaktoryError::Internal {
                msg: "empty error response".to_string(),
            },
        }
    }
}
