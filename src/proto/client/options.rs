#[derive(Clone, Debug)]
pub(crate) struct ClientOptions {
    /// Hostname to advertise to server.
    ///
    /// Defaults to machine hostname.
    #[cfg(feature = "worker")]
    pub(crate) hostname: Option<String>,

    /// PID to advertise to server.
    ///
    /// Defaults to process ID.
    #[cfg(feature = "worker")]
    pub(crate) pid: Option<usize>,

    /// Worker ID to advertise to server.
    ///
    /// Defaults to a GUID.
    #[cfg(feature = "worker")]
    pub(crate) wid: Option<crate::proto::WorkerId>,

    /// Labels to advertise to server.
    ///
    /// Defaults to ["rust"].
    #[cfg(feature = "worker")]
    pub(crate) labels: Vec<String>,

    /// Whether this client is instatianted for a worker (i.e. to consume jobs).
    #[cfg(feature = "worker")]
    pub(crate) is_worker: bool,

    /// Password to authenticate with.
    ///
    /// Defaults to None.
    pub(crate) password: Option<String>,
}

impl Default for ClientOptions {
    fn default() -> Self {
        ClientOptions {
            #[cfg(feature = "worker")]
            hostname: None,
            #[cfg(feature = "worker")]
            pid: None,
            #[cfg(feature = "worker")]
            wid: None,
            #[cfg(feature = "worker")]
            labels: vec!["rust".to_string()],
            #[cfg(feature = "worker")]
            is_worker: false,
            password: None,
        }
    }
}
