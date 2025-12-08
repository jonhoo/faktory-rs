use crate::proto::WorkerId;

#[derive(Clone, Debug)]
pub(crate) struct ClientOptions {
    /// Hostname to advertise to server.
    ///
    /// Defaults to machine hostname.
    pub(crate) hostname: Option<String>,

    /// PID to advertise to server.
    ///
    /// Defaults to process ID.
    pub(crate) pid: Option<usize>,

    /// Worker ID to advertise to server.
    ///
    /// Defaults to a GUID.
    pub(crate) wid: Option<WorkerId>,

    /// Labels to advertise to server.
    ///
    /// Defaults to ["rust"].
    pub(crate) labels: Vec<String>,

    /// Password to authenticate with.
    ///
    /// Defaults to None.
    pub(crate) password: Option<String>,

    /// Whether this client is instatianted for a worker (i.e. to consume jobs).
    pub(crate) is_worker: bool,

    #[cfg(feature = "sysinfo")]
    pub(crate) system: Option<std::sync::Arc<std::sync::Mutex<sysinfo::System>>>,
}

impl Default for ClientOptions {
    fn default() -> Self {
        ClientOptions {
            hostname: None,
            pid: None,
            wid: None,
            labels: vec!["rust".to_string()],
            password: None,
            is_worker: false,
            #[cfg(feature = "sysinfo")]
            system: None,
        }
    }
}
