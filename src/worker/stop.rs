#[cfg(doc)]
use super::{Worker, WorkerBuilder};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// A reason why [`Worker::run`] has discontinued.
#[non_exhaustive]
pub enum StopReason {
    /// Graceful shutdown completed.
    ///
    /// A future provided via [`WorkerBuilder::with_graceful_shutdown`] has resolved
    /// signalling the worker to stop.
    GracefulShutdown,

    /// The Faktory server asked us to shut down.
    ///
    /// Under the hood, the worker is being in constant communication with the Faktory server,
    /// not only fetching jobs and reporting on processing results, but also listening for
    /// the server's instructions, one of which can be to disengage (e.g., to indicate that the
    /// server is shutting down.
    ServerInstruction,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Holds some details aroung a worker's run stoppage, such as the reason why this worker discontinued
/// and the number of workers that might still be processing jobs at that instant.
pub struct StopDetails {
    /// A [`reason`](StopReason) why the worker's run has discontinued.
    pub reason: StopReason,

    /// The number of workers that might still be processing jobs.
    pub nrunning: usize,
}

impl StopDetails {
    pub(crate) fn new(reason: StopReason, nrunning: usize) -> Self {
        StopDetails { reason, nrunning }
    }
}
