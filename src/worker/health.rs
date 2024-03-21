use super::{Worker, STATUS_QUIET, STATUS_RUNNING, STATUS_TERMINATING};
use crate::{proto::HeartbeatStatus, Error, Reconnect};
use std::{
    error::Error as StdError,
    sync::{atomic, Arc},
    time,
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tokio::time::sleep as tokio_sleep;

const CHECK_STATE_INTERVAL_MILLIS: u64 = 100;
const HEARTBEAT_INTERVAL_SECS: u64 = 5;

impl<
        S: AsyncBufReadExt + AsyncWriteExt + Reconnect + Send + Unpin + 'static,
        E: StdError + 'static + Send,
    > Worker<S, E>
{
    pub(crate) async fn listen_for_heartbeats(
        &mut self,
        statuses: &Vec<Arc<atomic::AtomicUsize>>,
    ) -> Result<bool, Error> {
        let mut target = STATUS_RUNNING;

        let mut last = time::Instant::now();

        loop {
            tokio_sleep(time::Duration::from_millis(CHECK_STATE_INTERVAL_MILLIS)).await;

            let worker_failure = target == STATUS_RUNNING
                && statuses
                    .iter()
                    .any(|s| s.load(atomic::Ordering::SeqCst) == STATUS_TERMINATING);
            if worker_failure {
                // tell all workers to exit
                // (though chances are they've all failed already)
                for s in statuses {
                    s.store(STATUS_TERMINATING, atomic::Ordering::SeqCst);
                }
                break Ok(false);
            }

            if last.elapsed().as_secs() < HEARTBEAT_INTERVAL_SECS {
                continue;
            }

            match self.c.heartbeat().await {
                Ok(hb) => {
                    match hb {
                        HeartbeatStatus::Ok => {
                            tracing::trace!("Faktory server HEARTBEAT status is OK.");
                        }
                        HeartbeatStatus::Quiet => {
                            tracing::trace!("Faktory server HEARTBEAT status is QUIET.");
                            // tell the workers to eventually terminate
                            for s in statuses {
                                s.store(STATUS_QUIET, atomic::Ordering::SeqCst);
                            }
                            target = STATUS_QUIET;
                        }
                        HeartbeatStatus::Terminate => {
                            tracing::trace!("Faktory server HEARTBEAT status is TERMINATE.");
                            // tell the workers to terminate
                            // *and* fail the current job and immediately return
                            for s in statuses {
                                s.store(STATUS_QUIET, atomic::Ordering::SeqCst);
                            }
                            break Ok(true);
                        }
                    }
                }
                Err(e) => {
                    // for this to fail, the workers have probably also failed
                    for s in statuses {
                        s.store(STATUS_TERMINATING, atomic::Ordering::SeqCst);
                    }
                    break Err(e);
                }
            }
            last = time::Instant::now();
        }
    }
}
