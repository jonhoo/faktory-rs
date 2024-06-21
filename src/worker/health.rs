use super::{Worker, STATUS_QUIET, STATUS_RUNNING, STATUS_TERMINATING};
use crate::{proto::HeartbeatStatus, Error};
use std::{
    error::Error as StdError,
    sync::{atomic, Arc},
    time,
};
use tokio::time::sleep as tokio_sleep;

impl<E> Worker<E>
where
    E: StdError,
{
    /// Send beats to Fakotry and quiet/terminate workers if signalled so.
    ///
    /// Some core details:
    /// - beats should be sent to Faktory at least every 15 seconds;
    /// - a worker's lifecycle is "running -> quiet -> terminate";
    /// - STATUS_QUIET means the worker should not consume any new jobs,
    ///   but should _continue_ processing its current job (if any);
    ///
    /// See more details [here](https://github.com/contribsys/faktory/blob/b4a93227a3323ab4b1365b0c37c2fac4f9588cc8/server/workers.go#L13-L49).
    pub(crate) async fn listen_for_heartbeats(
        &mut self,
        statuses: &[Arc<atomic::AtomicUsize>],
    ) -> Result<bool, Error> {
        let mut target = STATUS_RUNNING;

        let mut last = time::Instant::now();

        loop {
            tokio_sleep(time::Duration::from_millis(100)).await;

            // has a worker failed?
            if target == STATUS_RUNNING
                && statuses
                    .iter()
                    .any(|s| s.load(atomic::Ordering::SeqCst) == STATUS_TERMINATING)
            {
                // tell all workers to exit
                // (though chances are they've all failed already)
                for s in statuses {
                    s.store(STATUS_TERMINATING, atomic::Ordering::SeqCst);
                }
                break Ok(false);
            }

            if last.elapsed().as_secs() < 5 {
                // don't sent a heartbeat yet
                continue;
            }

            match self.c.heartbeat().await {
                Ok(hb) => {
                    match hb {
                        HeartbeatStatus::Ok => {}
                        HeartbeatStatus::Quiet => {
                            // tell the workers to eventually terminate
                            for s in statuses {
                                s.store(STATUS_QUIET, atomic::Ordering::SeqCst);
                            }
                            target = STATUS_QUIET;
                        }
                        HeartbeatStatus::Terminate => {
                            // tell the workers to terminate
                            // *and* fail the current job and immediately return
                            for s in statuses {
                                s.store(STATUS_TERMINATING, atomic::Ordering::SeqCst);
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
