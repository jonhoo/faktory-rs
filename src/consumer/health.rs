use super::{Consumer, STATUS_QUIET, STATUS_RUNNING, STATUS_TERMINATING};
use crate::{proto::HeartbeatStatus, Error, Reconnect};
use std::{
    error::Error as StdError,
    sync::{atomic, Arc},
    time,
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tokio::time::sleep as tokio_sleep;

impl<
        S: AsyncBufReadExt + AsyncWriteExt + Reconnect + Send + Unpin + 'static,
        E: StdError + 'static + Send,
    > Consumer<S, E>
{
    pub(crate) async fn listen_for_heartbeats(
        &mut self,
        statuses: &Vec<Arc<atomic::AtomicUsize>>,
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
