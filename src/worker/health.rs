use super::{Worker, STATUS_QUIET, STATUS_RUNNING, STATUS_TERMINATING};
use crate::{proto::HeartbeatStatus, Error};
use std::{
    error::Error as StdError,
    sync::{atomic, Arc},
    time::{self, Duration},
};

const CHECK_STATE_INTERVAL: Duration = Duration::from_millis(100);
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);

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
    ///
    /// Note that this method is not cancellation safe. We are using an interval timer internally, that
    /// would be reset should we call this method anew. Besides, the `Heartbeat` command is being issued
    /// with the help of `AsyncWriteExt::write_all` which is not cancellation safe either.
    pub(crate) async fn listen_for_heartbeats(
        &mut self,
        statuses: &[Arc<atomic::AtomicUsize>],
    ) -> Result<bool, Error> {
        let mut target = STATUS_RUNNING;

        let mut last = time::Instant::now();
        let mut check_state_interval = tokio::time::interval(CHECK_STATE_INTERVAL);
        check_state_interval.tick().await;

        loop {
            check_state_interval.tick().await;

            // has a worker failed?
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

            if last.elapsed() < HEARTBEAT_INTERVAL {
                // don't send a heartbeat yet
                continue;
            }

            match self.heartbeat().await {
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

    async fn heartbeat(&mut self) -> Result<HeartbeatStatus, Error> {
        let rss_kb = if cfg!(feature = "sysinfo") {
            use sysinfo::{Pid, ProcessesToUpdate};
            self.sys.as_mut().map(|sys| {
                let pid = Pid::from(self.c.opts.pid.expect("every worker to have pid"));
                sys.refresh_processes(ProcessesToUpdate::Some(&[pid]), true);
                let process_stats = sys.process(pid).expect("current process to exist");
                let rss_bytes = process_stats.memory();
                rss_bytes >> 10
            })
        } else {
            None
        };
        self.c.heartbeat(rss_kb).await
    }
}
