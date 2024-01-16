// use super::{Client, AsynReconnect};
use crate::{
    consumer::{Failed, STATUS_RUNNING, STATUS_TERMINATING},
    proto::{Ack, Fail},
    Error, Job,
};

use std::sync::{atomic, Arc};
use std::{error::Error as StdError, sync::atomic::AtomicUsize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use tokio::task::JoinHandle;

mod builder;
mod health;
mod registries;

pub use builder::AsyncConsumerBuilder;
use registries::{CallbacksRegistry, StatesRegistry};

use super::proto::{AsyncReconnect, Client};

/// Asynchronous version of the [`Consumer`](struct.Consumer.html).
pub struct AsyncConsumer<S: AsyncBufReadExt + AsyncWriteExt + Send, E> {
    c: Client<S>,
    worker_states: Arc<StatesRegistry>,
    callbacks: Arc<CallbacksRegistry<E>>,
    terminated: bool,
}

impl<S: AsyncBufReadExt + AsyncWriteExt + Send + Unpin + AsyncReconnect, E> AsyncConsumer<S, E> {
    async fn reconnect(&mut self) -> Result<(), Error> {
        self.c.reconnect().await
    }
}

impl<S: AsyncBufReadExt + AsyncWriteExt + Send + Unpin, E> AsyncConsumer<S, E> {
    async fn new(c: Client<S>, workers_count: usize, callbacks: CallbacksRegistry<E>) -> Self {
        AsyncConsumer {
            c,
            callbacks: Arc::new(callbacks),
            worker_states: Arc::new(StatesRegistry::new(workers_count)),
            terminated: false,
        }
    }
}

impl<S: AsyncBufReadExt + AsyncWriteExt + Send + Unpin, E: StdError + 'static + Send>
    AsyncConsumer<S, E>
{
    async fn run_job(&mut self, job: Job) -> Result<(), Failed<E>> {
        let handler = self
            .callbacks
            .get(job.kind())
            .ok_or(Failed::BadJobType(job.kind().to_string()))?;
        handler(job).await.map_err(Failed::Application)
    }

    async fn report_failure_to_server(&mut self, f: &Fail) -> Result<(), Error> {
        self.c.issue(f).await?.read_ok().await
    }

    async fn report_success_to_server(&mut self, jid: impl Into<String>) -> Result<(), Error> {
        self.c.issue(&Ack::new(jid)).await?.read_ok().await
    }

    async fn report_on_all_workers(&mut self) -> Result<(), Error> {
        let worker_states = Arc::get_mut(&mut self.worker_states)
            .expect("all workers are scoped to &mut of the user-code-visible Consumer");

        // retry delivering notification about our last job result.
        // we know there's no leftover thread at this point, so there's no race on the option.
        for wstate in worker_states.iter_mut() {
            let wstate = wstate.get_mut().unwrap();
            if let Some(res) = wstate.last_job_result.take() {
                let r = match res {
                    Ok(ref jid) => self.c.issue(&Ack::new(jid)).await,
                    Err(ref fail) => self.c.issue(fail).await,
                };

                let r = match r {
                    Ok(r) => r,
                    Err(e) => {
                        wstate.last_job_result = Some(res);
                        return Err(e);
                    }
                };

                if let Err(e) = r.read_ok().await {
                    // it could be that the server did previously get our ACK/FAIL, and that it was
                    // the resulting OK that failed. in that case, we would get an error response
                    // when re-sending the job response. this should not count as critical. other
                    // errors, however, should!
                    if let Error::IO(_) = e {
                        wstate.last_job_result = Some(res);
                        return Err(e);
                    }
                }
            }
        }

        Ok(())
    }

    /// Asynchronously fetch and run a single job, and then return.
    pub async fn run_one<Q>(&mut self, worker: usize, queues: &[Q]) -> Result<bool, Error>
    where
        Q: AsRef<str> + Sync,
    {
        let job = match self.c.fetch(queues).await? {
            None => return Ok(false),
            Some(j) => j,
        };

        let jid = job.jid.clone();

        self.worker_states.register_running(worker, jid.clone());

        match self.run_job(job).await {
            Ok(_) => {
                self.worker_states.register_success(worker, jid.clone());
                self.report_success_to_server(jid).await?;
            }
            Err(e) => {
                let fail = match e {
                    Failed::BadJobType(jt) => Fail::generic(jid, format!("No handler for {}", jt)),
                    Failed::Application(e) => Fail::generic_with_backtrace(jid, e),
                };
                self.worker_states.register_failure(worker, &fail);
                self.report_failure_to_server(&fail).await?;
            }
        }

        self.worker_states.reset(worker);

        Ok(true)
    }
}

impl<
        S: AsyncBufReadExt + AsyncWriteExt + AsyncReconnect + Send + Unpin + 'static,
        E: StdError + 'static + Send,
    > AsyncConsumer<S, E>
{
    async fn for_worker(&mut self) -> Result<Self, Error> {
        Ok(AsyncConsumer {
            c: self.c.connect_again().await?,
            callbacks: Arc::clone(&self.callbacks),
            worker_states: Arc::clone(&self.worker_states),
            terminated: self.terminated,
        })
    }

    async fn spawn_worker<Q>(
        &mut self,
        status: Arc<AtomicUsize>,
        worker: usize,
        queues: &[Q],
    ) -> Result<JoinHandle<Result<(), Error>>, Error>
    where
        Q: AsRef<str>,
    {
        let mut w = self.for_worker().await?;
        let queues: Vec<_> = queues.iter().map(|s| s.as_ref().to_string()).collect();
        Ok(tokio::spawn(async move {
            while status.load(atomic::Ordering::SeqCst) == STATUS_RUNNING {
                if let Err(e) = w.run_one(worker, &queues[..]).await {
                    status.store(STATUS_TERMINATING, atomic::Ordering::SeqCst);
                    return Err(e);
                }
            }
            status.store(STATUS_TERMINATING, atomic::Ordering::SeqCst);
            Ok(())
        }))
    }

    /// Async version of [`run`](struct.Consumer.html#structmethod.run).
    pub async fn run<Q>(&mut self, queues: &[Q]) -> Result<(), Error>
    where
        Q: AsRef<str>,
    {
        assert!(!self.terminated, "do not re-run a terminated worker");
        self.report_on_all_workers().await?;

        // keep track of the current status of each worker
        let statuses: Vec<_> = (0..self.worker_states.len())
            .map(|_| Arc::new(atomic::AtomicUsize::new(STATUS_RUNNING)))
            .collect();

        let mut spawned_workers = Vec::with_capacity(self.worker_states.len());
        for i in 0..self.worker_states.len() {
            let handle = self
                .spawn_worker(Arc::clone(&statuses[i]), i, queues)
                .await?;
            spawned_workers.push(handle)
        }

        let _exit = self.listen_for_heartbeats(&statuses).await;

        Ok(())
    }

    /// Async version of [`run_to_completion`](struct.Consumer.html#structmethod.run_to_completion).
    pub async fn run_to_completion<Q>(mut self, queues: &[Q]) -> !
    where
        Q: AsRef<str>,
    {
        use std::process;
        while self.run(queues).await.is_err() {
            if self.reconnect().await.is_err() {
                break;
            }
        }

        process::exit(0);
    }
}
