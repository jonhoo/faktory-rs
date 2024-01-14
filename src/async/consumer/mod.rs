use super::{Client, Reconnect};
use crate::{consumer::Failed, Error, Job};
use std::error::Error as StdError;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};

mod builder;
mod registries;
mod runner;

pub use builder::AsyncConsumerBuilder;
use registries::{CallbacksRegistry, WorkerStatesRegistry};
pub use runner::AsyncJobRunner;

/// Asynchronous version of the [`Consumer`](struct.Consumer.html).
pub struct AsyncConsumer<S: AsyncBufReadExt + AsyncWriteExt + Send, E> {
    c: Client<S>,
    worker_states: Arc<WorkerStatesRegistry>,
    callbacks: Arc<CallbacksRegistry<E>>,
    terminated: bool,
}

impl<S: AsyncBufReadExt + AsyncWriteExt + Send + Unpin + Reconnect, E> AsyncConsumer<S, E> {
    async fn reconnect(&mut self) -> Result<(), Error> {
        self.c.reconnect().await
    }
}

impl<S: AsyncBufReadExt + AsyncWriteExt + Send + Unpin, E> AsyncConsumer<S, E> {
    async fn new(c: Client<S>, workers_count: usize, callbacks: CallbacksRegistry<E>) -> Self {
        AsyncConsumer {
            c,
            callbacks: Arc::new(callbacks),
            worker_states: Arc::new(WorkerStatesRegistry::new(workers_count)),
            terminated: false,
        }
    }
}

impl<S: AsyncBufReadExt + AsyncWriteExt + Send + Unpin, E: StdError + 'static> AsyncConsumer<S, E> {
    async fn run_job(&mut self, job: Job) -> Result<(), Failed<E>> {
        self.callbacks
            .get(job.kind())
            .ok_or(Failed::BadJobType(job.kind().to_string()))?
            .run(job)
            .await
            .map_err(Failed::Application)
    }

    async fn run_one<Q>(&mut self, worker: usize, queues: &[Q]) -> Result<bool, Error>
    where
        Q: AsRef<str> + Sync,
    {
        // get a job ...
        let job = match self.c.fetch(queues).await? {
            None => return Ok(false),
            Some(j) => j,
        };

        // ... and remember its id
        let jid = job.jid.clone();

        // keep track of running job in case we're terminated during it:
        self.worker_states.register_running(worker, jid.clone());

        // process the job
        let r = self.run_job(job).await;
        Ok(false)
    }
}
