use super::{Client, Reconnect};
use crate::{consumer::Failed, Error, Job};
use std::error::Error as StdError;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};

mod builder;
mod registries;

pub use builder::AsyncConsumerBuilder;
use registries::{CallbacksRegistry, StatesRegistry};

/// Asynchronous version of the [`Consumer`](struct.Consumer.html).
pub struct AsyncConsumer<S: AsyncBufReadExt + AsyncWriteExt + Send, E> {
    c: Client<S>,
    worker_states: Arc<StatesRegistry>,
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
        tokio::spawn(handler(job))
            .await
            .expect("joined ok")
            .map_err(Failed::Application)
    }

    /// Asynchronously fetch and run a single job on the current thread, and then return.
    pub async fn run_one<Q>(&mut self, worker: usize, queues: &[Q]) -> Result<bool, Error>
    where
        Q: AsRef<str> + Sync,
    {
        // get a job
        let job = match self.c.fetch(queues).await? {
            None => return Ok(false),
            Some(j) => j,
        };

        // remember its id
        let jid = job.jid.clone();

        // keep track of running job in case we're terminated during it
        self.worker_states.register_running(worker, jid.clone());

        // process the job
        let _r = self.run_job(job).await;

        // report back
        // ...

        // we won't have to tell the server again
        self.worker_states.reset(worker);

        Ok(true)
    }
}
