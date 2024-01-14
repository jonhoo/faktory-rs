use super::{Client, Reconnect};
use crate::Error;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};

mod builder;
mod registries;

pub use builder::AsyncConsumerBuilder;
pub use registries::AsyncJobRunner;
use registries::{CallbacksRegistry, WorkerStatesRegistry};

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

    async fn run_one<Q>(&mut self, worker: usize, queues: &[Q]) -> Result<bool, Error>
    where
        Q: AsRef<str>,
    {
        Ok(false)
    }
}
