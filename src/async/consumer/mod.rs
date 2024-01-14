use std::{
    future::Future,
    sync::{Arc, Mutex},
};

use fnv::FnvHashMap;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};

use super::Client;
use crate::{consumer::WorkerState, proto::ClientOptions, Error, Job};

type JubRunner<E> = dyn Fn(Job) -> Box<dyn Future<Output = Result<(), E>>>;
type BoxedJobRunner<E> = Box<JubRunner<E>>;

struct WorkerStatesRegistry(Vec<Mutex<WorkerState>>);

impl WorkerStatesRegistry {
    fn new(workers_count: usize) -> Self {
        Self((0..workers_count).map(|_| Default::default()).collect())
    }
}

struct CallbacksRegistry<E>(FnvHashMap<String, BoxedJobRunner<E>>);

impl<E> Default for CallbacksRegistry<E> {
    fn default() -> CallbacksRegistry<E> {
        Self(FnvHashMap::default())
    }
}

/// Asynchronous version of the [`Consumer`](struct.Consumer.html).
pub struct AsyncConsumer<S: AsyncBufReadExt + AsyncWriteExt + Send, E> {
    c: Client<S>,
    worker_states: Arc<WorkerStatesRegistry>,
    callbacks: Arc<CallbacksRegistry<E>>,
    terminated: bool,
}

/// Convenience wrapper for building a Faktory worker.
///
/// See the [`AsyncConsumer`] documentation for details.
pub struct AsyncConsumerBuilder<E> {
    opts: ClientOptions,
    workers_count: usize,
    callbacks: CallbacksRegistry<E>,
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
