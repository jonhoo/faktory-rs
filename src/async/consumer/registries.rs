use std::{future::Future, sync::Mutex};
use crate::{consumer::WorkerState, Job};
use fnv::FnvHashMap;

/// A convenience wrapper type for a [`Job`] handler.
pub struct AsyncJobRunner<E>(Box<dyn Fn(Job) -> Box<dyn Future<Output = Result<(), E>>>>);

impl<E> AsyncJobRunner<E> {
    /// Creates a new `JobRunner`.
    pub fn new(
        runner: impl Fn(Job) -> Box<dyn Future<Output = Result<(), E>>> + 'static,
    ) -> AsyncJobRunner<E> {
        Self(Box::new(runner))
    }
}

pub(crate) struct WorkerStatesRegistry(Vec<Mutex<WorkerState>>);

impl WorkerStatesRegistry {
    pub(crate) fn new(workers_count: usize) -> Self {
        Self((0..workers_count).map(|_| Default::default()).collect())
    }
}

pub(crate) struct CallbacksRegistry<E>(FnvHashMap<String, AsyncJobRunner<E>>);

impl<E> Default for CallbacksRegistry<E> {
    fn default() -> CallbacksRegistry<E> {
        Self(FnvHashMap::default())
    }
}

impl<E> CallbacksRegistry<E> {
    pub(crate) fn register(&mut self, kind: String, runner: AsyncJobRunner<E>) {
        self.0.insert(kind, runner);
    }
}
