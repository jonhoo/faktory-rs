use super::AsyncJobRunner;
use crate::consumer::WorkerState;
use fnv::FnvHashMap;
use std::{
    ops::{Deref, DerefMut},
    sync::Mutex,
};

pub(crate) struct WorkerStatesRegistry(Vec<Mutex<WorkerState>>);

impl Deref for WorkerStatesRegistry {
    type Target = Vec<Mutex<WorkerState>>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl WorkerStatesRegistry {
    pub(crate) fn new(workers_count: usize) -> Self {
        Self((0..workers_count).map(|_| Default::default()).collect())
    }

    pub(crate) fn register_running(&self, worker: usize, jid: String) {
        self[worker].lock().expect("lock acquired").running_job = Some(jid);
    }
}

pub(crate) struct CallbacksRegistry<E>(FnvHashMap<String, AsyncJobRunner<E>>);

impl<E> Deref for CallbacksRegistry<E> {
    type Target = FnvHashMap<String, AsyncJobRunner<E>>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<E> DerefMut for CallbacksRegistry<E> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<E> Default for CallbacksRegistry<E> {
    fn default() -> CallbacksRegistry<E> {
        Self(FnvHashMap::default())
    }
}
