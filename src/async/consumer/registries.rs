use super::BoxAsyncJobRunner;
use crate::consumer::WorkerState;
use fnv::FnvHashMap;
use std::{
    ops::{Deref, DerefMut},
    sync::Mutex,
};

pub(crate) struct StatesRegistry(Vec<Mutex<WorkerState>>);

impl Deref for StatesRegistry {
    type Target = Vec<Mutex<WorkerState>>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl StatesRegistry {
    pub(crate) fn new(workers_count: usize) -> Self {
        Self((0..workers_count).map(|_| Default::default()).collect())
    }

    pub(crate) fn register_running(&self, worker: usize, jid: String) {
        self[worker].lock().expect("lock acquired").running_job = Some(jid);
    }

    pub(crate) fn reset(&self, worker: usize) {
        let mut state = self[worker].lock().expect("lock acquired");
        state.last_job_result = None;
        state.running_job = None;
    }
}

pub(crate) struct CallbacksRegistry<E>(FnvHashMap<String, BoxAsyncJobRunner<E>>);

impl<E> Deref for CallbacksRegistry<E> {
    type Target = FnvHashMap<String, BoxAsyncJobRunner<E>>;
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
