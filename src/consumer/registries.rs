use super::runner::BoxedJobRunner;
use crate::proto::Fail;
use fnv::FnvHashMap;
use std::{
    ops::{Deref, DerefMut},
    sync::Mutex,
};

// --------------- CALLBACKS (Job Handlers) ----------------

pub(crate) struct CallbacksRegistry<E>(FnvHashMap<String, BoxedJobRunner<E>>);

impl<E> Deref for CallbacksRegistry<E> {
    type Target = FnvHashMap<String, BoxedJobRunner<E>>;
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

// -------------------- WORKER STATES ---------------------

#[derive(Default)]
pub(crate) struct WorkerState {
    pub(crate) last_job_result: Option<Result<String, Fail>>,
    pub(crate) running_job: Option<String>,
}

pub(crate) struct StatesRegistry(Vec<Mutex<WorkerState>>);

impl Deref for StatesRegistry {
    type Target = Vec<Mutex<WorkerState>>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for StatesRegistry {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl StatesRegistry {
    pub(crate) fn new(workers_count: usize) -> Self {
        Self((0..workers_count).map(|_| Default::default()).collect())
    }

    pub(crate) fn register_running(&self, worker: usize, jid: String) {
        self[worker].lock().expect("lock acquired").running_job = Some(jid);
    }

    pub(crate) fn register_success(&self, worker: usize, jid: String) {
        self[worker].lock().expect("lock acquired").last_job_result = Some(Ok(jid));
    }

    pub(crate) fn register_failure(&self, worker: usize, f: &Fail) {
        self[worker].lock().expect("lock acquired").last_job_result = Some(Err(f.clone()));
    }

    pub(crate) fn reset(&self, worker: usize) {
        let mut state = self[worker].lock().expect("lock acquired");
        state.last_job_result = None;
        state.running_job = None;
    }
}
