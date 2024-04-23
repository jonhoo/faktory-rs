use crate::proto::{Fail, JobId};
use std::{
    ops::{Deref, DerefMut},
    sync::Mutex,
};

#[derive(Default)]
pub(crate) struct WorkerState {
    last_job_result: Option<Result<JobId, Fail>>,
    running_job: Option<JobId>,
}

impl WorkerState {
    pub(crate) fn take_last_result(&mut self) -> Option<Result<JobId, Fail>> {
        self.last_job_result.take()
    }

    pub(crate) fn take_cuurently_running(&mut self) -> Option<JobId> {
        self.running_job.take()
    }

    pub(crate) fn save_last_result(&mut self, res: Result<JobId, Fail>) {
        self.last_job_result = Some(res)
    }
}

pub(crate) struct WorkerStatesRegistry(Vec<Mutex<WorkerState>>);

impl Deref for WorkerStatesRegistry {
    type Target = Vec<Mutex<WorkerState>>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for WorkerStatesRegistry {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<'a> IntoIterator for &'a WorkerStatesRegistry {
    type Item = &'a Mutex<WorkerState>;
    type IntoIter = <&'a Vec<Mutex<WorkerState>> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl<'a> IntoIterator for &'a mut WorkerStatesRegistry {
    type Item = &'a mut Mutex<WorkerState>;
    type IntoIter = <&'a mut Vec<Mutex<WorkerState>> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter_mut()
    }
}

impl WorkerStatesRegistry {
    pub(crate) fn new(workers_count: usize) -> Self {
        Self((0..workers_count).map(|_| Default::default()).collect())
    }

    pub(crate) fn register_running(&self, worker: usize, jid: JobId) {
        self[worker].lock().expect("lock acquired").running_job = Some(jid);
    }

    pub(crate) fn register_success(&self, worker: usize, jid: JobId) {
        self[worker]
            .lock()
            .expect("lock acquired")
            .save_last_result(Ok(jid));
    }

    pub(crate) fn register_failure(&self, worker: usize, f: Fail) {
        self[worker]
            .lock()
            .expect("lock acquired")
            .save_last_result(Err(f));
    }

    pub(crate) fn reset(&self, worker: usize) {
        let mut state = self[worker].lock().expect("lock acquired");
        state.last_job_result = None;
        state.running_job = None;
    }
}
