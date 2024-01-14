use crate::Job;
use std::{future::Future, pin::Pin};

/// A convenience wrapper type for a [`Job`] handler.
pub struct AsyncJobRunner<E>(Box<dyn Fn(Job) -> Pin<Box<dyn Future<Output = Result<(), E>>>>>);

impl<E: 'static> AsyncJobRunner<E> {
    /// Creates a new `JobRunner`.
    pub fn new(
        runner: impl Fn(Job) -> Pin<Box<dyn Future<Output = Result<(), E>>>> + 'static,
    ) -> AsyncJobRunner<E> {
        Self(Box::new(move |job| Box::pin(runner(job))))
    }

    pub(crate) async fn run(&self, job: Job) -> Result<(), E> {
        self.0(job).await
    }
}
