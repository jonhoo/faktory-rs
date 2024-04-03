#[cfg(doc)]
use super::Worker;

use crate::Job;
use std::future::Future;

/// Implementations of this trait can be registered to run jobs in a [`Worker`](Worker).
///
/// # Example
///
/// Create a worker with all default options, register a single handler (for the `foo` job
/// type), connect to the Faktory server, and start accepting jobs.
/// The handler is a struct that implements [`JobRunner`].
///
/// ```no_run
/// # tokio_test::block_on(async {
/// use async_trait::async_trait;
/// use faktory::{Job, JobRunner, WorkerBuilder};
/// use std::io;
///
/// struct MyHandler {
///     config: String,
/// }
///
/// #[async_trait]
/// impl JobRunner for MyHandler {
///    type Error = io::Error;
///    async fn run(&self, job: Job) -> Result<(), Self::Error> {
///       println!("config: {}", self.config);
///       println!("job: {:?}", job);
///       Ok(())
///   }
/// }
///
/// let mut w = WorkerBuilder::default();
/// let handler = MyHandler {
///    config: "bar".to_string(),
/// };
/// w.register_runner("foo", handler);
/// let mut w = w.connect(None).await.unwrap();
/// if let Err(e) = w.run(&["default"]).await {
///     println!("worker failed: {}", e);
/// }
/// });
/// ```
#[async_trait::async_trait]
pub trait JobRunner: Send + Sync {
    /// The error type that the handler may return.
    type Error;
    /// A handler function that runs a job.
    async fn run(&self, job: Job) -> Result<(), Self::Error>;
}

// Implements JobRunner for a closure that takes a Job and returns a Result<(), E>
#[async_trait::async_trait]
impl<E, F, Fut> JobRunner for Box<F>
where
    F: Send + Sync + Fn(Job) -> Fut,
    Fut: Future<Output = Result<(), E>> + Send,
{
    type Error = E;
    async fn run(&self, job: Job) -> Result<(), E> {
        self(job).await
    }
}

// Additional Blanket Implementations
#[async_trait::async_trait]
impl<'a, E, F, Fut> JobRunner for &'a F
where
    F: Send + Sync + Fn(Job) -> Fut,
    Fut: Future<Output = Result<(), E>> + Send,
{
    type Error = E;
    async fn run(&self, job: Job) -> Result<(), E> {
        self(job).await
    }
}

#[repr(transparent)]
pub(crate) struct Closure<F>(pub F);

#[async_trait::async_trait]
impl<E, F, Fut> JobRunner for Closure<F>
where
    F: Send + Sync + Fn(Job) -> Fut,
    Fut: Future<Output = Result<(), E>> + Send,
{
    type Error = E;
    async fn run(&self, job: Job) -> Result<(), E> {
        (self.0)(job).await
    }
}

pub(crate) type BoxedJobRunner<E> = Box<dyn JobRunner<Error = E>>;
