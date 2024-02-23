use crate::Job;
use std::{future::Future, pin::Pin};

/// Implementations of this trait can be registered to run jobs in a `Consumer`.
///
/// # Example
///
/// Create a worker with all default options, register a single handler (for the `foo` job
/// type), connect to the Faktory server, and start accepting jobs.
/// The handler is a struct that implements `JobRunner`.
///
/// ```no_run
/// # tokio_test::block_on(async {
/// use faktory::{ConsumerBuilder, JobRunner, Job};
/// use std::io;
///
/// struct MyHandler {
///     config: String,
/// }
///
/// #[async_trait::async_trait]
/// impl JobRunner for MyHandler {
///    type Error = io::Error;
///    async fn run(&self, job: Job) -> Result<(), Self::Error> {
///       println!("config: {}", self.config);
///       println!("job: {:?}", job);
///       Ok(())
///   }
/// }
///
/// let mut c = ConsumerBuilder::default();
/// let handler = MyHandler {
///    config: "bar".to_string(),
/// };
/// c.register_runner("foo", handler);
/// let mut c = c.connect(None).await.unwrap();
/// if let Err(e) = c.run(&["default"]).await {
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
impl<E, F> JobRunner for Box<F>
where
    F: Send + Sync + Fn(Job) -> Pin<Box<dyn Future<Output = Result<(), E>> + Send>>,
{
    type Error = E;
    async fn run(&self, job: Job) -> Result<(), E> {
        self(job).await
    }
}

// Additional Blanket Implementations
#[async_trait::async_trait]
impl<'a, E, F> JobRunner for &'a F
where
    F: Send + Sync + Fn(Job) -> Pin<Box<dyn Future<Output = Result<(), E>> + Send>>,
{
    type Error = E;
    async fn run(&self, job: Job) -> Result<(), E> {
        self(job).await
    }
}

#[async_trait::async_trait]
impl<'a, E, F> JobRunner for &'a mut F
where
    F: Send + Sync + Fn(Job) -> Pin<Box<dyn Future<Output = Result<(), E>> + Send>>,
{
    type Error = E;
    async fn run(&self, job: Job) -> Result<(), E> {
        (self as &F)(job).await
    }
}

#[repr(transparent)]
pub(crate) struct Closure<F>(pub F);

#[async_trait::async_trait]
impl<E, F> JobRunner for Closure<F>
where
    F: Send + Sync + Fn(Job) -> Pin<Box<dyn Future<Output = Result<(), E>> + Send>>,
{
    type Error = E;
    async fn run(&self, job: Job) -> Result<(), E> {
        (self.0)(job).await
    }
}

pub(crate) type BoxedJobRunner<E> = Box<dyn JobRunner<Error = E>>;
