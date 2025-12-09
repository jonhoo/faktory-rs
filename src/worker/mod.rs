use super::proto::Client;
use crate::error::Error;
use crate::proto::{Ack, Fail, Job};
use fnv::FnvHashMap;
use std::future::Future;
use std::pin::Pin;
use std::process;
use std::sync::{atomic, Arc};
use std::time::Duration;
use std::{error::Error as StdError, sync::atomic::AtomicUsize};
use tokio::task::{spawn, spawn_blocking, AbortHandle, JoinError, JoinSet};
use tokio::time::sleep as tokio_sleep;

mod builder;
mod health;
mod runner;
mod state;
mod stop;
#[cfg(feature = "sysinfo")]
mod system;

pub use builder::WorkerBuilder;
pub use runner::JobRunner;
pub use stop::{StopDetails, StopReason};

pub(crate) const STATUS_RUNNING: usize = 0;
pub(crate) const STATUS_QUIET: usize = 1;
pub(crate) const STATUS_TERMINATING: usize = 2;

type ShutdownSignal = Pin<Box<dyn Future<Output = ()> + 'static + Send>>;

pub(crate) enum Callback<E> {
    Async(runner::BoxedJobRunner<E>),
    Sync(Box<dyn Fn(Job) -> Result<(), E> + Sync + Send + 'static>),
}

type CallbacksRegistry<E> = FnvHashMap<String, Callback<E>>;

/// `Worker` is used to run a worker that processes jobs provided by Faktory.
///
/// # Building the worker
///
/// Faktory needs a decent amount of information from its workers, such as a unique worker ID, a
/// hostname for the worker, its process ID, and a set of labels used to identify the worker. In
/// order to enable setting all these, constructing a worker is a two-step process. You first use a
/// [`WorkerBuilder`] (which conveniently implements a sensible
/// `Default`) to set the worker metadata, as well as to register any job handlers. You then use
/// one of the `connect_*` methods to finalize the worker and connect to the Faktory server.
///
/// In most cases, [`WorkerBuilder::default()`] will do what you want. You only need to augment it
/// with calls to [`register`](WorkerBuilder::register) to register handlers
/// for each of your job types, and then you can connect. If you have different *types* of workers,
/// you may also want to use [`labels`](WorkerBuilder::labels) to distinguish
/// them in the Faktory Web UI. To specify that some jobs should only go to some workers, use
/// different queues.
///
/// ## Handlers
///
/// For each [`Job`](struct.Job.html) that the worker receives, the handler that is registered for
/// that job's type will be called. If a job is received with a type for which no handler exists,
/// the job will be failed and returned to the Faktory server. Similarly, if a handler returns an
/// error response, the job will be failed, and the error reported back to the Faktory server.
///
/// If you are new to Rust, getting the handler types to work out can be a little tricky. If you
/// want to understand why, I highly recommend that you have a look at the chapter on [closures and
/// generic
/// parameters](https://doc.rust-lang.org/book/second-edition/ch13-01-closures.html#using-closures-with-generic-parameters-and-the-fn-traits)
/// in the Rust Book. If you just want it to work, my recommendation is to either use regular
/// functions instead of closures, and giving `&func_name` as the handler, **or** wrapping all your
/// closures in `Box::new()`.
///
/// ## Concurrency
///
/// By default, only a single thread is spun up to process the jobs given to this worker. If you
/// want to dedicate more resources to processing jobs, you have a number of options listed below.
/// As you go down the list below, efficiency increases, but fault isolation decreases. I will not
/// give further detail here, but rather recommend that if these don't mean much to you, you should
/// use the last approach and let the library handle the concurrency for you.
///
///  - You can spin up more worker processes by launching your worker program more than once.
///  - You can create more than one `Worker`.
///  - You can call [`WorkerBuilder::workers`] to set
///    the number of worker threads you'd like the `Worker` to use internally.
///
/// # Connecting to Faktory
///
/// To fetch jobs, the `Worker` must first be connected to the Faktory server. Exactly how you do
/// that depends on your setup. In most cases, you'll want to use [`WorkerBuilder::connect`], and provide
/// a connection URL. If you supply a URL, it must be of the form:
///
/// ```text
/// protocol://[:password@]hostname[:port]
/// ```
///
/// Faktory suggests using the `FAKTORY_PROVIDER` and `FAKTORY_URL` environment variables (see
/// their docs for more information) with `localhost:7419` as the fallback default. If you want
/// this behavior, pass `None` as the URL.
///
/// See the [`Client` examples](struct.Client.html#examples) for examples of how to connect to
/// different Factory setups.
///
/// # Worker lifecycle
///
/// Okay, so you've built your worker and connected to the Faktory server. Now what?
///
/// If all this process is doing is handling jobs, reconnecting on failure, and exiting when told
/// to by the Faktory server, you should use
/// [`run_to_completion`](Worker::run_to_completion). If you want more
/// fine-grained control over the lifetime of your process, you should use
/// [`run`](Worker::run). See the documentation for each of these
/// methods for details.
///
/// # Examples
///
/// Create a worker with all default options, register a single handler (for the `foo` job
/// type), connect to the Faktory server, and start accepting jobs.
///
/// ```no_run
/// # tokio_test::block_on(async {
/// use faktory::{Worker, Job};
/// use std::io;
///
/// async fn process_job(job: Job) -> io::Result<()> {
///     println!("{:?}", job);
///     Ok(())
/// }
///
/// let mut w = Worker::builder()
///     .register_fn("foo", process_job)
///     .connect()
///     .await
///     .unwrap();
///
/// match w.run(&["default"]).await {
///     Err(e) => println!("worker failed: {}", e),
///     Ok(stop_details) => {
///         println!(
///            "Stop reason: {}, number of workers that were running: {}",
///             stop_details.reason,
///             stop_details.workers_still_running
///         );
///     }
/// }
/// # });
/// ```
///
/// Handler can be inlined.
///
/// ```no_run
/// # tokio_test::block_on(async {
/// # use faktory::Worker;
/// # use std::io;
/// let _w = Worker::builder()
///     .register_fn("bar", |job| async move {
///         println!("{:?}", job);
///         Ok::<(), io::Error>(())
///     })
///     .connect()
///     .await
///     .unwrap();
/// });
/// ```
///
/// You can also register anything that implements [`JobRunner`] to handle jobs
/// with [`register`](WorkerBuilder::register).
///
pub struct Worker<E> {
    c: Client,
    worker_states: Arc<state::WorkerStatesRegistry>,
    callbacks: Arc<CallbacksRegistry<E>>,
    terminated: bool,
    forever: bool,
    shutdown_timeout: Option<Duration>,

    // NOTE: this is always `Some` if `self.terminated == false` whenever any `pub` function
    // on this type returns. it is `Some(std::future::pending())` if no shutdown signaler is set.
    shutdown_signal: Option<ShutdownSignal>,

    // NOTE: we are storing quite a few worker-related things on the ClientOptions
    // (e.g. wid, hostname), but not doing so with `System`, because
    // 1) it's not `Clone` and so we would need an Arc
    // 2) we need it mutable and so we would need a Mutex
    // 3) it's actually only coordinator who should have access to it anyways
    #[cfg(feature = "sysinfo")]
    sys: Option<system::System>,
}

impl Worker<()> {
    /// Creates an ergonomic constructor for a new [`Worker`].
    ///
    /// Also equivalent to [`WorkerBuilder::default`].
    pub fn builder<E>() -> WorkerBuilder<E> {
        WorkerBuilder::default()
    }
}

impl<E> Worker<E> {
    async fn reconnect(&mut self) -> Result<(), Error> {
        self.c.reconnect().await
    }
}

impl<E> Worker<E> {
    fn new(
        c: Client,
        workers_count: usize,
        callbacks: CallbacksRegistry<E>,
        shutdown_timeout: Option<Duration>,
        shutdown_signal: Option<ShutdownSignal>,
        #[cfg(feature = "sysinfo")] sys: Option<system::System>,
    ) -> Self {
        Worker {
            c,
            callbacks: Arc::new(callbacks),
            worker_states: Arc::new(state::WorkerStatesRegistry::new(workers_count)),
            terminated: false,
            forever: false,
            shutdown_timeout,
            shutdown_signal: Some(
                shutdown_signal.unwrap_or_else(|| Box::pin(std::future::pending())),
            ),
            #[cfg(feature = "sysinfo")]
            sys,
        }
    }

    /// Tell if this worker has been terminated.
    ///
    /// Running a terminate worker ([`Worker::run_one`], [`Worker::run`], [`Worker::run_to_completion`])
    /// will cause a panic. If the worker is terminated, you will need to build and run a new worker instead.
    pub fn is_terminated(&self) -> bool {
        self.terminated
    }
}

enum Failed<E: StdError, JE: StdError> {
    Application(E),
    HandlerPanic(JE),
    BadJobType(String),
}

impl<E: StdError + 'static + Send> Worker<E> {
    async fn run_job(&mut self, job: Job) -> Result<(), Failed<E, JoinError>> {
        let handler = self
            .callbacks
            .get(&job.kind)
            .ok_or(Failed::BadJobType(job.kind().to_string()))?;
        let spawning_result = match handler {
            Callback::Async(_) => {
                let callbacks = self.callbacks.clone();
                let processing_task = async move {
                    let callback = callbacks.get(&job.kind).unwrap();
                    if let Callback::Async(cb) = callback {
                        cb.run(job).await
                    } else {
                        unreachable!()
                    }
                };
                spawn(processing_task).await
            }
            Callback::Sync(_) => {
                let callbacks = self.callbacks.clone();
                let processing_task = move || {
                    let callback = callbacks.get(&job.kind).unwrap();
                    if let Callback::Sync(cb) = callback {
                        cb(job)
                    } else {
                        unreachable!()
                    }
                };
                spawn_blocking(processing_task).await
            }
        };
        match spawning_result {
            Err(join_error) => Err(Failed::HandlerPanic(join_error)),
            Ok(processing_result) => processing_result.map_err(Failed::Application),
        }
    }

    async fn report_on_all_workers(&mut self) -> Result<(), Error> {
        let worker_states = Arc::get_mut(&mut self.worker_states)
            .expect("all workers are scoped to &mut of the user-code-visible Worker");

        // retry delivering notification about our last job result.
        // we know there's no leftover thread at this point, so there's no race on the option.
        for wstate in worker_states {
            let wstate = wstate.get_mut().unwrap();
            if let Some(res) = wstate.take_last_result() {
                let r = match res {
                    Ok(ref jid) => self.c.issue(&Ack::new(jid.clone())).await,
                    Err(ref fail) => self.c.issue(fail).await,
                };

                let r = match r {
                    Ok(r) => r,
                    Err(e) => {
                        wstate.save_last_result(res);
                        return Err(e);
                    }
                };

                if let Err(e) = r.read_ok().await {
                    // it could be that the server did previously get our ACK/FAIL, and that it was
                    // the resulting OK that failed. in that case, we would get an error response
                    // when re-sending the job response. this should not count as critical. other
                    // errors, however, should!
                    if let Error::IO(_) = e {
                        wstate.save_last_result(res);
                        return Err(e);
                    }
                }
            }
        }

        Ok(())
    }

    /// Fail currently running jobs.
    ///
    /// This will FAIL _all_ the jobs even though they're still running.
    /// Returns the number of workers that may still be processing jobs.
    async fn force_fail_all_workers(&mut self, reason: &str) -> usize {
        let mut running = 0;
        for wstate in &*self.worker_states {
            let may_be_jid = wstate.lock().unwrap().take_currently_running();
            if let Some(jid) = may_be_jid {
                running += 1;
                let f = Fail::generic(jid, reason);
                let _ = match self.c.issue(&f).await {
                    Ok(r) => r.read_ok().await,
                    // We are ignoring any FAIL command issue errors, since this is already
                    // an "emergency" case.
                    Err(_) => continue,
                }
                .is_ok();
            }
        }
        running
    }

    /// Fetch and run a single job, and then return.
    ///
    /// Note that if you called [`Worker::run`] on this worker previously and the run
    /// discontinued due to a signal from the Faktory server or a graceful shutdown signal,
    /// calling this method will mean you are trying to run a _terminated_ worker which will
    /// cause a panic. You will need to build and run a new worker instead.
    ///
    /// You can check if the worker has been terminated with [`Worker::is_terminated`].
    pub async fn run_one<Q>(&mut self, worker: usize, queues: &[Q]) -> Result<bool, Error>
    where
        Q: AsRef<str> + Sync,
    {
        assert!(
            !self.terminated,
            "do not re-run a terminated worker (coordinator)"
        );

        let job = match self.c.fetch(queues).await? {
            None => return Ok(false),
            Some(j) => j,
        };

        let jid = job.jid.clone();

        self.worker_states.register_running(worker, jid.clone());

        match self.run_job(job).await {
            Ok(_) => {
                self.worker_states.register_success(worker, jid.clone());
                self.c.issue(&Ack::new(jid)).await?.read_ok().await?;
            }
            Err(e) => {
                let fail = match e {
                    Failed::BadJobType(jt) => Fail::generic(jid, format!("No handler for {jt}")),
                    Failed::Application(e) => Fail::generic_with_backtrace(jid, e),
                    Failed::HandlerPanic(e) => {
                        if e.is_cancelled() {
                            Fail::generic(jid, "job processing was cancelled")
                        } else if e.is_panic() {
                            let panic_obj = e.into_panic();
                            if panic_obj.is::<String>() {
                                Fail::generic(jid, *panic_obj.downcast::<String>().unwrap())
                            } else if panic_obj.is::<&'static str>() {
                                Fail::generic(jid, *panic_obj.downcast::<&'static str>().unwrap())
                            } else {
                                Fail::generic(jid, "job processing panicked")
                            }
                        } else {
                            Fail::generic_with_backtrace(jid, e)
                        }
                    }
                };
                self.worker_states.register_failure(worker, fail.clone());
                self.c.issue(&fail).await?.read_ok().await?;
            }
        }

        self.worker_states.reset(worker);

        Ok(true)
    }
}

impl<E: StdError + 'static + Send> Worker<E> {
    async fn for_worker(&mut self) -> Result<Self, Error> {
        Ok(Worker {
            // We actually only need:
            //
            // 1) a connected client;
            // 2) access to callback registry;
            // 3) access to this worker's state (not all of them)
            //
            // For simplicity, we are currently creating a processing worker as a full replica
            // of the coordinating worker.
            //
            // In the future though this can be updated to strip off `terminated` from
            // the processing worker (as unused) and disallow access to other processing workers'
            // states from inside this processing worker (as privilege not needed).
            //
            c: self.c.connect_again().await?,
            callbacks: Arc::clone(&self.callbacks),
            worker_states: Arc::clone(&self.worker_states),
            terminated: self.terminated,
            forever: self.forever,
            shutdown_timeout: self.shutdown_timeout,
            shutdown_signal: Some(Box::pin(std::future::pending())),
            #[cfg(feature = "sysinfo")]
            sys: None,
        })
    }

    async fn spawn_worker_into<Q>(
        &mut self,
        set: &mut JoinSet<Result<(), Error>>,
        status: Arc<AtomicUsize>,
        worker: usize,
        queues: &[Q],
    ) -> Result<AbortHandle, Error>
    where
        Q: AsRef<str>,
    {
        let mut w = self.for_worker().await?;
        let queues: Vec<_> = queues.iter().map(|s| s.as_ref().to_string()).collect();
        Ok(set.spawn(async move {
            while status.load(atomic::Ordering::SeqCst) == STATUS_RUNNING {
                if let Err(e) = w.run_one(worker, &queues[..]).await {
                    status.store(STATUS_TERMINATING, atomic::Ordering::SeqCst);
                    return Err(e);
                }
            }
            status.store(STATUS_TERMINATING, atomic::Ordering::SeqCst);
            Ok(())
        }))
    }

    /// Run this worker on the given `queues`.
    ///
    /// Will run the worker until an I/O error occurs (`Err` is returned), or until the server tells the worker
    /// to disengage (`Ok` is returned), or a signal from the user-space code has been received via a future
    /// supplied to [`WorkerBuilder::with_graceful_shutdown`](`Ok` is returned).
    ///
    /// The value in an `Ok` holds [`details`](StopDetails) about the reason why the run has discontinued (see [`StopReason`])
    /// and the number of workers that may still be processing jobs. Note that `0` in [`StopDetails::workers_still_running`]
    /// can also indicate that the [graceful shutdown period](WorkerBuilder::shutdown_timeout) has been exceeded.
    ///
    /// If an error occurred while reporting a job success or failure, the result will be re-reported to the server
    /// without re-executing the job. If the worker was terminated (i.e., `run` returns  with an `Ok` response),
    /// the worker should **not** try to resume by calling `run` again. This will cause a panic.
    ///
    /// Note that if you provided a shutdown signal when building this worker (see [`WorkerBuilder::with_graceful_shutdown`]),
    /// and this signal resolved, the worker will be marked as terminated and calling this method will cause a panic.
    /// You will need to build and run a new worker instead.
    ///
    /// You can check if the worker has been terminated with [`Worker::is_terminated`].
    pub async fn run<Q>(&mut self, queues: &[Q]) -> Result<StopDetails, Error>
    where
        Q: AsRef<str>,
    {
        assert!(
            !self.terminated,
            "do not re-run a terminated worker (coordinator)"
        );
        self.report_on_all_workers().await?;

        let nworkers = self.worker_states.len();

        // keep track of the current status of each worker
        let statuses: Vec<_> = (0..nworkers)
            .map(|_| Arc::new(atomic::AtomicUsize::new(STATUS_RUNNING)))
            .collect();

        let mut join_set = JoinSet::new();
        for (worker, status) in statuses.iter().enumerate() {
            let _abort_handle = self
                .spawn_worker_into(&mut join_set, Arc::clone(status), worker, queues)
                .await?;
        }

        // the only place `shutdown_signal` is set to `None` is when we `.take()` it here.
        // later on, we maintain the invariant that either `self.terminated` is set to `true` OR we
        // restore `shutdown_signal` to `Some` (such as if the heartbeat future fails). in the
        // former case, we'll never hit this `.take()` again due to the `assert` above, and in the
        // latter case the `take()` will yet again succeed.
        let mut shutdown_signal = self
            .shutdown_signal
            .take()
            .expect("see shutdown_signal comment");
        // we set terminated = true proactively to maintain the invariant with shutdown_signal even
        // in the case of a panic. it gets set to false in the heartbeat error case.
        self.terminated = true;
        let maybe_shutdown_timeout = self.shutdown_timeout;

        let report = tokio::select! {
            // A signal from the user space received.
            _ = &mut shutdown_signal => {
                let nrunning = tokio::select! {
                     _ = async { tokio_sleep(maybe_shutdown_timeout.unwrap()).await; }, if maybe_shutdown_timeout.is_some() => {
                        0
                    },
                    nrunning = self.force_fail_all_workers("termination signal received from user space") => {
                        nrunning
                    }
                };
                Ok(stop::StopDetails::new(StopReason::GracefulShutdown, nrunning))
            },
            // A signal from the Faktory server received or an error occurred.
            // Even though `Worker::listen_for_hearbeats` is not cancellation safe, we are ok using it here,
            // since we are not `select!`ing in a loop and we are eventually _either_ marking the worker as
            // terminated, _or_ re-creating a connection, i.e. we never end up re-using the "broken" connection.
            exit = self.listen_for_heartbeats(&statuses) => {
                // there are a couple of cases here:
                //  - we got TERMINATE, so we should just return, even if a worker is still running
                //  - we got TERMINATE and all workers have exited
                //  - we got an error from heartbeat()
                //
                // note that if it is an error from heartbeat(), the worker will _not_ be marked as
                // terminated and _can_ be restarted
                if exit.is_err() {
                    self.terminated = false;
                }
                // restore shutdown signal since it has not resolved
                self.shutdown_signal = Some(shutdown_signal);

                if let Ok(true) = exit {
                    let nrunning = self.force_fail_all_workers("terminated").await;
                    if nrunning != 0 {
                        return Ok(stop::StopDetails::new(StopReason::ServerInstruction, nrunning));
                    }
                }

                // we want to expose worker errors, or otherwise the heartbeat error
                let mut results = Vec::with_capacity(nworkers);
                while let Some(res) = join_set.join_next().await {
                    results.push(res.expect("joined ok"));
                }
                let results = results.into_iter().collect::<Result<Vec<_>, _>>();

                match exit {
                    Ok(_) => results.map(|_| stop::StopDetails::new(StopReason::ServerInstruction, 0)),
                    Err(e) => results.and(Err(e)),
                }
            }
        };

        report
    }

    /// Run this worker until the server tells us to exit or a connection cannot be re-established,
    /// or a signal from the user-space code has been received via a future passed to [`WorkerBuilder::with_graceful_shutdown`]
    ///
    /// This function never returns. When the worker decides to exit or a signal to shutdown gracefully
    /// has been received, the process is terminated within the [shutdown period](WorkerBuilder::shutdown_timeout).
    pub async fn run_to_completion<Q>(mut self, queues: &[Q]) -> !
    where
        Q: AsRef<str>,
    {
        while self.run(queues).await.is_err() {
            if self.reconnect().await.is_err() {
                break;
            }
        }

        process::exit(0);
    }
}
