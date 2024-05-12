use super::proto::{Client, Reconnect};
use crate::error::Error;
use crate::proto::{Ack, Fail, Job};
use fnv::FnvHashMap;
use std::sync::{atomic, Arc};
use std::{error::Error as StdError, sync::atomic::AtomicUsize};
use tokio::io::{AsyncBufRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::task::{AbortHandle, JoinSet};

mod builder;
mod health;
mod runner;
mod state;

pub use builder::WorkerBuilder;
pub use runner::JobRunner;

pub(crate) const STATUS_RUNNING: usize = 0;
pub(crate) const STATUS_QUIET: usize = 1;
pub(crate) const STATUS_TERMINATING: usize = 2;

type CallbacksRegistry<E> = FnvHashMap<String, runner::BoxedJobRunner<E>>;

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
///     .connect(None)
///     .await
///     .unwrap();
///
/// if let Err(e) = w.run(&["default"]).await {
///     println!("worker failed: {}", e);
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
///     .connect(None)
///     .await
///     .unwrap();
/// });
/// ```
///
/// You can also register anything that implements [`JobRunner`] to handle jobs
/// with [`register`](WorkerBuilder::register).
///
pub struct Worker<S: AsyncWrite + Send + Unpin, E> {
    c: Client<S>,
    worker_states: Arc<state::WorkerStatesRegistry>,
    callbacks: Arc<CallbacksRegistry<E>>,
    terminated: bool,
}

impl Worker<TcpStream, ()> {
    /// Creates an ergonomic constructor for a new [`Worker`].
    ///
    /// Also equivalent to [`WorkerBuilder::default`].
    pub fn builder<E>() -> WorkerBuilder<E> {
        WorkerBuilder::default()
    }
}

impl<S: AsyncBufRead + AsyncWrite + Send + Unpin + Reconnect, E> Worker<S, E> {
    async fn reconnect(&mut self) -> Result<(), Error> {
        self.c.reconnect().await
    }
}

impl<S: AsyncWrite + Send + Unpin, E> Worker<S, E> {
    async fn new(c: Client<S>, workers_count: usize, callbacks: CallbacksRegistry<E>) -> Self {
        Worker {
            c,
            callbacks: Arc::new(callbacks),
            worker_states: Arc::new(state::WorkerStatesRegistry::new(workers_count)),
            terminated: false,
        }
    }
}

enum Failed<E: StdError> {
    Application(E),
    BadJobType(String),
}

impl<S: AsyncBufRead + AsyncWrite + Send + Unpin, E: StdError + 'static + Send> Worker<S, E> {
    async fn run_job(&mut self, job: Job) -> Result<(), Failed<E>> {
        let handler = self
            .callbacks
            .get(job.kind())
            .ok_or(Failed::BadJobType(job.kind().to_string()))?;
        handler.run(job).await.map_err(Failed::Application)
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
    async fn force_fail_all_workers(&mut self) -> usize {
        let mut running = 0;
        for wstate in &*self.worker_states {
            let may_be_jid = wstate.lock().unwrap().take_currently_running();
            if let Some(jid) = may_be_jid {
                running += 1;
                let f = Fail::generic(jid, "terminated");
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
    pub async fn run_one<Q>(&mut self, worker: usize, queues: &[Q]) -> Result<bool, Error>
    where
        Q: AsRef<str> + Sync,
    {
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
                    Failed::BadJobType(jt) => Fail::generic(jid, format!("No handler for {}", jt)),
                    Failed::Application(e) => Fail::generic_with_backtrace(jid, e),
                };
                self.worker_states.register_failure(worker, fail.clone());
                self.c.issue(&fail).await?.read_ok().await?;
            }
        }

        self.worker_states.reset(worker);

        Ok(true)
    }
}

impl<
        S: AsyncBufRead + AsyncWrite + Reconnect + Send + Unpin + 'static,
        E: StdError + 'static + Send,
    > Worker<S, E>
{
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

    /// Run this worker on the given `queues` until an I/O error occurs (`Err` is returned), or
    /// until the server tells the worker to disengage (`Ok` is returned).
    ///
    /// The value in an `Ok` indicates the number of workers that may still be processing jobs.
    ///
    /// If an error occurred while reporting a job success or failure, the result will be re-reported to the server
    /// without re-executing the job. If the worker was terminated (i.e., `run` returns  with an `Ok` response),
    /// the worker should **not** try to resume by calling `run` again. This will cause a panic.
    pub async fn run<Q>(&mut self, queues: &[Q]) -> Result<usize, Error>
    where
        Q: AsRef<str>,
    {
        assert!(
            !self.terminated,
            "do not re-run a terminated worker (coordinator)"
        );
        self.report_on_all_workers().await?;

        let workers_count = self.worker_states.len();

        // keep track of the current status of each worker
        let statuses: Vec<_> = (0..workers_count)
            .map(|_| Arc::new(atomic::AtomicUsize::new(STATUS_RUNNING)))
            .collect();

        let mut join_set = JoinSet::new();
        for (worker, status) in statuses.iter().enumerate() {
            let _abort_handle = self
                .spawn_worker_into(&mut join_set, Arc::clone(status), worker, queues)
                .await?;
        }

        let exit = self.listen_for_heartbeats(&statuses).await;

        // there are a couple of cases here:
        //
        //  - we got TERMINATE, so we should just return, even if a worker is still running
        //  - we got TERMINATE and all workers have exited
        //  - we got an error from heartbeat()
        //
        self.terminated = exit.is_ok();

        if let Ok(true) = exit {
            let running = self.force_fail_all_workers().await;
            if running != 0 {
                return Ok(running);
            }
        }

        // we want to expose worker errors, or otherwise the heartbeat error
        let mut results = Vec::with_capacity(workers_count);
        while let Some(res) = join_set.join_next().await {
            results.push(res.expect("joined ok"));
        }

        let result = results.into_iter().collect::<Result<Vec<_>, _>>();

        match exit {
            Ok(_) => result.map(|_| 0),
            Err(e) => result.and(Err(e)),
        }
    }

    /// Run this worker until the server tells us to exit or a connection cannot be re-established.
    ///
    /// This function never returns. When the worker decides to exit, the process is terminated.
    pub async fn run_to_completion<Q>(mut self, queues: &[Q]) -> !
    where
        Q: AsRef<str>,
    {
        use std::process;
        while self.run(queues).await.is_err() {
            if self.reconnect().await.is_err() {
                break;
            }
        }

        process::exit(0);
    }
}
