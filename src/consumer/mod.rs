use crate::error::Error;

use crate::proto::{
    self, parse_provided_or_from_env, Client, ClientOptions, HeartbeatStatus, Reconnect,
};

use fnv::FnvHashMap;
use std::error::Error as StdError;
use std::io::prelude::*;
use std::net::TcpStream;
use std::sync::{atomic, Arc, Mutex};

use crate::proto::{Ack, Fail, Job};

const STATUS_RUNNING: usize = 0;
const STATUS_QUIET: usize = 1;
const STATUS_TERMINATING: usize = 2;

type JobRunner<E> = dyn Fn(Job) -> Result<(), E> + Send + Sync;
type BoxedJobRunner<E> = Box<JobRunner<E>>;

/// `Consumer` is used to run a worker that processes jobs provided by Faktory.
///
/// # Building the worker
///
/// Faktory needs a decent amount of information from its workers, such as a unique worker ID, a
/// hostname for the worker, its process ID, and a set of labels used to identify the worker. In
/// order to enable setting all these, constructing a worker is a two-step process. You first use a
/// [`ConsumerBuilder`](struct.ConsumerBuilder.html) (which conveniently implements a sensible
/// `Default`) to set the worker metadata, as well as to register any job handlers. You then use
/// one of the `connect_*` methods to finalize the worker and connect to the Faktory server.
///
/// In most cases, `ConsumerBuilder::default()` will do what you want. You only need to augment it
/// with calls to [`register`](struct.ConsumerBuilder.html#method.register) to register handlers
/// for each of your job types, and then you can connect. If you have different *types* of workers,
/// you may also want to use [`labels`](struct.ConsumerBuilder.html#method.labels) to distinguish
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
///  - You can create more than one `Consumer`.
///  - You can call [`ConsumerBuilder::workers`](struct.ConsumerBuilder.html#method.workers) to set
///    the number of worker threads you'd like the `Consumer` to use internally.
///
/// # Connecting to Faktory
///
/// To fetch jobs, the `Consumer` must first be connected to the Faktory server. Exactly how you do
/// that depends on your setup. In most cases, you'll want to use `Consumer::connect`, and provide
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
/// See the [`Producer` examples](struct.Producer.html#examples) for examples of how to connect to
/// different Factory setups.
///
/// # Worker lifecycle
///
/// Okay, so you've built your worker and connected to the Faktory server. Now what?
///
/// If all this process is doing is handling jobs, reconnecting on failure, and exiting when told
/// to by the Faktory server, you should use
/// [`run_to_completion`](struct.Consumer.html#method.run_to_completion). If you want more
/// fine-grained control over the lifetime of your process, you should use
/// [`Consumer::run`](struct.Consumer.html#method.run). See the documentation for each of these
/// methods for details.
///
/// # Examples
///
/// Create a worker with all default options, register a single handler (for the `foobar` job
/// type), connect to the Faktory server, and start accepting jobs.
///
/// ```no_run
/// use faktory::ConsumerBuilder;
/// use std::io;
/// let mut c = ConsumerBuilder::default();
/// c.register("foobar", |job| -> io::Result<()> {
///     println!("{:?}", job);
///     Ok(())
/// });
/// let mut c = c.connect(None).unwrap();
/// if let Err(e) = c.run(&["default"]) {
///     println!("worker failed: {}", e);
/// }
/// ```
pub struct Consumer<S, E>
where
    S: Read + Write,
{
    c: Client<S>,
    worker_states: Arc<Vec<Mutex<WorkerState>>>,
    callbacks: Arc<FnvHashMap<String, BoxedJobRunner<E>>>,
    terminated: bool,
}

#[derive(Default)]
struct WorkerState {
    last_job_result: Option<Result<String, Fail>>,
    running_job: Option<String>,
}

/// Convenience wrapper for building a Faktory worker.
///
/// See the [`Consumer`](struct.Consumer.html) documentation for details.
pub struct ConsumerBuilder<E> {
    opts: ClientOptions,
    workers: usize,
    callbacks: FnvHashMap<String, BoxedJobRunner<E>>,
}

impl<E> Default for ConsumerBuilder<E> {
    /// Construct a new worker with default worker options and the url fetched from environment
    /// variables.
    ///
    /// This will construct a worker where:
    ///
    ///  - `hostname` is this machine's hostname.
    ///  - `wid` is a randomly generated string.
    ///  - `pid` is the OS PID of this process.
    ///  - `labels` is `["rust"]`.
    ///
    fn default() -> Self {
        ConsumerBuilder {
            opts: ClientOptions::default(),
            workers: 1,
            callbacks: Default::default(),
        }
    }
}

impl<E> ConsumerBuilder<E> {
    /// Set the hostname to use for this worker.
    ///
    /// Defaults to the machine's hostname as reported by the operating system.
    pub fn hostname(&mut self, hn: String) -> &mut Self {
        self.opts.hostname = Some(hn);
        self
    }

    /// Set a unique identifier for this worker.
    ///
    /// Defaults to a randomly generated ASCII string.
    pub fn wid(&mut self, wid: String) -> &mut Self {
        self.opts.wid = Some(wid);
        self
    }

    /// Set the labels to use for this worker.
    ///
    /// Defaults to `["rust"]`.
    pub fn labels(&mut self, labels: Vec<String>) -> &mut Self {
        self.opts.labels = labels;
        self
    }

    /// Set the number of workers to use for `run` and `run_to_completion_*`.
    ///
    /// Defaults to 1.
    pub fn workers(&mut self, w: usize) -> &mut Self {
        self.workers = w;
        self
    }

    /// Register a handler function for the given job type (`kind`).
    ///
    /// Whenever a job whose type matches `kind` is fetched from the Faktory, the given handler
    /// function is called with that job as its argument.
    pub fn register<K, H>(&mut self, kind: K, handler: H) -> &mut Self
    where
        K: Into<String>,
        // Annoyingly, can't just use the JobRunner<E> type alias here.
        H: Fn(Job) -> Result<(), E> + Send + Sync + 'static,
    {
        self.callbacks.insert(kind.into(), Box::new(handler));
        self
    }

    /// Connect to a Faktory server.
    ///
    /// If `url` is not given, will use the standard Faktory environment variables. Specifically,
    /// `FAKTORY_PROVIDER` is read to get the name of the environment variable to get the address
    /// from (defaults to `FAKTORY_URL`), and then that environment variable is read to get the
    /// server address. If the latter environment variable is not defined, the connection will be
    /// made to
    ///
    /// ```text
    /// tcp://localhost:7419
    /// ```
    ///
    /// If `url` is given, but does not specify a port, it defaults to 7419.
    pub fn connect(self, url: Option<&str>) -> Result<Consumer<TcpStream, E>, Error> {
        let url = parse_provided_or_from_env(url)?;
        let stream = TcpStream::connect(proto::host_from_url(&url))?;
        Self::connect_with(self, stream, url.password().map(|p| p.to_string()))
    }

    /// Connect to a Faktory server with a non-standard stream.
    pub fn connect_with<S: Read + Write>(
        mut self,
        stream: S,
        pwd: Option<String>,
    ) -> Result<Consumer<S, E>, Error> {
        self.opts.password = pwd;
        Ok(Consumer::new(
            Client::new(stream, self.opts)?,
            self.workers,
            self.callbacks,
        ))
    }
}

enum Failed<E: StdError> {
    Application(E),
    BadJobType(String),
}

impl<E, S: Read + Write> Consumer<S, E> {
    fn new(c: Client<S>, workers: usize, callbacks: FnvHashMap<String, BoxedJobRunner<E>>) -> Self {
        Consumer {
            c,
            callbacks: Arc::new(callbacks),
            worker_states: Arc::new((0..workers).map(|_| Default::default()).collect()),
            terminated: false,
        }
    }
}

impl<E, S: Read + Write + Reconnect> Consumer<S, E> {
    fn reconnect(&mut self) -> Result<(), Error> {
        self.c.reconnect()
    }
}

impl<S, E> Consumer<S, E>
where
    S: Read + Write,
    E: StdError,
{
    fn run_job(&mut self, job: Job) -> Result<(), Failed<E>> {
        match self.callbacks.get(&job.kind) {
            Some(callback) => callback(job).map_err(Failed::Application),
            None => {
                // cannot execute job, since no handler exists
                Err(Failed::BadJobType(job.kind))
            }
        }
    }

    /// Fetch and run a single job on the current thread, and then return.
    pub fn run_one<Q>(&mut self, worker: usize, queues: &[Q]) -> Result<bool, Error>
    where
        Q: AsRef<str>,
    {
        // get a job
        let job = match self.c.fetch(queues)? {
            Some(job) => job,
            None => return Ok(false),
        };

        // remember the job id
        let jid = job.jid.clone();

        // keep track of running job in case we're terminated during it
        self.worker_states[worker].lock().unwrap().running_job = Some(jid.clone());

        // process the job
        let r = self.run_job(job);

        // report back
        match r {
            Ok(_) => {
                // job done -- acknowledge
                // remember it in case we fail to notify the server (e.g., broken connection)
                self.worker_states[worker].lock().unwrap().last_job_result = Some(Ok(jid.clone()));
                self.c.issue(&Ack::new(jid))?.await_ok()?;
            }
            Err(e) => {
                // job failed -- let server know
                // "unknown" is the errtype used by the go library too
                let fail = match e {
                    Failed::BadJobType(jt) => {
                        Fail::new(jid, "unknown", format!("No handler for {}", jt))
                    }
                    Failed::Application(e) => {
                        let mut f = Fail::new(jid, "unknown", format!("{}", e));
                        let mut root = e.source();
                        let mut backtrace = Vec::new();
                        while let Some(r) = root.take() {
                            backtrace.push(format!("{}", r));
                            root = r.source();
                        }
                        f.set_backtrace(backtrace);
                        f
                    }
                };

                let fail2 = fail.clone();
                self.worker_states[worker].lock().unwrap().last_job_result = Some(Err(fail));
                self.c.issue(&fail2)?.await_ok()?;
            }
        }

        // we won't have to tell the server again
        {
            let mut state = self.worker_states[worker].lock().unwrap();
            state.last_job_result = None;
            state.running_job = None;
        }
        Ok(true)
    }

    #[cfg(test)]
    pub(crate) fn run_n<Q>(&mut self, n: usize, queues: &[Q]) -> Result<(), Error>
    where
        Q: AsRef<str>,
    {
        for _ in 0..n {
            self.run_one(0, queues)?;
        }
        Ok(())
    }
}

impl<S, E> Consumer<S, E>
where
    S: Read + Write + Reconnect + Send + 'static,
    E: StdError + 'static,
{
    fn for_worker(&mut self) -> Result<Self, Error> {
        Ok(Consumer {
            c: self.c.connect_again()?,
            callbacks: Arc::clone(&self.callbacks),
            worker_states: Arc::clone(&self.worker_states),
            terminated: self.terminated,
        })
    }

    /// Run this worker on the given `queues` until an I/O error occurs (`Err` is returned), or
    /// until the server tells the worker to disengage (`Ok` is returned).
    ///
    /// The value in an `Ok` indicates the number of workers that may still be processing jobs.
    ///
    /// Note that if the worker fails, [`reconnect()`](struct.Consumer.html#method.reconnect)
    /// should likely be called before calling `run()` again. If an error occurred while reporting
    /// a job success or failure, the result will be re-reported to the server without re-executing
    /// the job. If the worker was terminated (i.e., `run` returns with an `Ok` response), the
    /// worker should **not** try to resume by calling `run` again. This will cause a panic.
    pub fn run<Q>(&mut self, queues: &[Q]) -> Result<usize, Error>
    where
        Q: AsRef<str>,
    {
        assert!(!self.terminated, "do not re-run a terminated worker");
        let worker_states = Arc::get_mut(&mut self.worker_states)
            .expect("all workers are scoped to &mut of the user-code-visible Consumer");

        // retry delivering notification about our last job result.
        // we know there's no leftover thread at this point, so there's no race on the option.
        for wstate in worker_states.iter_mut() {
            let wstate = wstate.get_mut().unwrap();
            if let Some(res) = wstate.last_job_result.take() {
                let r = match res {
                    Ok(ref jid) => self.c.issue(&Ack::new(&**jid)),
                    Err(ref fail) => self.c.issue(fail),
                };

                let r = match r {
                    Ok(r) => r,
                    Err(e) => {
                        wstate.last_job_result = Some(res);
                        return Err(e);
                    }
                };

                if let Err(e) = r.await_ok() {
                    // it could be that the server did previously get our ACK/FAIL, and that it was
                    // the resulting OK that failed. in that case, we would get an error response
                    // when re-sending the job response. this should not count as critical. other
                    // errors, however, should!
                    if let Error::IO(_) = e {
                        wstate.last_job_result = Some(res);
                        return Err(e);
                    }
                }
            }
        }

        // keep track of the current status of each worker
        let status: Vec<_> = (0..self.worker_states.len())
            .map(|_| Arc::new(atomic::AtomicUsize::new(STATUS_RUNNING)))
            .collect();

        // start worker threads
        use std::thread;
        let workers = status
            .iter()
            .enumerate()
            .map(|(worker, status)| {
                let mut w = self.for_worker()?;
                let status = Arc::clone(status);
                let queues: Vec<_> = queues.iter().map(|s| s.as_ref().to_string()).collect();
                Ok(thread::spawn(move || {
                    while status.load(atomic::Ordering::SeqCst) == STATUS_RUNNING {
                        if let Err(e) = w.run_one(worker, &queues[..]) {
                            status.store(STATUS_TERMINATING, atomic::Ordering::SeqCst);
                            return Err(e);
                        }
                    }
                    status.store(STATUS_TERMINATING, atomic::Ordering::SeqCst);
                    Ok(())
                }))
            })
            .collect::<Result<Vec<_>, Error>>()?;

        // listen for heartbeats
        let mut target = STATUS_RUNNING;
        let exit = {
            use std::time;
            let mut last = time::Instant::now();

            loop {
                thread::sleep(time::Duration::from_millis(100));

                // has a worker failed?
                if target == STATUS_RUNNING
                    && status
                        .iter()
                        .any(|s| s.load(atomic::Ordering::SeqCst) == STATUS_TERMINATING)
                {
                    // tell all workers to exit
                    // (though chances are they've all failed already)
                    for s in &status {
                        s.store(STATUS_TERMINATING, atomic::Ordering::SeqCst);
                    }
                    break Ok(false);
                }

                if last.elapsed().as_secs() < 5 {
                    // don't sent a heartbeat yet
                    continue;
                }

                match self.c.heartbeat() {
                    Ok(hb) => {
                        match hb {
                            HeartbeatStatus::Ok => {}
                            HeartbeatStatus::Quiet => {
                                // tell the workers to eventually terminate
                                for s in &status {
                                    s.store(STATUS_QUIET, atomic::Ordering::SeqCst);
                                }
                                target = STATUS_QUIET;
                            }
                            HeartbeatStatus::Terminate => {
                                // tell the workers to terminate
                                // *and* fail the current job and immediately return
                                for s in &status {
                                    s.store(STATUS_QUIET, atomic::Ordering::SeqCst);
                                }
                                break Ok(true);
                            }
                        }
                    }
                    Err(e) => {
                        // for this to fail, the workers have probably also failed
                        for s in &status {
                            s.store(STATUS_TERMINATING, atomic::Ordering::SeqCst);
                        }
                        break Err(e);
                    }
                }
                last = time::Instant::now();
            }
        };

        // there are a couple of cases here:
        //
        //  - we got TERMINATE, so we should just return, even if a worker is still running
        //  - we got TERMINATE and all workers has exited
        //  - we got an error from heartbeat()
        //
        self.terminated = exit.is_ok();
        if let Ok(true) = exit {
            // FAIL currently running jobs even though they're still running
            let mut running = 0;
            for wstate in self.worker_states.iter() {
                if let Some(jid) = wstate.lock().unwrap().running_job.take() {
                    let f = Fail::new(&*jid, "unknown", "terminated");

                    // if this fails, we don't want to exit with Err(),
                    // because we *were* still terminated!
                    let _ = self.c.issue(&f).and_then(|r| r.await_ok()).is_ok();

                    running += 1;
                }
            }

            if running != 0 {
                return Ok(running);
            }
        }

        match exit {
            Ok(_) => {
                // we want to expose any worker errors
                workers
                    .into_iter()
                    .map(|w| w.join().unwrap())
                    .collect::<Result<Vec<_>, _>>()
                    .map(|_| 0)
            }
            Err(e) => {
                // we want to expose worker errors, or otherwise the heartbeat error
                workers
                    .into_iter()
                    .map(|w| w.join().unwrap())
                    .collect::<Result<Vec<_>, _>>()
                    .and(Err(e))
            }
        }
    }

    /// Run this worker until the server tells us to exit or a connection cannot be re-established.
    ///
    /// This function never returns. When the worker decides to exit, the process is terminated.
    pub fn run_to_completion<Q>(mut self, queues: &[Q]) -> !
    where
        Q: AsRef<str>,
    {
        use std::process;
        while self.run(queues).is_err() {
            if self.reconnect().is_err() {
                break;
            }
        }

        process::exit(0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    // https://github.com/rust-lang/rust/pull/42219
    //#[allow_fail]
    #[ignore]
    fn it_works() {
        use crate::producer::Producer;
        use std::io;

        let mut p = Producer::connect(None).unwrap();
        let mut j = Job::new("foobar", vec!["z"]);
        j.queue = "worker_test_1".to_string();
        p.enqueue(j).unwrap();

        let mut c = ConsumerBuilder::default();
        c.register("foobar", |job: Job| -> Result<(), io::Error> {
            assert_eq!(job.args, vec!["z"]);
            Ok(())
        });
        let mut c = c.connect(None).unwrap();
        let e = c.run_n(1, &["worker_test_1"]);
        if e.is_err() {
            println!("{:?}", e);
        }
        assert!(e.is_ok());
    }
}
