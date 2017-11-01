use std::io::prelude::*;
use std::io;
use std::error::Error;
use proto::{Client, ClientOptions, HeartbeatStatus, StreamConnector};
use std::collections::HashMap;
use std::sync::{atomic, Arc, Mutex};
use atomic_option::AtomicOption;

use proto::{Ack, Fail, Job};

const STATUS_RUNNING: usize = 0;
const STATUS_QUIET: usize = 1;
const STATUS_TERMINATING: usize = 2;

/// This struct represents a single Faktory worker.
///
/// The worker consumes jobs fetched from the Faktory server, processes them, and reports the
/// results back to the Faktory server upon completion.
///
/// A worker should be constructed using a [`ConsumerBuilder`](struct.ConsumerBuilder.html), so
/// that any non-default worker parameters can be set.
///
/// ```no_run
/// use faktory::{Consumer, TcpEstablisher};
/// use std::io;
/// let mut c = Consumer::default::<TcpEstablisher>().unwrap();
/// c.register("foobar", |job| -> io::Result<()> {
///     println!("{:?}", job);
///     Ok(())
/// });
/// if let Err(e) = c.run(&["default"]) {
///     println!("worker failed: {}", e);
/// }
/// ```
pub struct Consumer<S, F>
where
    S: Read + Write,
{
    c: Arc<Mutex<Client<S>>>,
    last_job_result: Arc<AtomicOption<Result<String, Fail>>>,
    running_job: Arc<AtomicOption<String>>,
    callbacks: HashMap<String, F>,
    terminated: bool,
}

/// Convenience wrapper for building a [`Consumer`](struct.Consumer.html) with non-standard
/// options.
#[derive(Default, Clone)]
pub struct ConsumerBuilder(ClientOptions);

impl ConsumerBuilder {
    /// Set the hostname to use for this worker.
    ///
    /// Defaults to the machine's hostname as reported by the operating system.
    pub fn hostname(&mut self, hn: String) -> &mut Self {
        self.0.hostname = Some(hn);
        self
    }

    /// Set a unique identifier for this worker.
    ///
    /// Defaults to a randomly generated ASCII string.
    pub fn wid(&mut self, wid: String) -> &mut Self {
        self.0.wid = Some(wid);
        self
    }

    /// Set the labels to use for this worker.
    ///
    /// Defaults to `["rust"]`.
    pub fn labels(&mut self, labels: Vec<String>) -> &mut Self {
        self.0.labels = labels;
        self
    }

    /// Connect to a Faktory server using the standard environment variables.
    ///
    /// Will first read `FAKTORY_PROVIDER` to get the name of the environment variable to get the
    /// address from (defaults to `FAKTORY_URL`), and then read that environment variable to get
    /// the server address. If the latter environment variable is not defined, the url defaults to:
    ///
    /// ```text
    /// tcp://localhost:7419
    /// ```
    pub fn connect_env<C: StreamConnector, F>(
        self,
        connector: C,
    ) -> io::Result<Consumer<C::Stream, F>> {
        Ok(Consumer::from(Client::connect_env(connector, self.0)?))
    }

    /// Connect to a Faktory server at the given URL.
    ///
    /// The url is in standard URL form:
    ///
    /// ```text
    /// tcp://[:password@]hostname[:port]
    /// ```
    ///
    /// Port defaults to 7419 if not given.
    pub fn connect<C: StreamConnector, F, U: AsRef<str>>(
        self,
        connector: C,
        url: U,
    ) -> io::Result<Consumer<C::Stream, F>> {
        Ok(Consumer::from(
            Client::connect(connector, self.0, url.as_ref())?,
        ))
    }
}

enum Failed<E: Error> {
    Application(E),
    BadJobType(String),
}

impl<F, S: Read + Write> From<Client<S>> for Consumer<S, F> {
    fn from(c: Client<S>) -> Self {
        Consumer {
            c: Arc::new(Mutex::new(c)),
            callbacks: Default::default(),
            running_job: Arc::new(AtomicOption::empty()),
            last_job_result: Arc::new(AtomicOption::empty()),
            terminated: false,
        }
    }
}

impl<F, S: Read + Write + 'static> Consumer<S, F> {
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
    pub fn default<C: StreamConnector<Stream = S> + Default>() -> io::Result<Consumer<S, F>> {
        ConsumerBuilder::default().connect_env(C::default())
    }

    /// Re-establish this worker's connection to the Faktory server using default environment
    /// variables.
    pub fn reconnect_env<C: StreamConnector<Stream = S>>(
        &mut self,
        connector: C,
    ) -> io::Result<()> {
        self.c.lock().unwrap().reconnect_env(connector)
    }

    /// Re-establish this worker's connection to the Faktory server using the given `url`.
    pub fn reconnect<U: AsRef<str>, C: StreamConnector<Stream = S>>(
        &mut self,
        connector: C,
        url: U,
    ) -> io::Result<()> {
        self.c.lock().unwrap().reconnect(connector, url.as_ref())
    }
}

impl<S, E, F> Consumer<S, F>
where
    S: Read + Write + 'static + Send,
    E: Error,
    F: FnMut(Job) -> Result<(), E> + Send + 'static,
{
    /// Run this worker until the server tells us to exit or a connection cannot be re-established.
    ///
    /// This function never returns. When the worker decides to exit, the process is terminated.
    pub fn run_to_completion<Q, U, C>(mut self, queues: &[Q], connector: C, url: U) -> !
    where
        Q: AsRef<str>,
        U: AsRef<str>,
        C: StreamConnector<Stream = S> + Clone,
    {
        use std::process;
        let url = url.as_ref();
        while self.run(queues).is_err() {
            if self.reconnect(connector.clone(), url).is_err() {
                break;
            }
        }

        process::exit(0);
    }

    /// Run this worker until the server tells us to exit or a connection cannot be re-established.
    ///
    /// This function never returns. When the worker decides to exit, the process is terminated.
    pub fn run_to_completion_env<Q, C>(mut self, queues: &[Q], connector: C) -> !
    where
        Q: AsRef<str>,
        C: StreamConnector<Stream = S> + Clone,
    {
        use std::process;
        while self.run(queues).is_err() {
            if self.reconnect_env(connector.clone()).is_err() {
                break;
            }
        }

        process::exit(0);
    }

    fn for_worker(&mut self) -> Self {
        use std::mem;
        Consumer {
            c: self.c.clone(),
            callbacks: mem::replace(&mut self.callbacks, HashMap::default()),
            running_job: self.running_job.clone(),
            last_job_result: self.last_job_result.clone(),
            terminated: self.terminated,
        }
    }

    /// Register a handler function for the given `kind` of job.
    ///
    /// Whenever a job whose type matches `kind` is fetched from the Faktory, the given handler
    /// function is called with that job.
    pub fn register<K>(&mut self, kind: K, handler: F)
    where
        K: ToString,
    {
        self.callbacks.insert(kind.to_string(), handler);
    }

    fn run_job(&mut self, job: Job) -> Result<(), Failed<E>> {
        match self.callbacks.get_mut(&job.kind) {
            Some(callback) => (callback)(job).map_err(Failed::Application),
            None => {
                // cannot execute job, since no handler exists
                Err(Failed::BadJobType(job.kind))
            }
        }
    }

    fn run_one<Q>(&mut self, queues: &[Q]) -> io::Result<()>
    where
        Q: AsRef<str>,
    {
        // get a job
        let job = self.c.lock().unwrap().fetch(queues)?;

        // remember the job id
        let jid = job.jid.clone();

        // keep track of running job in case we're terminated during it
        self.running_job
            .swap(Box::new(jid.clone()), atomic::Ordering::SeqCst);

        // process the job
        let r = self.run_job(job);

        // report back
        match r {
            Ok(_) => {
                // job done -- acknowledge
                // remember it in case we fail to notify the server (e.g., broken connection)
                self.last_job_result
                    .swap(Box::new(Ok(jid.clone())), atomic::Ordering::SeqCst);
                self.c.lock().unwrap().issue(Ack::new(jid))?.await_ok()?;
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
                        let mut root = e.cause();
                        let mut backtrace = Vec::new();
                        while let Some(r) = root.take() {
                            backtrace.push(format!("{}", r));
                            root = r.cause();
                        }
                        f.set_backtrace(backtrace);
                        f
                    }
                };

                let fail2 = fail.clone();
                self.last_job_result
                    .swap(Box::new(Err(fail)), atomic::Ordering::SeqCst);
                self.c.lock().unwrap().issue(&fail2)?.await_ok()?;
            }
        }

        // we won't have to tell the server again
        self.last_job_result.take(atomic::Ordering::SeqCst);
        self.running_job.take(atomic::Ordering::SeqCst);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn run_n<Q>(&mut self, n: usize, queues: &[Q]) -> io::Result<()>
    where
        Q: AsRef<str>,
    {
        for _ in 0..n {
            self.run_one(queues)?;
        }
        Ok(())
    }

    /// Run this worker on the given `queues` until an I/O error occurs, or until the server tells
    /// the worker to disengage.
    ///
    /// The value in `Ok()` indicates the number of workers that may still be processing jobs.
    ///
    /// Note that if the worker fails, `reconnect()` should likely be called before calling `run()`
    /// again. If an error occurred while reporting a job success or failure, the result will be
    /// re-reported to the server without re-executing the job. If the worker was terminated (i.e.,
    /// `run` returns with an `Ok` response), the worker should **not** try to resume by calling
    /// `run` again. This will cause a panic.
    pub fn run<Q>(&mut self, queues: &[Q]) -> io::Result<usize>
    where
        Q: AsRef<str>,
    {
        assert!(self.terminated, "do not re-run a terminated worker");
        assert_eq!(Arc::strong_count(&self.last_job_result), 1);

        // retry delivering notification about our last job result.
        // we know there's no leftover thread at this point, so there's no race on the option.
        if let Some(res) = self.last_job_result.take(atomic::Ordering::SeqCst) {
            let mut c = self.c.lock().unwrap();
            let r = match *res {
                Ok(ref jid) => c.issue(Ack::new(jid)),
                Err(ref fail) => c.issue(fail),
            };

            let r = match r {
                Ok(r) => r,
                Err(e) => {
                    self.last_job_result.swap(res, atomic::Ordering::SeqCst);
                    return Err(e);
                }
            };

            if let Err(e) = r.await_ok() {
                // it could be that the server did previously get our ACK/FAIL, and that it was the
                // resulting OK that failed. in that case, we would get an error response when
                // re-sending the job response. this should not count as critical. other errors,
                // however, should!
                if e.kind() != io::ErrorKind::InvalidInput {
                    self.last_job_result.swap(res, atomic::Ordering::SeqCst);
                    return Err(e);
                }
            }
        }

        // keep track of the current status of this worker
        let status: Arc<atomic::AtomicUsize> = Default::default();

        // start a worker thread
        use std::thread;
        let mut w = self.for_worker();
        let w = {
            let status = status.clone();
            let queues: Vec<_> = queues.into_iter().map(|s| s.as_ref().to_string()).collect();
            thread::spawn(move || {
                while status.load(atomic::Ordering::SeqCst) == STATUS_RUNNING {
                    if let Err(e) = w.run_one(&queues[..]) {
                        status.store(STATUS_TERMINATING, atomic::Ordering::SeqCst);
                        return Err(e);
                    }
                }
                status.store(STATUS_TERMINATING, atomic::Ordering::SeqCst);
                Ok(())
            })
        };

        // listen for heartbeats
        let exit = loop {
            match self.c.lock().unwrap().heartbeat() {
                Ok(hb) => {
                    use std::thread;
                    use std::time;

                    match hb {
                        HeartbeatStatus::Ok => {}
                        HeartbeatStatus::Quiet => {
                            // tell the worker to eventually terminate
                            status.store(STATUS_QUIET, atomic::Ordering::SeqCst);
                        }
                        HeartbeatStatus::Terminate => {
                            // tell the worker to terminate
                            // *and* fail the current job and immediately return
                            status.store(STATUS_QUIET, atomic::Ordering::SeqCst);
                            break Ok(true);
                        }
                    }

                    thread::sleep(time::Duration::from_secs(5));

                    // has worker terminated?
                    if status.load(atomic::Ordering::SeqCst) == STATUS_TERMINATING {
                        break Ok(false);
                    }
                }
                Err(e) => {
                    // for this to fail, the worker must probably also have failed
                    status.store(STATUS_QUIET, atomic::Ordering::SeqCst);
                    break Err(e);
                }
            }
        };

        // there are a couple of cases here:
        //
        //  - we got TERMINATE, so we should just return, even if worker is still running
        //  - we got TERMINATE and worker has exited
        //  - we got an error from heartbeat()
        //
        self.terminated = exit.is_ok();
        match exit {
            Ok(forced) if forced => {
                // FAIL currently running job even though it's still running
                if let Some(jid) = self.running_job.take(atomic::Ordering::SeqCst) {
                    let f = Fail::new(jid, "unknown", "terminated");

                    // if this fails, we don't want to exit with Err(),
                    // because we *were* still terminated!
                    self.c
                        .lock()
                        .unwrap()
                        .issue(&f)
                        .and_then(|r| r.await_ok())
                        .is_ok();
                }
                self.c.lock().unwrap().end_early().is_ok();
                Ok(1)
            }
            Ok(_) => w.join().unwrap().map(|_| 0),
            Err(e) => w.join().unwrap().and_then(|_| Err(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proto::TcpEstablisher;

    #[test]
    #[ignore]
    fn it_works() {
        use std::io;
        use producer::Producer;

        let mut p = Producer::default::<TcpEstablisher>().unwrap();
        let mut j = Job::new("foobar", vec!["z"]);
        j.queue = "worker_test_1".to_string();
        p.enqueue(j).unwrap();

        let mut c = Consumer::default::<TcpEstablisher>().unwrap();
        c.register("foobar", |job| -> io::Result<()> {
            println!("{:?}", job);
            assert_eq!(job.args, vec!["z"]);
            Ok(())
        });
        let e = c.run_n(1, &["worker_test_1"]);
        if e.is_err() {
            println!("{:?}", e);
        }
        assert!(e.is_ok());
    }
}
