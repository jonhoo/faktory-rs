use std::io::prelude::*;
use std::io;
use std::error::Error;
use proto::{Client, ClientOptions, StreamConnector};
use std::collections::HashMap;
use std::sync::{atomic, Arc, Mutex};

use proto::{Ack, Fail, Job};

/// This struct represents a single Faktory worker.
///
/// The worker consumes jobs fetched from the Faktory server, processes them, and reports the
/// results back to the Faktory server upon completion.
///
/// A worker should be constructed using a [`ConsumerBuilder`](struct.ConsumerBuilder.html), so
/// that any non-default worker parameters can be set.
///
/// ```no_run
/// # use faktory::ConsumerBuilder;
/// use std::io;
/// use std::net::TcpStream;
/// let mut c = ConsumerBuilder::default().connect_env::<TcpStream, _>().unwrap();
/// c.register("foobar", |job| -> io::Result<()> {
///     println!("{:?}", job);
///     Ok(())
/// });
/// let e = c.run(&["default"]);
/// println!("worker failed: {}", e);
/// ```
pub struct Consumer<S, F>
where
    S: Read + Write,
{
    c: Arc<Mutex<Client<S>>>,
    callbacks: HashMap<String, F>,
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

    /// Connect to an unsecured Faktory server using the standard environment variables.
    ///
    /// Will first read `FAKTORY_PROVIDER` to get the name of the environment variable to get the
    /// address from (defaults to `FAKTORY_URL`), and then read that environment variable to get
    /// the server address. If the latter environment variable is not defined, the url defaults to:
    ///
    /// ```text
    /// tcp://localhost:7419
    /// ```
    pub fn connect_env<S: StreamConnector, F>(self) -> io::Result<Consumer<S, F>> {
        Ok(Consumer::from(Client::connect_env(self.0)?))
    }

    /// Connect to an unsecured Faktory server.
    ///
    /// The url is in standard URL form:
    ///
    /// ```text
    /// tcp://[:password@]hostname[:port]
    /// ```
    ///
    /// Port defaults to 7419 if not given.
    pub fn connect<S: StreamConnector, F, U: AsRef<str>>(
        self,
        url: U,
    ) -> io::Result<Consumer<S, F>> {
        Ok(Consumer::from(Client::connect(self.0, url.as_ref())?))
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
        }
    }
}

impl<F, S: StreamConnector> Consumer<S, F> {
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
    pub fn default() -> io::Result<Self> {
        ConsumerBuilder::default().connect_env()
    }

    /// Re-establish this worker's connection to the Faktory server using default environment
    /// variables.
    pub fn reconnect_env(&mut self) -> io::Result<()> {
        self.c.lock().unwrap().reconnect_env()
    }

    /// Re-establish this worker's connection to the Faktory server using the given `url`.
    pub fn reconnect<U: AsRef<str>>(&mut self, url: U) -> io::Result<()> {
        self.c.lock().unwrap().reconnect(url.as_ref())
    }
}

impl<S, E, F> Consumer<S, F>
where
    S: Read + Write + Send + 'static,
    E: Error,
    F: FnMut(Job) -> Result<(), E>,
{
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
        let job = self.c.lock().unwrap().fetch(queues)?;
        let jid = job.jid.clone();
        let r = self.run_job(job);
        match r {
            Ok(_) => {
                // job done -- acknowledge
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
                self.c.lock().unwrap().issue(fail)?.await_ok()?;
            }
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn run_n<Q>(mut self, n: usize, queues: &[Q]) -> io::Result<()>
    where
        Q: AsRef<str>,
    {
        for _ in 0..n {
            self.run_one(queues)?;
        }
        Ok(())
    }

    /// Run this worker on the given `queues` until an I/O error occurs.
    pub fn run<Q>(mut self, queues: &[Q]) -> io::Error
    where
        Q: AsRef<str>,
    {
        // start heartbeat thread
        use std::thread;
        use std::time;
        let c = self.c.clone();
        let kill: Arc<atomic::AtomicBool> = Default::default();
        let killed = kill.clone();
        let hbt = thread::spawn(move || while let Ok(_) = c.lock().unwrap().heartbeat() {
            if killed.load(atomic::Ordering::SeqCst) {
                break;
            }
            thread::sleep(time::Duration::from_secs(5));
        });

        loop {
            if let Err(e) = self.run_one(queues) {
                kill.store(true, atomic::Ordering::SeqCst);
                hbt.join().unwrap();
                return e;
            }
            // TODO: remember the current job if we didn't get to ack/fail it!
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        use std::net::TcpStream;

        use std::io;
        use producer::Producer;

        let mut p = Producer::<TcpStream>::connect_env().unwrap();
        let mut j = Job::new("foobar", vec!["z"]);
        j.queue = "worker_test_1".to_string();
        p.enqueue(j).unwrap();

        let mut c = Consumer::<TcpStream, _>::default().unwrap();
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
