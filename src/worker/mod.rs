use std::io::prelude::*;
use std::io;
use std::net::{TcpStream, ToSocketAddrs};
use std::error::Error;
use proto::Client;
use std::collections::HashMap;

pub use proto::{Ack, Fail, Job};

pub struct Consumer<S, E, F>
where
    S: Read + Write,
    E: Error,
    F: FnMut(Job) -> Result<(), E>,
{
    c: Client<S>,
    callbacks: HashMap<String, F>,
}

impl<E, F> Consumer<TcpStream, E, F>
where
    E: Error,
    F: FnMut(Job) -> Result<(), E>,
{
    /// Connect to an unsecured Faktory server.
    pub fn connect<A: ToSocketAddrs>(addr: A) -> io::Result<Self> {
        Ok(Consumer {
            c: Client::connect(addr)?,
            callbacks: Default::default(),
        })
    }
}

enum Failed<E: Error> {
    Application(E),
    BadJobType(String),
}

impl<S, E, F> Consumer<S, E, F>
where
    S: Read + Write,
    E: Error,
    F: FnMut(Job) -> Result<(), E>,
{
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

    pub fn run_one<Q>(&mut self, queues: &[Q]) -> io::Result<()>
    where
        Q: AsRef<str>,
    {
        let job = self.c.fetch(queues)?;
        let jid = job.jid.clone();
        match self.run_job(job) {
            Ok(_) => {
                // job done -- acknowledge
                self.c.issue(Ack::new(jid))?;
                self.c.await_ok()?;
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
                self.c.issue(fail)?;
                self.c.await_ok()?;
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

    pub fn run<Q>(mut self, queues: &[Q]) -> io::Error
    where
        Q: AsRef<str>,
    {
        loop {
            if let Err(e) = self.run_one(queues) {
                return e;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        use std::io;
        use client::Producer;

        let mut p = Producer::connect(("127.0.0.1", 7419)).unwrap();
        let mut j = Job::new("foobar", &["z"]);
        j.queue = "worker_test_1".to_string();
        p.issue(j).unwrap();

        let mut c = Consumer::connect(("127.0.0.1", 7419)).unwrap();
        c.register("foobar", |job| -> io::Result<()> {
            println!("{:?}", job);
            assert_eq!(job.args, vec!["z"]);
            Ok(())
        });
        let e = c.run_n(1, &["worker_test_1"]);
        assert!(e.is_ok());
    }
}
