use std::io::prelude::*;
use std::io;
use std::net::{TcpStream, ToSocketAddrs};
use proto::{Client, Info, Push};
use serde_json;

pub use proto::Job;

pub struct Producer<S: Read + Write> {
    c: Client<S>,
}

impl Producer<TcpStream> {
    /// Connect to an unsecured Faktory server.
    pub fn connect<A: ToSocketAddrs>(addr: A) -> io::Result<Producer<TcpStream>> {
        Ok(Producer {
            c: Client::connect(addr)?,
        })
    }
}

impl<S: Read + Write> Producer<S> {
    pub fn issue(&mut self, job: Job) -> io::Result<()> {
        self.c.issue(Push::from(job))?;
        self.c.await_ok()
    }

    pub fn info(&mut self) -> io::Result<serde_json::Value> {
        self.c.issue(Info).map_err(serde_json::Error::io)?;
        self.c.read_json()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let mut p = Producer::connect(("127.0.0.1", 7419)).unwrap();
        p.issue(Job::new("foobar", vec!["z"])).unwrap();
    }
}
