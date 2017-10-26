use bufstream::BufStream;
use hostname::get_hostname;
use libc::getpid;
use std::io::prelude::*;
use std::io;
use serde;
use std::net::{TcpStream, ToSocketAddrs};

mod single;

// commands that users can issue
pub use self::single::{Ack, Fail, Info, Job, Push};

// responses that users can see
pub use self::single::Hi;

pub struct ClientOptions {
    /// Hostname to advertise to server.
    /// Defaults to machine hostname.
    hostname: Option<String>,

    /// PID to advertise to server.
    /// Defaults to process ID.
    pid: Option<usize>,

    /// Worker ID to advertise to server.
    /// Defaults to a GUID.
    wid: Option<String>,

    /// Labels to advertise to server.
    /// Defaults to ["rust"].
    labels: Vec<String>,
}

impl Default for ClientOptions {
    fn default() -> Self {
        ClientOptions {
            hostname: None,
            pid: None,
            wid: None,
            labels: vec!["rust".to_string()],
        }
    }
}

pub struct Client<S: Read + Write> {
    stream: BufStream<S>,
}

impl<S: Read + Write> Client<S> {
    fn init(&mut self, opts: ClientOptions, password: Option<&str>) -> io::Result<()> {
        let hi = single::read_hi(&mut self.stream)?;
        let hostname = opts.hostname
            .or_else(|| get_hostname())
            .unwrap_or_else(|| "local".to_string());
        let pid = opts.pid.unwrap_or_else(|| unsafe { getpid() } as usize);
        let wid = opts.wid.unwrap_or_else(|| {
            use rand::{thread_rng, Rng};
            thread_rng().gen_ascii_chars().take(32).collect()
        });
        let mut hello = single::Hello::new(hostname, wid, pid, &opts.labels[..]);
        if let Some(salt) = hi.salt {
            if let Some(pwd) = password {
                hello.set_password(&salt, pwd);
            } else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "server requires authentication, but no password given",
                ));
            }
        }
        single::write_command_and_await_ok(&mut self.stream, hello)
    }

    fn new(stream: S, opts: ClientOptions, password: Option<&str>) -> io::Result<Client<S>> {
        let mut s = Client {
            stream: BufStream::new(stream),
        };
        s.init(opts, password)?;
        Ok(s)
    }
}

impl<S: Read + Write> Drop for Client<S> {
    fn drop(&mut self) {
        single::write_command(&mut self.stream, single::End).unwrap();
    }
}

impl Client<TcpStream> {
    /// Connect to an unsecured Faktory server.
    pub fn connect<A: ToSocketAddrs>(addr: A) -> io::Result<Client<TcpStream>> {
        let stream = TcpStream::connect(addr)?;
        Client::new(stream, ClientOptions::default(), None)
    }
}

impl<S: Read + Write> Client<S> {
    pub fn issue<C: self::single::FaktoryCommand>(&mut self, c: C) -> io::Result<()> {
        single::write_command(&mut self.stream, c)
    }

    pub fn fetch<Q>(&mut self, queues: &[Q]) -> io::Result<Job>
    where
        Q: AsRef<str>,
    {
        self::single::write_command(&mut self.stream, single::Fetch::from(queues))?;
        self.read_json()
    }

    pub fn await_ok(&mut self) -> io::Result<()> {
        single::read_ok(&mut self.stream)
    }

    pub fn read_json<T>(&mut self) -> io::Result<T>
    where
        T: serde::de::DeserializeOwned,
    {
        Ok(single::read_json(&mut self.stream)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        Client::connect(("127.0.0.1", 7419)).unwrap();
    }
}
