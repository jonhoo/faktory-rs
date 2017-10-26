use bufstream::BufStream;
use hostname::get_hostname;
use libc::getpid;
use std::io::prelude::*;
use std::io;
use serde;
use std::net::TcpStream;
use url::Url;

mod single;

// commands that users can issue
pub use self::single::{Ack, Fail, Heartbeat, Info, Job, Push};

// responses that users can see
pub use self::single::Hi;

#[derive(Clone)]
pub(crate) struct ClientOptions {
    /// Hostname to advertise to server.
    /// Defaults to machine hostname.
    pub(crate) hostname: Option<String>,

    /// PID to advertise to server.
    /// Defaults to process ID.
    pub(crate) pid: Option<usize>,

    /// Worker ID to advertise to server.
    /// Defaults to a GUID.
    pub(crate) wid: Option<String>,

    /// Labels to advertise to server.
    /// Defaults to ["rust"].
    pub(crate) labels: Vec<String>,
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

pub(crate) struct Client<S: Read + Write> {
    stream: BufStream<S>,
    wid: String,
}

impl<S: Read + Write> Client<S> {
    fn init(&mut self, opts: ClientOptions, pwd: Option<&str>) -> io::Result<()> {
        let hi = single::read_hi(&mut self.stream)?;
        let hostname = opts.hostname
            .or_else(|| get_hostname())
            .unwrap_or_else(|| "local".to_string());
        let pid = opts.pid.unwrap_or_else(|| unsafe { getpid() } as usize);
        let wid = opts.wid.unwrap_or_else(|| {
            use rand::{thread_rng, Rng};
            thread_rng().gen_ascii_chars().take(32).collect()
        });
        let mut hello = single::Hello::new(hostname, &wid, pid, &opts.labels[..]);
        self.wid = wid;
        if let Some(salt) = hi.salt {
            if let Some(pwd) = pwd {
                hello.set_password(&salt, &pwd);
            } else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "server requires authentication, but no password given",
                ));
            }
        }
        single::write_command_and_await_ok(&mut self.stream, hello)
    }

    fn new(stream: S, opts: ClientOptions, pwd: Option<&str>) -> io::Result<Client<S>> {
        let mut s = Client {
            stream: BufStream::new(stream),
            wid: "".to_string(), // set by init
        };
        s.init(opts, pwd)?;
        Ok(s)
    }
}

impl<S: Read + Write> Drop for Client<S> {
    fn drop(&mut self) {
        single::write_command(&mut self.stream, single::End).unwrap();
    }
}

/// A type that can be constructed from a Url connection string.
pub trait FromUrl {
    /// Construct a new `Self` from the given url.
    fn from_url(url: &Url) -> Self;
}

impl FromUrl for String {
    fn from_url(url: &Url) -> Self {
        format!("{}:{}", url.host_str().unwrap(), url.port().unwrap_or(7419))
    }
}

/// A stream that can be established using a url.
pub trait StreamConnector: Sized + Read + Write + 'static {
    /// The address used to connect this kind of stream.
    type Addr: FromUrl;

    /// Establish a new stream using the given `addr`.
    fn connect(addr: Self::Addr) -> io::Result<Self>;
}

impl StreamConnector for TcpStream {
    type Addr = String;
    fn connect(addr: Self::Addr) -> io::Result<Self> {
        TcpStream::connect(&addr)
    }
}

impl<C: StreamConnector> Client<C> {
    /// Connect to an unsecured Faktory server using the standard environment variables.
    ///
    /// Will first read `FAKTORY_PROVIDER` to get the name of the environment variable to get the
    /// address from (defaults to `FAKTORY_URL`), and then read that environment variable to get
    /// the server address. If the latter environment variable is not defined, the url defaults to:
    ///
    /// ```text
    /// tcp://localhost:7419
    /// ```
    pub fn connect_env(opts: ClientOptions) -> io::Result<Self> {
        use std::env;
        let var = env::var("FAKTORY_PROVIDER").unwrap_or_else(|_| "FAKTORY_URL".to_string());
        let url = env::var(var).unwrap_or_else(|_| "tcp://localhost:7419".to_string());
        Self::connect(opts, &url)
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
    pub fn connect(opts: ClientOptions, url: &str) -> io::Result<Self> {
        let url = Url::parse(url).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        if url.scheme() != "tcp" {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("unknown scheme '{}'", url.scheme()),
            ));
        }

        if url.host_str().is_none() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "no hostname given",
            ));
        }

        let addr = FromUrl::from_url(&url);
        let stream = C::connect(addr)?;
        Client::new(stream, opts, url.password())
    }
}

pub struct ReadToken<'a, S: Read + Write + 'a>(&'a mut Client<S>);

impl<S: Read + Write> Client<S> {
    pub fn issue<C: self::single::FaktoryCommand>(&mut self, c: C) -> io::Result<ReadToken<S>> {
        single::write_command(&mut self.stream, c)?;
        Ok(ReadToken(self))
    }

    pub fn heartbeat(&mut self) -> io::Result<()> {
        single::write_command(&mut self.stream, Heartbeat::new(&self.wid))?;
        single::read_ok(&mut self.stream)
    }

    pub fn fetch<Q>(&mut self, queues: &[Q]) -> io::Result<Job>
    where
        Q: AsRef<str>,
    {
        self.issue(single::Fetch::from(queues))?.read_json()
    }
}

impl<'a, S: Read + Write> ReadToken<'a, S> {
    pub fn await_ok(self) -> io::Result<()> {
        single::read_ok(&mut self.0.stream)
    }

    pub fn read_json<T>(self) -> io::Result<T>
    where
        T: serde::de::DeserializeOwned,
    {
        Ok(single::read_json(&mut self.0.stream)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        Client::<TcpStream>::connect_env(ClientOptions::default()).unwrap();
    }
}
