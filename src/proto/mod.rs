use crate::error::{self, Error};
use bufstream::BufStream;
use libc::getpid;
use std::io;
use std::io::prelude::*;
use std::net::TcpStream;
use url::Url;

pub(crate) const EXPECTED_PROTOCOL_VERSION: usize = 2;

mod single;

// commands that users can issue
pub use self::single::{
    Ack, Fail, Heartbeat, Info, Job, JobBuilder, Push, QueueAction, QueueControl,
};

// responses that users can see
pub use self::single::Hi;

pub(crate) fn get_env_url() -> String {
    use std::env;
    let var = env::var("FAKTORY_PROVIDER").unwrap_or_else(|_| "FAKTORY_URL".to_string());
    env::var(var).unwrap_or_else(|_| "tcp://localhost:7419".to_string())
}

pub(crate) fn host_from_url(url: &Url) -> String {
    format!("{}:{}", url.host_str().unwrap(), url.port().unwrap_or(7419))
}

pub(crate) fn url_parse(url: &str) -> Result<Url, Error> {
    let url = Url::parse(url).map_err(error::Connect::ParseUrl)?;
    if url.scheme() != "tcp" {
        return Err(error::Connect::BadScheme {
            scheme: url.scheme().to_string(),
        }
        .into());
    }

    if url.host_str().is_none() || url.host_str().unwrap().is_empty() {
        return Err(error::Connect::MissingHostname.into());
    }

    Ok(url)
}

/// A stream that can be re-established after failing.
pub trait Reconnect: Sized {
    /// Re-establish the stream.
    fn reconnect(&self) -> io::Result<Self>;
}

impl Reconnect for TcpStream {
    fn reconnect(&self) -> io::Result<Self> {
        TcpStream::connect(self.peer_addr().unwrap())
    }
}

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

    /// Password to authenticate with
    /// Defaults to None,
    pub(crate) password: Option<String>,

    is_producer: bool,
}

impl Default for ClientOptions {
    fn default() -> Self {
        ClientOptions {
            hostname: None,
            pid: None,
            wid: None,
            labels: vec!["rust".to_string()],
            password: None,
            is_producer: false,
        }
    }
}

pub(crate) struct Client<S: Read + Write> {
    stream: BufStream<S>,
    opts: ClientOptions,
}

impl<S> Client<S>
where
    S: Read + Write + Reconnect,
{
    pub(crate) fn connect_again(&self) -> Result<Self, Error> {
        let s = self.stream.get_ref().reconnect()?;
        Client::new(s, self.opts.clone())
    }

    pub fn reconnect(&mut self) -> Result<(), Error> {
        let s = self.stream.get_ref().reconnect()?;
        self.stream = BufStream::new(s);
        self.init()
    }
}

impl<S: Read + Write> Client<S> {
    pub(crate) fn new(stream: S, opts: ClientOptions) -> Result<Client<S>, Error> {
        let mut c = Client {
            stream: BufStream::new(stream),
            opts,
        };
        c.init()?;
        Ok(c)
    }

    pub(crate) fn new_producer(stream: S, pwd: Option<String>) -> Result<Client<S>, Error> {
        let opts = ClientOptions {
            password: pwd,
            is_producer: true,
            ..Default::default()
        };
        Self::new(stream, opts)
    }
}

impl<S: Read + Write> Client<S> {
    fn init(&mut self) -> Result<(), Error> {
        let hi = single::read_hi(&mut self.stream)?;

        if hi.version != EXPECTED_PROTOCOL_VERSION {
            return Err(error::Connect::VersionMismatch {
                ours: EXPECTED_PROTOCOL_VERSION,
                theirs: hi.version,
            }
            .into());
        }

        // fill in any missing options, and remember them for re-connect
        let mut hello = single::Hello::default();
        if !self.opts.is_producer {
            let hostname = self
                .opts
                .hostname
                .clone()
                .or_else(|| hostname::get().ok()?.into_string().ok())
                .unwrap_or_else(|| "local".to_string());
            self.opts.hostname = Some(hostname);
            let pid = self
                .opts
                .pid
                .unwrap_or_else(|| unsafe { getpid() } as usize);
            self.opts.pid = Some(pid);
            let wid = self.opts.wid.clone().unwrap_or_else(|| {
                use rand::{thread_rng, Rng};
                thread_rng()
                    .sample_iter(&rand::distributions::Alphanumeric)
                    .map(char::from)
                    .take(32)
                    .collect()
            });
            self.opts.wid = Some(wid);

            hello.hostname = Some(self.opts.hostname.clone().unwrap());
            hello.wid = Some(self.opts.wid.clone().unwrap());
            hello.pid = Some(self.opts.pid.unwrap());
            hello.labels = self.opts.labels.clone();
        }

        if hi.salt.is_some() {
            if let Some(ref pwd) = self.opts.password {
                hello.set_password(&hi, pwd);
            } else {
                return Err(error::Connect::AuthenticationNeeded.into());
            }
        }

        single::write_command_and_await_ok(&mut self.stream, &hello)
    }
}

impl<S: Read + Write> Drop for Client<S> {
    fn drop(&mut self) {
        single::write_command(&mut self.stream, &single::End).unwrap();
    }
}

pub struct ReadToken<'a, S: Read + Write>(&'a mut Client<S>);

pub(crate) enum HeartbeatStatus {
    Ok,
    Terminate,
    Quiet,
}

impl<S: Read + Write> Client<S> {
    pub(crate) fn issue<FC: self::single::FaktoryCommand>(
        &mut self,
        c: &FC,
    ) -> Result<ReadToken<'_, S>, Error> {
        single::write_command(&mut self.stream, c)?;
        Ok(ReadToken(self))
    }

    pub(crate) fn heartbeat(&mut self) -> Result<HeartbeatStatus, Error> {
        single::write_command(
            &mut self.stream,
            &Heartbeat::new(&**self.opts.wid.as_ref().unwrap()),
        )?;

        match single::read_json::<_, serde_json::Value>(&mut self.stream)? {
            None => Ok(HeartbeatStatus::Ok),
            Some(s) => match s
                .as_object()
                .and_then(|m| m.get("state"))
                .and_then(|s| s.as_str())
            {
                Some("terminate") => Ok(HeartbeatStatus::Terminate),
                Some("quiet") => Ok(HeartbeatStatus::Quiet),
                _ => Err(error::Protocol::BadType {
                    expected: "heartbeat response",
                    received: format!("{}", s),
                }
                .into()),
            },
        }
    }

    pub(crate) fn fetch<Q>(&mut self, queues: &[Q]) -> Result<Option<Job>, Error>
    where
        Q: AsRef<str>,
    {
        self.issue(&single::Fetch::from(queues))?.read_json()
    }
}

impl<'a, S: Read + Write> ReadToken<'a, S> {
    pub(crate) fn await_ok(self) -> Result<(), Error> {
        single::read_ok(&mut self.0.stream)
    }

    pub(crate) fn read_json<T>(self) -> Result<Option<T>, Error>
    where
        T: serde::de::DeserializeOwned,
    {
        single::read_json(&mut self.0.stream)
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
        Client::new(
            TcpStream::connect("localhost:7419").unwrap(),
            ClientOptions::default(),
        )
        .unwrap();
    }

    #[test]
    fn correct_env_parsing() {
        use std::env;

        if env::var_os("FAKTORY_URL").is_some() {
            eprintln!("skipping test to avoid messing with user-set FAKTORY_URL");
            return;
        }

        assert_eq!(get_env_url(), "tcp://localhost:7419");

        env::set_var("FAKTORY_URL", "tcp://example.com:7500");
        assert_eq!(get_env_url(), "tcp://example.com:7500");

        env::set_var("FAKTORY_PROVIDER", "URL");
        env::set_var("URL", "tcp://example.com:7501");
        assert_eq!(get_env_url(), "tcp://example.com:7501");
    }

    #[test]
    fn url_port_default() {
        use url::Url;
        let url = Url::parse("tcp://example.com").unwrap();
        assert_eq!(host_from_url(&url), "example.com:7419");
    }

    #[test]
    fn url_requires_tcp() {
        url_parse("foobar").unwrap_err();
    }

    #[test]
    fn url_requires_host() {
        url_parse("tcp://:7419").unwrap_err();
    }

    #[test]
    fn url_doesnt_require_port() {
        url_parse("tcp://example.com").unwrap();
    }

    #[test]
    fn url_can_take_password_and_port() {
        url_parse("tcp://:foobar@example.com:7419").unwrap();
    }
}
