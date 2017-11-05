use bufstream::BufStream;
use hostname::get_hostname;
use libc::getpid;
use std::io::prelude::*;
use std::io;
use serde;
use std::net::TcpStream;
use url::Url;
use native_tls::{TlsConnector, TlsConnectorBuilder, TlsStream};

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
    opts: ClientOptions,
}

impl<S: Read + Write> Client<S> {
    fn init(&mut self, pwd: Option<&str>) -> io::Result<()> {
        let hi = single::read_hi(&mut self.stream)?;

        // fill in any missing options, and remember them for re-connect
        let hostname = self.opts
            .hostname
            .clone()
            .or_else(|| get_hostname())
            .unwrap_or_else(|| "local".to_string());
        self.opts.hostname = Some(hostname);
        let pid = self.opts
            .pid
            .unwrap_or_else(|| unsafe { getpid() } as usize);
        self.opts.pid = Some(pid);
        let wid = self.opts.wid.clone().unwrap_or_else(|| {
            use rand::{thread_rng, Rng};
            thread_rng().gen_ascii_chars().take(32).collect()
        });
        self.opts.wid = Some(wid);

        let mut hello = single::Hello::new(
            self.opts.hostname.as_ref().unwrap(),
            self.opts.wid.as_ref().unwrap(),
            self.opts.pid.unwrap(),
            &self.opts.labels[..],
        );

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

    fn new(stream: S, opts: ClientOptions) -> Client<S> {
        Client {
            stream: BufStream::new(stream),
            opts: opts,
        }
    }
}

impl<S: Read + Write> Drop for Client<S> {
    fn drop(&mut self) {
        single::write_command(&mut self.stream, single::End).unwrap();
    }
}

/// A type that can be constructed from a `Url` connection string.
pub trait FromUrl {
    /// Construct a new `Self` from the given url.
    fn from_url(url: &Url) -> Self;
}

impl FromUrl for Url {
    fn from_url(url: &Url) -> Self {
        // ugh
        url.clone()
    }
}

impl FromUrl for String {
    fn from_url(url: &Url) -> Self {
        format!("{}:{}", url.host_str().unwrap(), url.port().unwrap_or(7419))
    }
}

/// A stream that can be established using a `Url`.
pub trait StreamConnector {
    /// The address used to connect this kind of stream.
    type Addr: FromUrl;

    /// The stream produced by this connector.
    type Stream: Sized + Read + Write + 'static;

    /// Establish a new stream using the given `addr`.
    fn connect(self, addr: Self::Addr) -> io::Result<Self::Stream>;
}

/// A regular TCP stream.
///
/// Will connect to the hostname:port pair given in the URL.
#[derive(Default)]
pub struct TcpEstablisher;

impl StreamConnector for TcpEstablisher {
    type Addr = String;
    type Stream = TcpStream;
    fn connect(self, addr: Self::Addr) -> io::Result<Self::Stream> {
        TcpStream::connect(&addr)
    }
}

/// A stream encrypted with TLS.
///
/// Will connect using the underlying stream connector `B`, and establish a TLS session on top of
/// that. You generally want to use [`TcpEstablisher`](struct.TcpEstablisher.html) as `B`. The
/// remote side must present a certificate that is valid for the hostname used in the URL. To set
/// custom options, construct a `native_tls::TlsConnectorBuilder`, configure it appropriately, and
/// then use
///
/// ```rust,no_run
/// # extern crate native_tls;
/// # extern crate faktory;
/// # fn main() {
/// # use faktory::{TlsEstablisher, TcpEstablisher};
/// # let tls = native_tls::TlsConnector::builder().unwrap();
/// TlsEstablisher::<TcpEstablisher>::from(tls);
/// # }
/// ```
pub struct TlsEstablisher<B = TcpEstablisher>
where
    B: StreamConnector,
{
    builder: TlsConnectorBuilder,
    base: B,
}

impl<B: Default + StreamConnector> Default for TlsEstablisher<B> {
    fn default() -> Self {
        TlsEstablisher {
            builder: TlsConnector::builder().unwrap(),
            base: B::default(),
        }
    }
}

impl<B: Default + StreamConnector> From<TlsConnectorBuilder> for TlsEstablisher<B> {
    fn from(o: TlsConnectorBuilder) -> Self {
        TlsEstablisher {
            builder: o,
            base: B::default(),
        }
    }
}

use std::fmt::Debug;
impl<B> StreamConnector for TlsEstablisher<B>
where
    B: StreamConnector,
    B::Stream: Debug + Send + Sync,
{
    type Addr = Url;
    type Stream = TlsStream<B::Stream>;
    fn connect(self, url: Self::Addr) -> io::Result<Self::Stream> {
        let TlsEstablisher { builder, base } = self;
        let stream = base.connect(B::Addr::from_url(&url))?;

        builder
            .build()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))
            .and_then(|b| {
                b.connect(url.host_str().unwrap(), stream)
                    .map_err(|e| io::Error::new(io::ErrorKind::ConnectionAborted, e))
            })
    }
}

fn get_env_url() -> String {
    use std::env;
    let var = env::var("FAKTORY_PROVIDER").unwrap_or_else(|_| "FAKTORY_URL".to_string());
    env::var(var).unwrap_or_else(|_| "tcp://localhost:7419".to_string())
}

fn url_parse(url: &str) -> io::Result<Url> {
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

    Ok(url)
}

fn stream_from_url<C: StreamConnector>(connector: C, url: &Url) -> io::Result<C::Stream> {
    let addr = FromUrl::from_url(url);
    connector.connect(addr)
}

impl<S: Read + Write + Sized + 'static> Client<S> {
    pub(crate) fn connect<C: StreamConnector<Stream = S>>(
        connector: C,
        opts: ClientOptions,
        url: &str,
    ) -> io::Result<Client<S>> {
        let url = url_parse(url)?;
        let stream = stream_from_url(connector, &url)?;
        let mut c = Client::new(stream, opts);
        c.init(url.password())?;
        Ok(c)
    }

    pub(crate) fn connect_env<C: StreamConnector<Stream = S>>(
        connector: C,
        opts: ClientOptions,
    ) -> io::Result<Client<S>> {
        Self::connect(connector, opts, &get_env_url())
    }

    pub fn reconnect_env<C>(&mut self, connector: C) -> io::Result<()>
    where
        C: StreamConnector<Stream = S>,
    {
        self.reconnect(connector, &get_env_url())
    }

    pub fn reconnect<C>(&mut self, connector: C, url: &str) -> io::Result<()>
    where
        C: StreamConnector<Stream = S>,
    {
        let url = url_parse(url)?;
        self.stream = BufStream::new(stream_from_url(connector, &url)?);
        self.init(url.password())
    }
}

pub struct ReadToken<'a, S: Read + Write + 'a>(&'a mut Client<S>);

pub(crate) enum HeartbeatStatus {
    Ok,
    Terminate,
    Quiet,
}

impl<S: Read + Write> Client<S> {
    pub(crate) fn end_early(&mut self) -> io::Result<()> {
        // TODO: also shutdown socket
        single::write_command(&mut self.stream, single::End)
    }

    pub(crate) fn issue<C: self::single::FaktoryCommand>(
        &mut self,
        c: C,
    ) -> io::Result<ReadToken<S>> {
        single::write_command(&mut self.stream, c)?;
        Ok(ReadToken(self))
    }

    pub(crate) fn heartbeat(&mut self) -> io::Result<HeartbeatStatus> {
        single::write_command(
            &mut self.stream,
            Heartbeat::new(self.opts.wid.as_ref().unwrap()),
        )?;

        let v = single::read_str(&mut self.stream)?;
        match &*v {
            "OK" => Ok(HeartbeatStatus::Ok),
            "terminate" => Ok(HeartbeatStatus::Terminate),
            "quiet" => Ok(HeartbeatStatus::Quiet),
            s => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("got unexpected heartbeat response '{}'", s),
            )),
        }
    }

    pub(crate) fn fetch<Q>(&mut self, queues: &[Q]) -> io::Result<Option<Job>>
    where
        Q: AsRef<str>,
    {
        self.issue(single::Fetch::from(queues))?.read_json()
    }
}

impl<'a, S: Read + Write> ReadToken<'a, S> {
    pub(crate) fn await_ok(self) -> io::Result<()> {
        single::read_ok(&mut self.0.stream)
    }

    pub(crate) fn read_json<T>(self) -> io::Result<Option<T>>
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
    #[ignore]
    fn it_works() {
        Client::connect_env(TcpEstablisher::default(), ClientOptions::default()).unwrap();
    }
}
