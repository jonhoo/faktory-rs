use crate::proto::{self, Reconnect};
use failure::Error;
use native_tls::TlsConnector;
use native_tls::TlsStream as NativeTlsStream;
use std::io;
use std::io::prelude::*;
use std::net::TcpStream;

/// A reconnectable stream encrypted with TLS.
///
/// This can be used as an argument to `Consumer::connect_with` and `Producer::connect_with` to
/// connect to a TLS-secured Faktory server.
///
/// # Examples
///
/// ```no_run
/// use faktory::{Producer, TlsStream};
/// let tls = TlsStream::connect(None).unwrap();
/// let p = Producer::connect_with(tls, None).unwrap();
/// # drop(p);
/// ```
///
pub struct TlsStream<S> {
    connector: TlsConnector,
    hostname: String,
    stream: NativeTlsStream<S>,
}

impl TlsStream<TcpStream> {
    /// Create a new TLS connection over TCP.
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
    pub fn connect(url: Option<&str>) -> Result<Self, Error> {
        TlsStream::with_connector(TlsConnector::builder().build()?, url)
    }

    /// Create a new TLS connection over TCP using a non-default TLS configuration.
    ///
    /// See `connect` for details about the `url` parameter.
    pub fn with_connector(tls: TlsConnector, url: Option<&str>) -> Result<Self, Error> {
        let url = match url {
            Some(url) => proto::url_parse(url),
            None => proto::url_parse(&proto::get_env_url()),
        }?;
        let stream = TcpStream::connect(proto::host_from_url(&url))?;
        Ok(TlsStream::new(stream, tls, url.host_str().unwrap())?)
    }
}

use std::fmt::Debug;
impl<S> TlsStream<S>
where
    S: Read + Write + Reconnect + Send + Sync + Debug + 'static,
{
    /// Create a new TLS connection on an existing stream.
    pub fn default(stream: S, hostname: &str) -> io::Result<Self> {
        Self::new(stream, TlsConnector::builder().build().unwrap(), hostname)
    }

    /// Create a new TLS connection on an existing stream with a non-default TLS configuration.
    pub fn new(stream: S, tls: TlsConnector, hostname: &str) -> io::Result<Self> {
        let stream = tls
            .clone()
            .connect(hostname, stream)
            .map_err(|e| io::Error::new(io::ErrorKind::ConnectionAborted, e))?;

        Ok(TlsStream {
            connector: tls,
            hostname: hostname.to_string(),
            stream: stream,
        })
    }
}

impl<S> Reconnect for TlsStream<S>
where
    S: Read + Write + Reconnect + Send + Sync + Debug + 'static,
{
    fn reconnect(&self) -> io::Result<Self> {
        Self::new(
            self.stream.get_ref().reconnect()?,
            self.connector.clone(),
            &self.hostname,
        )
    }
}

use std::ops::{Deref, DerefMut};
impl<S> Deref for TlsStream<S> {
    type Target = NativeTlsStream<S>;
    fn deref(&self) -> &Self::Target {
        &self.stream
    }
}
impl<S> DerefMut for TlsStream<S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.stream
    }
}

impl<S: Read + Write> Read for TlsStream<S> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.stream.read(buf)
    }
}

impl<S: Read + Write> Write for TlsStream<S> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.stream.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.stream.flush()
    }
}
