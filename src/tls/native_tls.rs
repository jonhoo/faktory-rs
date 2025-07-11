#[cfg(doc)]
use crate::{Client, WorkerBuilder};

use crate::error::{self, Error};
use crate::proto::{self, utils};
use crate::Reconnect;
use std::io;
use std::ops::{Deref, DerefMut};
use tokio::io::{AsyncRead, AsyncWrite, BufStream};
use tokio::net::TcpStream as TokioTcpStream;
use tokio_native_tls::TlsStream as NativeTlsStream;
use tokio_native_tls::{native_tls::TlsConnector, TlsConnector as AsyncTlsConnector};

/// A reconnectable stream encrypted with TLS.
///
/// This can be used as an argument to [`WorkerBuilder::connect_with`] and [`Client::connect_with`] to
/// connect to a TLS-secured Faktory server.
///
/// # Examples
///
/// ```no_run
/// # tokio_test::block_on(async {
/// use faktory::Client;
/// use faktory::native_tls::TlsStream;
/// use tokio::io::BufStream;
///
/// let stream = TlsStream::connect().await.unwrap();
/// let buffered = BufStream::new(stream);
/// let cl = Client::connect_with(buffered, None).await.unwrap();
/// # drop(cl);
/// # });
/// ```
///
#[pin_project::pin_project]
pub struct TlsStream<S> {
    connector: AsyncTlsConnector,
    hostname: String,
    #[pin]
    stream: NativeTlsStream<S>,
}

impl TlsStream<TokioTcpStream> {
    /// Create a new TLS connection to Faktory over TCP.
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
    pub async fn connect() -> Result<Self, Error> {
        TlsStream::with_connector(
            TlsConnector::builder()
                .build()
                .map_err(error::Stream::NativeTls)?,
            None,
        )
        .await
    }

    /// Create a new TLS connection to Faktory over TCP using specified address.
    pub async fn connect_to(addr: &str) -> Result<Self, Error> {
        TlsStream::with_connector(
            TlsConnector::builder()
                .build()
                .map_err(error::Stream::NativeTls)?,
            Some(addr),
        )
        .await
    }

    /// Create a new TLS connection over TCP using a non-default TLS configuration.
    ///
    /// See `connect` for details about the `url` parameter.
    pub async fn with_connector(connector: TlsConnector, url: Option<&str>) -> Result<Self, Error> {
        let url = match url {
            Some(url) => utils::url_parse(url),
            None => utils::url_parse(&utils::get_env_url()),
        }?;
        let hostname = utils::host_from_url(&url);
        let tcp_stream = TokioTcpStream::connect(&hostname).await?;
        Ok(TlsStream::new(tcp_stream, connector, hostname).await?)
    }
}

impl<S> TlsStream<S>
where
    S: AsyncRead + AsyncWrite + Send + Unpin,
{
    /// Create a new TLS connection on an existing stream.
    ///
    /// Internally, creates a [`TlsConnector`]
    /// with default settings.
    ///
    /// Use [`new`](TlsStream::new) for a customized connector.
    pub async fn default(stream: S, hostname: String) -> io::Result<Self> {
        let connector = TlsConnector::builder()
            .build()
            .map_err(error::Stream::NativeTls)
            .unwrap();
        Self::new(stream, connector, hostname).await
    }

    /// Create a new TLS connection on an existing stream with a non-default TLS configuration.
    pub async fn new(
        stream: S,
        connector: impl Into<AsyncTlsConnector>,
        hostname: String,
    ) -> io::Result<Self> {
        let connector: AsyncTlsConnector = connector.into();
        let tls_stream = connector
            .connect(&hostname, stream)
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::ConnectionAborted, e))?;
        Ok(TlsStream {
            connector,
            hostname,
            stream: tls_stream,
        })
    }
}

#[async_trait::async_trait]
impl Reconnect for BufStream<TlsStream<tokio::net::TcpStream>> {
    async fn reconnect(&mut self) -> io::Result<proto::BoxedConnection> {
        let stream = self
            .get_mut()
            .stream
            .get_mut()
            .get_mut()
            .get_mut()
            .reconnect()
            .await?;
        let res = TlsStream::new(
            stream,
            self.get_ref().connector.clone(),
            self.get_ref().hostname.clone(),
        )
        .await?;
        let buffered = BufStream::new(res);
        Ok(Box::new(buffered))
    }
}

#[async_trait::async_trait]
impl Reconnect for BufStream<TlsStream<proto::BoxedConnection>> {
    async fn reconnect(&mut self) -> io::Result<proto::BoxedConnection> {
        let stream = self
            .get_mut()
            .stream
            .get_mut()
            .get_mut()
            .get_mut()
            .reconnect()
            .await?;
        let res = TlsStream::new(
            stream,
            self.get_ref().connector.clone(),
            self.get_ref().hostname.clone(),
        )
        .await?;
        let buffered = BufStream::new(res);
        Ok(Box::new(buffered))
    }
}

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

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncRead for TlsStream<S> {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        self.project().stream.poll_read(cx, buf)
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncWrite for TlsStream<S> {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, io::Error>> {
        self.project().stream.poll_write(cx, buf)
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), io::Error>> {
        self.project().stream.poll_flush(cx)
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), io::Error>> {
        self.project().stream.poll_shutdown(cx)
    }
}
