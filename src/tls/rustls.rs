#[cfg(doc)]
use crate::{Client, WorkerBuilder};

use crate::proto::{self, utils};
use crate::{Error, Reconnect};
use rustls_platform_verifier::ConfigVerifierExt;
use std::io;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite, BufStream};
use tokio::net::TcpStream as TokioTcpStream;
use tokio_rustls::client::TlsStream as RustlsStream;
use tokio_rustls::rustls::{ClientConfig, RootCertStore};
use tokio_rustls::TlsConnector;

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
/// use faktory::rustls::TlsStream;
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
    connector: TlsConnector,
    hostname: String,
    #[pin]
    stream: RustlsStream<S>,
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
    ///
    /// Internally, creates a [`ClientConfig`](https://docs.rs/rustls/latest/rustls/client/struct.ClientConfig.html)
    /// with an _empty_ root certificates store and _no client authentication_.
    ///
    /// Use [`with_client_config`](TlsStream::with_client_config) or [`with_connector`](TlsStream::with_connector)
    /// for customized `ClientConfig` and [`TlsConnector`](https://docs.rs/tokio-rustls/latest/tokio_rustls/struct.TlsConnector.html)
    /// accordingly.
    pub async fn connect() -> Result<Self, Error> {
        let config = ClientConfig::builder()
            .with_root_certificates(RootCertStore::empty())
            .with_no_client_auth();
        let connnector = TlsConnector::from(Arc::new(config));
        TlsStream::with_connector(connnector, None).await
    }

    /// Create a new TLS connection to Faktory over TCP using specified address.
    pub async fn connect_to(addr: &str) -> Result<Self, Error> {
        let config = ClientConfig::builder()
            .with_root_certificates(RootCertStore::empty())
            .with_no_client_auth();
        let connector = TlsConnector::from(Arc::new(config));
        TlsStream::with_connector(connector, Some(addr)).await
    }

    /// Create a new TLS connection over TCP using platform certificates.
    ///
    /// Unlike [`TlsStream::connect`], creates a root certificates store populated
    /// with the certificates loaded from a platform-native certificate store.
    ///
    /// Similarly to [`TlsStream::connect`], no client authentication will be used.
    pub async fn connect_with_native_certs_to(addr: &str) -> Result<Self, Error> {
        let config = ClientConfig::with_platform_verifier();
        TlsStream::with_connector(TlsConnector::from(Arc::new(config)), Some(addr)).await
    }

    /// Create a new TLS connection over TCP using a non-default TLS configuration.
    ///
    /// See `connect` for details about the `url` parameter.
    pub async fn with_client_config(conf: ClientConfig, url: Option<&str>) -> Result<Self, Error> {
        let con = TlsConnector::from(Arc::new(conf));
        TlsStream::with_connector(con, url).await
    }

    /// Create a new TLS connection over TCP using a connector with a non-default TLS configuration.
    ///
    /// See `connect` for details about the `url` parameter.
    pub async fn with_connector(connector: TlsConnector, url: Option<&str>) -> Result<Self, Error> {
        let url = match url {
            Some(url) => utils::url_parse(url),
            None => utils::url_parse(&utils::get_env_url()),
        }?;
        let host_and_port = utils::host_from_url(&url);
        let tcp_stream = TokioTcpStream::connect(&host_and_port).await?;
        let host = url.host_str().unwrap().to_string();
        Ok(TlsStream::new(tcp_stream, connector, host).await?)
    }
}

impl<S> TlsStream<S>
where
    S: AsyncRead + AsyncWrite + Send + Unpin,
{
    /// Create a new TLS connection on an existing stream.
    ///
    /// Internally, creates a [`ClientConfig`](https://docs.rs/tokio-native-tls/latest/tokio_native_tls/native_tls/struct.TlsConnector.html)
    /// with an empty root certificates store and no client authentication.
    ///
    /// Use [`new`](TlsStream::new) for a customized [`TlsConnector`](https://docs.rs/tokio-rustls/latest/tokio_rustls/struct.TlsConnector.html).
    pub async fn default(stream: S, hostname: String) -> io::Result<Self> {
        let conf = ClientConfig::builder()
            .with_root_certificates(RootCertStore::empty())
            .with_no_client_auth();

        Self::new(stream, TlsConnector::from(Arc::new(conf)), hostname).await
    }

    /// Create a new TLS connection on an existing stream with a non-default TLS configuration.
    pub async fn new(stream: S, connector: TlsConnector, hostname: String) -> io::Result<Self> {
        let server_name = hostname
            .clone()
            .try_into()
            .expect("a valid DNS name or IP address");
        let tls_stream = connector
            .connect(server_name, stream)
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
        let stream = self.get_mut().stream.get_mut().0.reconnect().await?;
        let tls_stream = TlsStream::new(
            stream,
            self.get_ref().connector.clone(),
            self.get_ref().hostname.clone(),
        )
        .await?;
        let buffered = BufStream::new(tls_stream);
        Ok(Box::new(buffered))
    }
}

#[async_trait::async_trait]
impl Reconnect for BufStream<TlsStream<proto::BoxedConnection>> {
    async fn reconnect(&mut self) -> io::Result<proto::BoxedConnection> {
        let stream = self.get_mut().stream.get_mut().0.reconnect().await?;
        let tls_stream = TlsStream::new(
            stream,
            self.get_ref().connector.clone(),
            self.get_ref().hostname.clone(),
        )
        .await?;
        let buffered = BufStream::new(tls_stream);
        Ok(Box::new(buffered))
    }
}

impl<S> Deref for TlsStream<S> {
    type Target = RustlsStream<S>;
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
