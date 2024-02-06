use std::fmt::Debug;
use std::io;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream as TokioTcpStream;
use tokio_rustls::client::TlsStream;
use tokio_rustls::rustls::{ClientConfig, RootCertStore};
use tokio_rustls::TlsConnector;

use crate::{proto, AsyncReconnect, Error};

/// A reconnectable asynchronous stream encrypted with TLS.
///
/// This can be used as an argument to `AsyncConsumer::connect_with` and `AsyncProducer::connect_with` to
/// connect to a TLS-secured Faktory server.
///
/// # Examples
///
/// ```no_run
/// # tokio_test::block_on(async {
/// use faktory::{AsyncProducer, AsyncTlsStream};
/// let tls = AsyncTlsStream::connect(None).await.unwrap();
/// let p = AsyncProducer::connect_with(tls, None).await.unwrap();
/// # drop(p);
/// # })
/// ```
///
#[pin_project::pin_project]
pub struct AsyncTlsStream<S> {
    connector: TlsConnector,
    hostname: String,
    #[pin]
    stream: TlsStream<S>,
}

impl AsyncTlsStream<TokioTcpStream> {
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
    ///
    /// Internally creates a `ClientConfig` with an empty root certificates store and no client
    /// authentication. Use [`with_client_config`](struct.AsyncTlsStream.html#structmethod.with_client_config)
    /// or [`with_connector`](struct.AsyncTlsStream.html#structmethod.with_connector) for customized
    /// `ClientConfig` and `TlsConnector` accordingly.
    ///
    pub async fn connect(url: Option<&str>) -> Result<Self, Error> {
        let conf = ClientConfig::builder()
            .with_root_certificates(RootCertStore::empty())
            .with_no_client_auth();
        let con = TlsConnector::from(Arc::new(conf));
        AsyncTlsStream::with_connector(con, url).await
    }

    /// Create a new asynchronous TLS connection over TCP using a non-default TLS configuration.
    ///
    /// See `connect` for details about the `url` parameter.
    pub async fn with_client_config(conf: ClientConfig, url: Option<&str>) -> Result<Self, Error> {
        let con = TlsConnector::from(Arc::new(conf));
        AsyncTlsStream::with_connector(con, url).await
    }

    /// Create a new asynchronous TLS connection over TCP using a connector with a non-default TLS configuration.
    ///
    /// See `connect` for details about the `url` parameter.
    pub async fn with_connector(connector: TlsConnector, url: Option<&str>) -> Result<Self, Error> {
        let url = match url {
            Some(url) => proto::url_parse(url),
            None => proto::url_parse(&proto::get_env_url()),
        }?;
        let hostname = proto::host_from_url(&url);
        let tcp_stream = TokioTcpStream::connect(&hostname).await?;
        Ok(AsyncTlsStream::new(tcp_stream, connector, url.host_str().unwrap()).await?)
    }
}

impl<S> AsyncTlsStream<S>
where
    S: AsyncRead + AsyncWrite + Send + Unpin + AsyncReconnect + Debug + 'static,
{
    /// Create a new asynchronous TLS connection on an existing stream.
    ///
    /// Internally creates a `ClientConfig` with an empty root certificates store and no client
    /// authentication. Use [`new`](struct.AsyncTlsStream.html#structmethod.new) for a customized `TlsConnector`.
    pub async fn default(stream: S, hostname: &str) -> io::Result<Self> {
        let conf = ClientConfig::builder()
            .with_root_certificates(RootCertStore::empty())
            .with_no_client_auth();

        Self::new(stream, TlsConnector::from(Arc::new(conf)), hostname).await
    }

    /// Create a new asynchronous TLS connection on an existing stream with a non-default TLS configuration.
    pub async fn new(stream: S, connector: TlsConnector, hostname: &str) -> io::Result<Self> {
        let domain = hostname
            .to_string()
            .clone()
            .try_into()
            .expect("a valid DNS name or IP address");
        let tls_stream = connector
            .connect(domain, stream)
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::ConnectionAborted, e))?;
        Ok(AsyncTlsStream {
            connector,
            hostname: hostname.to_string(),
            stream: tls_stream,
        })
    }
}

#[async_trait::async_trait]
impl<S> AsyncReconnect for AsyncTlsStream<S>
where
    S: AsyncRead + AsyncWrite + Send + Unpin + AsyncReconnect + Debug + 'static + Sync,
{
    async fn reconnect(&self) -> Result<Self, Error> {
        let stream = self.stream.get_ref().0.reconnect().await?;
        Ok(Self::new(stream, self.connector.clone(), &self.hostname).await?)
    }
}

impl<S> Deref for AsyncTlsStream<S> {
    type Target = TlsStream<S>;
    fn deref(&self) -> &Self::Target {
        &self.stream
    }
}

impl<S> DerefMut for AsyncTlsStream<S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.stream
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncRead for AsyncTlsStream<S> {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        self.project().stream.poll_read(cx, buf)
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncWrite for AsyncTlsStream<S> {
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
