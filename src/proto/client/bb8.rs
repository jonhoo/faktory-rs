use bb8::ManageConnection;
use super::{Client, utils::get_env_url};
use crate::Error;

#[cfg(any(feature = "native_tls", feature = "rustls"))]
use tokio::io::BufStream;

#[cfg(feature = "rustls")]
use {
    std::sync::Arc,
    crate::tls::rustls::TlsStream,
    tokio_rustls::{rustls::{ClientConfig, RootCertStore}, TlsConnector}
};

#[cfg(feature = "native_tls")]
use tokio_native_tls::{native_tls::TlsConnector};

/// A BB8 connection pool for Faktory clients.
pub type PooledClient = bb8::Pool<ClientConnectionManager>;

/// TLS configuration for Faktory clients.
pub enum Tls {
    /// No TLS.
    NoTls,
    /// TLS using Rustls.
    #[cfg(feature = "rustls")]
    Rustls(TlsConnector),
    /// TLS using Native TLS.
    #[cfg(feature = "native_tls")]
    NativeTls(TlsConnector),
}

/// A connection manager for Faktory clients to be used with BB8.
pub struct ClientConnectionManager {
    url: String,
    tls: Tls
}

impl ClientConnectionManager {
    /// Create a new connection manager for the given URL.
    pub fn new(url: &str, tls: Tls) -> Self {
        Self {
            url: url.to_string(),
            tls
        }
    }

    /// Create a new connection manager using the URL from the `FAKTORY_PROVIDER`
    pub fn from_env() -> Result<Self, Error> {
        let tls = Tls::NoTls;

        #[cfg(feature = "rustls")]
        let tls = Tls::Rustls({
            let config = ClientConfig::builder()
                .with_root_certificates(RootCertStore::empty())
                .with_no_client_auth();

            tokio_rustls::TlsConnector::from(Arc::new(config))
        });

        #[cfg(feature = "native_tls")]
        let tls = Tls::NativeTls({
            TlsConnector::builder()
                .build()
                .expect("Failed to build native TLS connector")
        });

        Ok(Self {
            url: get_env_url(),
            tls
        })
    }
}

impl ManageConnection for ClientConnectionManager {
    type Connection = Client;
    type Error = Error;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        match self.tls {
            Tls::NoTls => {
                Client::connect_to(self.url.as_str()).await
            },
            #[cfg(feature = "rustls")]
            Tls::Rustls(ref connector) => {
                let stream = TlsStream::with_connector(
                    connector.clone(),
                    Some(self.url.as_str())
                ).await?;

                let buffered = BufStream::new(stream);
                Client::connect_with(buffered, None).await
            }
            #[cfg(feature = "native_tls")]
            Tls::NativeTls(ref connector) => {
                let stream = crate::tls::native_tls::TlsStream::with_connector(
                    connector.clone(),
                    Some(self.url.as_str())
                ).await?;

                let buffered = BufStream::new(stream);
                Client::connect_with(buffered, None).await
            }
        }
    }
    
    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        conn.current_info().await?;
        Ok(())
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        false
    }
}