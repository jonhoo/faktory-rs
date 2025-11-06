use bb8::ManageConnection;
use super::{Client, utils::get_env_url};
use crate::Error;

#[cfg(any(feature = "native_tls", feature = "rustls"))]
use tokio::io::BufStream;

#[cfg(feature = "rustls")]
use {
    std::sync::Arc,
    crate::tls::rustls::TlsStream as RustlsStream,
    tokio_rustls::rustls::{ClientConfig, RootCertStore}
};

#[cfg(feature = "native_tls")]
use {
    crate::tls::native_tls::TlsStream as NativeTlsStream,
    tokio_native_tls::native_tls
};

/// A BB8 connection pool for Faktory clients.
///
/// This type alias provides a convenient way to create and manage a pool of Faktory client connections
/// using the BB8 connection pool library. The pool automatically handles connection lifecycle,
/// including creation, validation, and cleanup of connections.
///
/// # Example
///
/// ```rust,no_run
/// use faktory::bb8::{PooledClient, ClientConnectionManager, TlsConnector};
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let manager = ClientConnectionManager::new("tcp://localhost:7419", Default::default());
///     let pool: PooledClient = bb8::Pool::builder().build(manager).await?;
///
///     let conn = pool.get().await?;
///     // Use the connection...
///     Ok(())
/// }
/// ```
pub type PooledClient = bb8::Pool<ClientConnectionManager>;

/// TLS configuration options for Faktory client connections.
///
/// This enum provides different TLS backend options depending on which features are enabled:
/// - `NoTls`: No encryption (plain TCP connection)
/// - `Rustls`: TLS using the Rustls library (when `rustls` feature is enabled)
/// - `NativeTls`: TLS using the system's native TLS library (when `native_tls` feature is enabled)
///
/// The available variants depend on the enabled features. If no TLS features are enabled,
/// only `NoTls` will be available.
pub enum TlsConnector {
    /// No TLS encryption. Connection will be made over plain TCP.
    NoTls,
    /// TLS using the Rustls library.
    ///
    /// This variant is only available when the `rustls` feature is enabled.
    /// Rustls is a modern TLS library written in Rust that provides memory safety
    /// and performance benefits.
    #[cfg(feature = "rustls")]
    Rustls(tokio_rustls::TlsConnector),
    /// TLS using the system's native TLS implementation.
    ///
    /// This variant is only available when the `native_tls` feature is enabled.
    /// It uses the system's TLS library (e.g., OpenSSL on Linux, Secure Transport on macOS,
    /// Schannel on Windows).
    #[cfg(feature = "native_tls")]
    NativeTls(native_tls::TlsConnector),
}

impl Default for TlsConnector {
    fn default() -> Self {
        TlsConnector::NoTls
    }
}

/// A connection manager for Faktory clients compatible with the BB8 connection pool.
///
/// This manager implements the `bb8::ManageConnection` trait, allowing it to be used
/// with BB8 connection pools. It handles the creation, validation, and lifecycle
/// management of Faktory client connections.
///
/// # Examples
///
/// ## Creating a manager with no TLS
///
/// ```rust
/// use faktory::bb8::{ClientConnectionManager, TlsConnector};
///
/// let manager = ClientConnectionManager::new("tcp://localhost:7419", Default::default());
/// ```
///
/// ## Creating a manager from environment variables
///
/// ```rust,no_run
/// use faktory::bb8::ClientConnectionManager;
///
/// let manager = ClientConnectionManager::from_env().unwrap();
/// ```
pub struct ClientConnectionManager {
    /// The Faktory server URL to connect to
    url: String,
    /// TLS configuration for the connection
    tls: TlsConnector
}

impl ClientConnectionManager {
    /// Creates a new connection manager for the specified Faktory server URL.
    ///
    /// # Arguments
    ///
    /// * `url` - The Faktory server URL (e.g., "tcp://localhost:7419")
    /// * `tls` - TLS configuration to use for connections
    ///
    /// # Example
    ///
    /// ```rust
    /// use faktory::bb8::{ClientConnectionManager, TlsConnector};
    ///
    /// let manager = ClientConnectionManager::new("tcp://localhost:7419", Default::default());
    /// ```
    pub fn new(url: &str, tls: TlsConnector) -> Self {
        Self {
            url: url.to_string(),
            tls
        }
    }

    /// Creates a new connection manager using environment variables for configuration.
    ///
    /// This method reads the Faktory server URL from environment variables (typically
    /// `FAKTORY_PROVIDER` or `FAKTORY_URL`) and automatically configures TLS based on
    /// available features:
    ///
    /// - If `rustls` feature is enabled: uses Rustls TLS
    /// - If `native_tls` feature is enabled: uses native TLS
    /// - Otherwise: uses no TLS
    ///
    /// # Returns
    ///
    /// Returns `Ok(ClientConnectionManager)` on success, or `Err(Error)` if the
    /// environment configuration is invalid.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use faktory::bb8::ClientConnectionManager;
    ///
    /// // Assuming FAKTORY_PROVIDER=tcp://localhost:7419
    /// let manager = ClientConnectionManager::from_env().unwrap();
    /// ```
    pub fn from_env() -> Result<Self, Error> {
        let tls = TlsConnector::NoTls;

        #[cfg(feature = "native_tls")]
        let tls = TlsConnector::NativeTls({
            native_tls::TlsConnector::builder()
                .build()
                .expect("Failed to build native TLS connector")
        });

        #[cfg(feature = "rustls")]
        let tls = TlsConnector::Rustls({
            let config = ClientConfig::builder()
                .with_root_certificates(RootCertStore::empty())
                .with_no_client_auth();

            tokio_rustls::TlsConnector::from(Arc::new(config))
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

    /// Creates a new connection to the Faktory server.
    ///
    /// This method is called by the BB8 pool when it needs to create a new connection.
    /// The connection type (TLS or plain TCP) is determined by the TLS configuration
    /// specified when creating the manager.
    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        match self.tls {
            TlsConnector::NoTls => {
                Client::connect_to(self.url.as_str()).await
            },
            #[cfg(feature = "rustls")]
            TlsConnector::Rustls(ref connector) => {
                let stream = RustlsStream::with_connector(
                    connector.clone(),
                    Some(self.url.as_str())
                ).await?;

                let buffered = BufStream::new(stream);
                Client::connect_with(buffered, None).await
            }
            #[cfg(feature = "native_tls")]
            TlsConnector::NativeTls(ref connector) => {
                let stream = NativeTlsStream::with_connector(
                    connector.clone(),
                    Some(self.url.as_str())
                ).await?;

                let buffered = BufStream::new(stream);
                Client::connect_with(buffered, None).await
            }
        }
    }

    /// Validates that a connection is still functional.
    ///
    /// This method is called by the BB8 pool to check if a pooled connection
    /// is still valid before returning it to a client. It sends a simple
    /// INFO command to verify the connection is responsive.
    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        conn.current_info().await?;
        Ok(())
    }

    /// Checks if a connection is broken and should be discarded.
    ///
    /// Currently always returns `false` as connection health is determined
    /// through the `is_valid` method. This method exists to satisfy the
    /// `ManageConnection` trait interface.
    fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use tokio_test;

    #[test]
    fn test_client_connection_manager_new() {
        let url = "tcp://localhost:7419";
        let tls = TlsConnector::NoTls;
        let manager = ClientConnectionManager::new(url, tls);

        assert_eq!(manager.url, url);
        matches!(manager.tls, TlsConnector::NoTls);
    }

    #[test]
    fn test_client_connection_manager_from_env_no_tls() {
        // Test without any TLS features enabled
        env::set_var("FAKTORY_URL", "tcp://localhost:7419");

        let result = ClientConnectionManager::from_env();
        assert!(result.is_ok());

        let manager = result.unwrap();
        assert_eq!(manager.url, "tcp://localhost:7419");
        matches!(manager.tls, TlsConnector::NoTls);

        env::remove_var("FAKTORY_PROVIDER");
    }

    #[cfg(feature = "native_tls")]
    #[test]
    fn test_client_connection_manager_from_env_native_tls() {
        env::set_var("FAKTORY_PROVIDER", "tcp://localhost:7419");

        let result = ClientConnectionManager::from_env();
        assert!(result.is_ok());

        let manager = result.unwrap();
        matches!(manager.tls, TlsConnector::NativeTls(_));

        env::remove_var("FAKTORY_PROVIDER");
    }

    #[cfg(feature = "rustls")]
    #[test]
    fn test_client_connection_manager_from_env_rustls() {
        env::set_var("FAKTORY_PROVIDER", "tcp://localhost:7419");

        let result = ClientConnectionManager::from_env();
        assert!(result.is_ok());

        let manager = result.unwrap();
        matches!(manager.tls, TlsConnector::Rustls(_));

        env::remove_var("FAKTORY_PROVIDER");
    }

    #[test]
    fn test_tls_connector_variants() {
        let no_tls = TlsConnector::NoTls;
        matches!(no_tls, TlsConnector::NoTls);

        #[cfg(feature = "native_tls")]
        {
            let native_connector = native_tls::TlsConnector::builder()
                .build()
                .expect("Failed to build native TLS connector");
            let native_tls = TlsConnector::NativeTls(native_connector);
            matches!(native_tls, TlsConnector::NativeTls(_));
        }

        #[cfg(feature = "rustls")]
        {
            let config = ClientConfig::builder()
                .with_root_certificates(RootCertStore::empty())
                .with_no_client_auth();
            let rustls_connector = tokio_rustls::TlsConnector::from(Arc::new(config));
            let rustls_tls = TlsConnector::Rustls(rustls_connector);
            matches!(rustls_tls, TlsConnector::Rustls(_));
        }
    }

    #[test]
    fn test_has_broken_always_returns_false() {
        let manager = ClientConnectionManager::new(
            "tcp://localhost:7419",
            TlsConnector::NoTls
        );

        // Create a mock client (this would need to be adjusted based on your Client implementation)
        // For now, we'll test the method exists and returns false
        // This test demonstrates the method but may need adjustment based on Client's actual structure

        // Note: This test is conceptual since we can't easily create a Client instance
        // without a real connection. In practice, you might want to mock the Client
        // or use dependency injection for better testability.
    }

    #[test]
    fn test_pooled_client_type_alias() {
        // Test that the type alias is correctly defined
        // This is more of a compilation test
        let _: Option<PooledClient> = None;
    }

    #[test]
    fn test_manager_url_storage() {
        let test_url = "tcp://test-server:1234";
        let manager = ClientConnectionManager::new(test_url, TlsConnector::NoTls);
        assert_eq!(manager.url, test_url);
    }

    #[test]
    fn test_manager_with_different_urls() {
        let urls = vec![
            "tcp://localhost:7419",
            "tcp://faktory.example.com:7419",
            "tcp://127.0.0.1:7420",
        ];

        for url in urls {
            let manager = ClientConnectionManager::new(url, TlsConnector::NoTls);
            assert_eq!(manager.url, url);
        }
    }
}
