#[cfg(feature = "openssl")]
#[cfg_attr(docsrs, doc(cfg(feature = "openssl")))]
/// Namespace for OpenSSL-powered [`TlsStream`](crate::openssl::TlsStream).
///
/// The underlying crate (`native-tls`) will use _SChannel_ on Windows,
/// _SecureTransport_ on OSX, and _OpenSSL_ on other platforms.
pub mod openssl;

#[cfg(feature = "rustls")]
#[cfg_attr(docsrs, doc(cfg(feature = "rustls")))]
/// Namespace for Rustls-powered [`TlsStream`](crate::rustls::TlsStream).
pub mod rustls;
