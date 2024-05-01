#[cfg(feature = "native_tls")]
#[cfg_attr(docsrs, doc(cfg(feature = "native_tls")))]
/// Namespace for native TLS powered [`TlsStream`](crate::native_tls::TlsStream).
///
/// The underlying crate (`native-tls`) will use _SChannel_ on Windows,
/// _SecureTransport_ on OSX, and _OpenSSL_ on other platforms.
pub mod native_tls;

#[cfg(feature = "rustls")]
#[cfg_attr(docsrs, doc(cfg(feature = "rustls")))]
/// Namespace for Rustls-powered [`TlsStream`](crate::rustls::TlsStream).
pub mod rustls;
