use tokio::io::{AsyncBufRead, AsyncWrite};

use crate::Reconnect;

/// A duplex buffered stream to the Faktory service.
pub trait Connection: AsyncWrite + AsyncBufRead + Unpin + Send + Reconnect {}

impl<T> Connection for T where T: AsyncWrite + AsyncBufRead + Unpin + Send + Reconnect {}

pub type BoxedConnection = Box<dyn Connection>;
