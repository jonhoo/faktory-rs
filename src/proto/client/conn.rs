use tokio::io::{AsyncBufRead, AsyncWrite};

use crate::Reconnect;

pub(crate) trait FaktoryConnection:
    AsyncWrite + AsyncBufRead + Unpin + Send + Reconnect
{
}

pub(crate) type Connection = Box<dyn FaktoryConnection>;

impl<T> FaktoryConnection for T where T: AsyncWrite + AsyncBufRead + Unpin + Send + Reconnect {}
