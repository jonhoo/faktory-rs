use std::io;
use tokio::io::BufStream;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream as TokioStream;

mod client;
pub(crate) use client::{
    BoxedConnection, ClientOptions, HeartbeatStatus, EXPECTED_PROTOCOL_VERSION,
};
pub use client::{Client, Connection};

mod single;

pub use single::{
    DataSnapshot, Failure, FaktoryState, Job, JobBuilder, JobId, ServerSnapshot, WorkerId,
};

pub use single::mutation::{Filter, JobSet};

pub(crate) use single::{Ack, Fail, Info, Push, PushBulk, QueueAction, QueueControl};

pub(crate) mod utils;

#[cfg(feature = "ent")]
pub use self::single::ent::{JobState, Progress, ProgressUpdate, ProgressUpdateBuilder};

#[cfg(feature = "ent")]
pub(crate) use self::single::ent::FetchProgress;

#[cfg(feature = "ent")]
pub use self::single::BatchId;

#[cfg(feature = "ent")]
mod batch;
#[cfg(feature = "ent")]
pub use batch::{Batch, BatchBuilder, BatchHandle, BatchStatus, CallbackState};

/// A stream that can be re-established after failing.
#[async_trait::async_trait]
pub trait Reconnect {
    /// Re-establish the stream.
    async fn reconnect(&mut self) -> io::Result<BoxedConnection>;
}

#[async_trait::async_trait]
impl<S> Reconnect for Box<S>
where
    S: Reconnect + Send,
{
    async fn reconnect(&mut self) -> io::Result<BoxedConnection> {
        (**self).reconnect().await
    }
}

#[async_trait::async_trait]
impl Reconnect for TokioStream {
    async fn reconnect(&mut self) -> io::Result<BoxedConnection> {
        let addr = &self.peer_addr().expect("socket address");
        let stream = TokioStream::connect(addr).await?;
        Ok(Box::new(BufStream::new(stream)))
    }
}

#[async_trait::async_trait]
impl<S> Reconnect for BufStream<S>
where
    S: AsyncRead + AsyncWrite + Reconnect + Send + Sync,
{
    async fn reconnect(&mut self) -> io::Result<BoxedConnection> {
        self.get_mut().reconnect().await
    }
}
