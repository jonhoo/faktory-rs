use std::io;
use tokio::io::BufStream;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream as TokioStream;

mod client;
pub use client::Client;
pub(crate) use client::{ClientOptions, HeartbeatStatus, EXPECTED_PROTOCOL_VERSION};

mod single;

pub use single::{
    Ack, Fail, Info, Job, JobBuilder, JobId, Push, PushBulk, QueueAction, QueueControl, WorkerId,
};
pub(crate) mod utils;

#[cfg(feature = "ent")]
pub use self::single::ent::{JobState, Progress, ProgressUpdate, ProgressUpdateBuilder, Track};

#[cfg(feature = "ent")]
pub use self::single::BatchId;

#[cfg(feature = "ent")]
mod batch;
#[cfg(feature = "ent")]
pub use batch::{
    Batch, BatchBuilder, BatchHandle, BatchStatus, CallbackState, GetBatchStatus, OpenBatch,
};

/// A stream that can be re-established after failing.
#[async_trait::async_trait]
pub trait Reconnect: Sized {
    /// Re-establish the stream.
    async fn reconnect(&mut self) -> io::Result<Self>;
}

#[async_trait::async_trait]
impl Reconnect for TokioStream {
    async fn reconnect(&mut self) -> io::Result<Self> {
        let addr = &self.peer_addr().expect("socket address");
        TokioStream::connect(addr).await
    }
}

#[async_trait::async_trait]
impl<S> Reconnect for BufStream<S>
where
    S: AsyncRead + AsyncWrite + Reconnect + Send + Sync,
{
    async fn reconnect(&mut self) -> io::Result<Self> {
        let stream = self.get_mut().reconnect().await?;
        Ok(Self::new(stream))
    }
}
