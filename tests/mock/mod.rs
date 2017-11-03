use faktory::{StreamConnector, Url};
use std::io;
use mockstream::SyncMockStream;

#[derive(Clone)]
pub struct Stream(SyncMockStream);

impl Default for Stream {
    fn default() -> Self {
        SyncMockStream::new().into()
    }
}

use std::ops::{Deref, DerefMut};
impl Deref for Stream {
    type Target = SyncMockStream;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl DerefMut for Stream {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<SyncMockStream> for Stream {
    fn from(s: SyncMockStream) -> Self {
        Stream(s)
    }
}

impl StreamConnector for Stream {
    type Addr = Url;
    type Stream = SyncMockStream;

    fn connect(self, _: Self::Addr) -> io::Result<Self::Stream> {
        Ok(self.0)
    }
}

impl Stream {
    pub fn hello(&mut self) {
        self.push_bytes_to_read(b"+HI {\"v\":\"1\"}\r\n");
        self.push_bytes_to_read(b"+OK\r\n");
    }

    pub fn ok(&mut self) {
        self.push_bytes_to_read(b"+OK\r\n");
    }

    pub fn ignore(&mut self) {
        self.pop_bytes_written();
    }
}
