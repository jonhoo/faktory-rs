use faktory::{StreamConnector, Url};
use std::io;
use mockstream::SharedMockStream;

#[derive(Clone)]
pub struct Stream(SharedMockStream);

impl Default for Stream {
    fn default() -> Self {
        SharedMockStream::new().into()
    }
}

use std::ops::{Deref, DerefMut};
impl Deref for Stream {
    type Target = SharedMockStream;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl DerefMut for Stream {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<SharedMockStream> for Stream {
    fn from(s: SharedMockStream) -> Self {
        Stream(s)
    }
}

impl StreamConnector for Stream {
    type Addr = Url;
    type Stream = SharedMockStream;

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
