use faktory::Reconnect;
use std::io;
use mockstream::SyncMockStream;
use std::sync::{Arc, Mutex};

struct Inner {
    take_next: usize,
    streams: Vec<SyncMockStream>,
}

impl Inner {
    fn take_stream(&mut self) -> Option<SyncMockStream> {
        self.take_next += 1;

        self.streams.get(self.take_next - 1).cloned()
    }
}

#[derive(Clone)]
pub struct Stream {
    mine: Option<SyncMockStream>,
    all: Arc<Mutex<Inner>>,
}

impl Default for Stream {
    fn default() -> Self {
        Self::new(1)
    }
}

impl Reconnect for Stream {
    fn reconnect(&self) -> io::Result<Self> {
        let mine = self.all
            .lock()
            .unwrap()
            .take_stream()
            .expect("tried to make a new stream, but no more connections expected");
        Ok(Stream {
            mine: Some(mine),
            all: self.all.clone(),
        })
    }
}

impl io::Read for Stream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.mine.as_mut().unwrap().read(buf)
    }
}

impl io::Write for Stream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.mine.as_mut().unwrap().write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.mine.as_mut().unwrap().flush()
    }
}

impl Stream {
    fn make(salt: Option<&[u8]>, streams: usize) -> Self {
        let streams = (0..streams)
            .map(|_| {
                let mut s = SyncMockStream::new();
                // need to say HELLO
                if let Some(salt) = salt {
                    // include salt for pwdhash
                    s.push_bytes_to_read(b"+HI {\"v\":\"1\",\"s\":\"");
                    s.push_bytes_to_read(salt);
                    s.push_bytes_to_read(b"\"}\r\n");
                } else {
                    s.push_bytes_to_read(b"+HI {\"v\":\"1\"}\r\n");
                }
                s.push_bytes_to_read(b"+OK\r\n");
                s
            })
            .collect();

        let mut inner = Inner {
            take_next: 0,
            streams: streams,
        };
        let mine = inner.take_stream();

        Stream {
            mine: mine,
            all: Arc::new(Mutex::new(inner)),
        }
    }

    pub fn new(streams: usize) -> Self {
        Self::make(None, streams)
    }

    pub fn with_salt(salt: &[u8]) -> Self {
        Self::make(Some(salt), 1)
    }

    pub fn ok(&mut self, stream: usize) {
        self.push_bytes_to_read(stream, b"+OK\r\n");
    }

    pub fn ignore(&mut self, stream: usize) {
        self.pop_bytes_written(stream);
    }

    pub fn push_bytes_to_read(&mut self, stream: usize, bytes: &[u8]) {
        self.all.lock().unwrap().streams[stream].push_bytes_to_read(bytes);
    }

    pub fn pop_bytes_written(&mut self, stream: usize) -> Vec<u8> {
        self.all.lock().unwrap().streams[stream].pop_bytes_written()
    }
}

impl Drop for Stream {
    fn drop(&mut self) {
        let x = self.all.lock().unwrap();
        assert_eq!(x.take_next, x.streams.len());
    }
}
