use faktory::StreamConnector;
use url::Url;
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
pub struct Stream(Arc<Mutex<Inner>>);

impl Default for Stream {
    fn default() -> Self {
        Self::new(1)
    }
}

impl StreamConnector for Stream {
    type Addr = Url;
    type Stream = SyncMockStream;

    fn connect(&self, _: Self::Addr) -> io::Result<Self::Stream> {
        Ok(
            self.0
                .lock()
                .unwrap()
                .take_stream()
                .expect("tried to make a new stream, but no more connections expected"),
        )
    }
}

impl Stream {
    pub fn new(streams: usize) -> Self {
        let mut s = Stream(Arc::new(Mutex::new(Inner {
            take_next: 0,
            streams: vec![],
        })));
        s.set_num_streams(streams);
        s
    }

    fn set_num_streams(&mut self, n: usize) {
        let mut x = self.0.lock().unwrap();
        for _ in x.streams.len()..n {
            let mut s = SyncMockStream::new();
            // need to say HELLO
            s.push_bytes_to_read(b"+HI {\"v\":\"1\"}\r\n");
            s.push_bytes_to_read(b"+OK\r\n");
            x.streams.push(s);
        }
    }

    pub fn ok(&mut self, stream: usize) {
        self.push_bytes_to_read(stream, b"+OK\r\n");
    }

    pub fn ignore(&mut self, stream: usize) {
        self.pop_bytes_written(stream);
    }

    pub fn push_bytes_to_read(&mut self, stream: usize, bytes: &[u8]) {
        self.0.lock().unwrap().streams[stream].push_bytes_to_read(bytes);
    }

    pub fn pop_bytes_written(&mut self, stream: usize) -> Vec<u8> {
        self.0.lock().unwrap().streams[stream].pop_bytes_written()
    }
}

impl Drop for Stream {
    fn drop(&mut self) {
        let x = self.0.lock().unwrap();
        assert_eq!(x.take_next, x.streams.len());
    }
}
