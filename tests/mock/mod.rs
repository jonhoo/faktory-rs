use faktory::Reconnect;
use std::{
    io,
    pin::Pin,
    sync::{Arc, Mutex},
};
use tokio::io::{AsyncRead, AsyncWrite};

mod inner;

#[derive(Clone)]
pub struct Stream {
    mine: inner::MockStream,
    all: Arc<Mutex<inner::Inner>>,
}

impl Default for Stream {
    fn default() -> Self {
        Self::new(1)
    }
}

#[async_trait::async_trait]
impl Reconnect for Stream {
    async fn reconnect(&self) -> Result<Self, io::Error> {
        let mine = self
            .all
            .lock()
            .unwrap()
            .take_stream()
            .expect("tried to make a new stream, but no more connections expected");
        Ok(Stream {
            mine,
            all: Arc::clone(&self.all),
        })
    }
}

impl AsyncRead for Stream {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        // let wrapper = ;
        let mut duplex = self.mine.duplex.lock().unwrap();
        Pin::new(&mut duplex.reader).poll_read(cx, buf)
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, io::Error>> {
        let mut duplex = self.mine.duplex.lock().unwrap();
        Pin::new(&mut duplex.writer).poll_write(cx, buf)
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), io::Error>> {
        let mut duplex = self.mine.duplex.lock().unwrap();
        Pin::new(&mut duplex.writer).poll_flush(cx)
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), io::Error>> {
        let mut duplex = self.mine.duplex.lock().unwrap();
        Pin::new(&mut duplex.writer).poll_shutdown(cx)
    }
}

impl Stream {
    fn make(salt: Option<(usize, &str)>, streams: usize) -> Self {
        let streams = (0..streams)
            .map(|_| {
                let mut s = inner::MockStream::default();
                eprintln!("{:#?}", s);
                // need to say HELLO
                if let Some((iters, salt)) = salt {
                    // include salt for pwdhash
                    s.push_bytes_to_read(
                        format!("+HI {{\"v\":2,\"i\":{},\"s\":\"{}\"}}\r\n", iters, salt)
                            .as_bytes(),
                    )
                } else {
                    s.push_bytes_to_read(b"+HI {\"v\":2,\"i\":1}\r\n");
                }
                s.push_bytes_to_read(b"+OK\r\n");
                s
            })
            .collect();

        let mut inner = inner::Inner {
            take_next: 0,
            streams,
        };
        let mine = inner.take_stream().unwrap();

        Stream {
            mine,
            all: Arc::new(Mutex::new(inner)),
        }
    }

    pub fn new(streams: usize) -> Self {
        Self::make(None, streams)
    }

    pub fn with_salt(iters: usize, salt: &str) -> Self {
        Self::make(Some((iters, salt)), 1)
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
