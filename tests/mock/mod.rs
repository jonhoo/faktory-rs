use faktory::Reconnect;
use std::{
    io,
    pin::Pin,
    sync::{Arc, Mutex},
};
use tokio::io::{AsyncRead, AsyncWrite, BufStream};

mod inner;

#[derive(Clone)]
pub struct Stream {
    mine: inner::MockStream,
    all: Arc<Mutex<inner::Inner>>,
    check_count: bool,
}

impl Default for Stream {
    fn default() -> Self {
        Self::new(1)
    }
}

#[async_trait::async_trait]
impl Reconnect for Stream {
    async fn reconnect(&mut self) -> io::Result<Box<dyn faktory::Connection>> {
        let mine = self
            .all
            .lock()
            .unwrap()
            .take_stream()
            .expect("tried to make a new stream, but no more connections expected");
        Ok(Box::new(BufStream::new(Stream {
            mine,
            all: Arc::clone(&self.all),
            check_count: self.check_count,
        })))
    }
}

impl AsyncRead for Stream {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        let mut duplex = self.mine.du.lock().unwrap();
        Pin::new(&mut duplex.reader).poll_read(cx, buf)
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, io::Error>> {
        let mut duplex = self.mine.du.lock().unwrap();
        Pin::new(&mut duplex.writer).poll_write(cx, buf)
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), io::Error>> {
        let mut duplex = self.mine.du.lock().unwrap();
        Pin::new(&mut duplex.writer).poll_flush(cx)
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), io::Error>> {
        let mut duplex = self.mine.du.lock().unwrap();
        Pin::new(&mut duplex.writer).poll_shutdown(cx)
    }
}

impl Stream {
    fn make(salt: Option<(usize, &str)>, streams: usize, check_count: bool) -> Self {
        let streams = (0..streams)
            .map(|_| {
                let mut s = inner::MockStream::default();
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

        // So if they asked for two stream (see `consumer::terminate` test),
        // the first one will be `mine` while they both will be accessible
        // internally via `all` (since `Inner::take_stream` is not actually
        // taking, it is rather _cloning_).
        Stream {
            mine,
            all: Arc::new(Mutex::new(inner)),
            check_count,
        }
    }

    pub fn new(streams: usize) -> Self {
        Self::make(None, streams, true)
    }

    /// Use this method if you want to opt out of comparing the number of used streams with the number of
    /// initially allocated streams when `Stream` is being dropped (see [`Stream::drop`]).
    ///
    /// We are normally doing this sanity check just to make sure the test went as we expected,
    /// like the number of spawned workers was correct (since each worker will get a dedicated strem).
    ///
    /// There is at least one scenario though where you will want to avoid this check. Imagine, the "coordinating"
    /// worker is still there but a "processing" worker has been dropped, due to some protocol error. They can still
    /// try and `Worker::run` the coordinator again and - since it has not been previously marked as terminated - the
    /// coordinator will spawn another worker, and this can in theory repeat however many times as long as we have
    /// allocated enough streams in [`Stream::all`]. So, in this scenario, we do not want to run our usual check whenever
    /// a processing worker is being dropped.
    #[allow(unused)]
    pub fn new_unchecked(stream: usize) -> Self {
        Self::make(None, stream, false)
    }

    pub fn with_salt(iters: usize, salt: &str) -> Self {
        Self::make(Some((iters, salt)), 1, true)
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
        if self.check_count {
            assert_eq!(x.take_next, x.streams.len());
        }
    }
}
