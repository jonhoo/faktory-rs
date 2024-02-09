use std::io::Cursor;

#[derive(Debug, Clone, Default)]
#[pin_project::pin_project]
pub(crate) struct MockStream(#[pin] pub Cursor<Vec<u8>>);

impl MockStream {
    pub fn new() -> Self {
        todo!()
    }

    pub fn push_bytes_to_read(&mut self, bytes: &[u8]) {
        todo!()
    }

    pub fn pop_bytes_written(&mut self) -> Vec<u8> {
        todo!()
    }
}

pub(crate) struct Inner {
    pub take_next: usize,
    pub streams: Vec<MockStream>,
}

impl Inner {
    pub fn take_stream(&mut self) -> Option<MockStream> {
        self.take_next += 1;

        self.streams.get(self.take_next - 1).cloned()
    }
}
