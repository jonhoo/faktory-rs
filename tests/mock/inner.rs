use std::sync::{Arc, Mutex};
use std::{io, mem};

#[derive(Debug, Clone, Default)]
pub(crate) struct Duplex {
    pub reader: io::Cursor<Vec<u8>>,
    pub writer: io::Cursor<Vec<u8>>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct MockStream {
    pub du: Arc<Mutex<Duplex>>,
}

impl MockStream {
    pub fn push_bytes_to_read(&mut self, bytes: &[u8]) {
        self.du.lock().unwrap().reader.get_mut().extend(bytes);
    }

    pub fn pop_bytes_written(&mut self) -> Vec<u8> {
        let mut du = self.du.lock().unwrap();
        let wr = mem::take(du.writer.get_mut());
        du.writer.set_position(0);
        wr
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
