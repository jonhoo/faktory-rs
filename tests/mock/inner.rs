use std::{
    io::{self},
    mem,
};

#[derive(Debug, Clone, Default)]
#[pin_project::pin_project]
pub(crate) struct MockStream {
    #[pin]
    pub reader: io::Cursor<Vec<u8>>,
    #[pin]
    pub writer: io::Cursor<Vec<u8>>,
}

impl MockStream {
    pub fn push_bytes_to_read(&mut self, bytes: &[u8]) {
        // let aval = self.0.get_ref().len();
        // if self.0.position() == aval as u64 {
        //     self.0 = Default::default();
        // }
        println!(
            "push_bytes_to_read  {:#?}",
            String::from_utf8(bytes.to_owned()).unwrap()
        );
        self.reader.get_mut().extend(bytes);
    }

    pub fn pop_bytes_written(&mut self) -> Vec<u8> {
        let mut wr = Vec::new();
        println!(
            "pop_bytes_written WHATS INSIDE WR {:#?}",
            String::from_utf8(self.writer.get_ref().clone()).unwrap()
        );
        println!(
            "pop_bytes_written WHATS INSIDE RE {:#?}",
            String::from_utf8(self.reader.get_ref().clone()).unwrap()
        );
        mem::swap(&mut wr, self.writer.get_mut());
        println!(
            "pop_bytes_written {:#?}",
            String::from_utf8(wr.clone()).unwrap()
        );
        self.writer.set_position(0);
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
