#[cfg(doc)]
use super::Worker;

use tokio::sync::mpsc::{self, Receiver, Sender};

/// Message sent to running worker.
///
/// See documentation to [`Worker::run`](Worker::run)
pub enum Message {
    /// Kill the process.
    ExitProcess,

    /// Return control to the userland.
    ReturnControl,
}

/// Returns multiple producers and a singler consumer of a [`Message`].
pub fn channel() -> (Sender<Message>, Receiver<Message>) {
    let buf_size = std::mem::size_of::<Message>();
    mpsc::channel::<Message>(buf_size)
}
