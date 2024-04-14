#[cfg(doc)]
use super::{Worker, WorkerBuilder};

use tokio::sync::oneshot::{self, Receiver, Sender};

/// Message sent to running worker.
///
/// See documentation to [`Worker::run`](Worker::run),
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Message {
    /// Ternimate the process with the provided status code.
    ///
    /// Analogue of hitting Ctrl+C in [`Worker::run_to_completion`].
    Exit(i32),

    /// Ternimate the process with the provided status code right away.
    ///
    /// Analogue of sending Ctrl+C signal twice on TTY.
    /// Normally, though, you will want to use [`Message::Exit`], which allows for
    /// graceful shutdown.
    ExitNow(i32),

    /// Return control to the calling site after performing the clean-up logic.
    ReturnControl,

    /// Return control to the calling site right away.
    ///
    /// Normally, though, you will want to use [`Message::ReturnControl`], which allows for
    /// clean-up within the specified [`time-out`](WorkerBuilder::graceful_shutdown_period).
    ReturnControlNow,
}

/// Returns multiple producers and a singler consumer one-shot channel for a [`Message`].
pub fn channel() -> (Sender<Message>, Receiver<Message>) {
    oneshot::channel::<Message>()
}
