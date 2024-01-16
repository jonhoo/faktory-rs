mod cmd;
mod resp;

pub use cmd::*;
pub use resp::*;

use tokio::io::{AsyncBufRead, AsyncWriteExt};

use crate::Error;

pub async fn write_command<W: AsyncWriteExt + Unpin + Send, C: AsyncFaktoryCommand>(
    w: &mut W,
    command: &C,
) -> Result<(), Error> {
    command.issue::<W>(w).await?;
    Ok(w.flush().await?)
}

pub async fn write_command_and_await_ok<
    S: AsyncBufRead + AsyncWriteExt + Unpin + Send,
    C: AsyncFaktoryCommand,
>(
    stream: &mut S,
    command: &C,
) -> Result<(), Error> {
    write_command(stream, command).await?;
    read_ok(stream).await
}
