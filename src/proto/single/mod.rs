use std::io::prelude::*;
use std::io;

mod cmd;
mod resp;

pub use self::cmd::*;
pub use self::resp::*;

pub fn write_command<W: Write, C: FaktoryCommand>(w: &mut W, command: C) -> io::Result<()> {
    command.issue::<W>(w)?;
    w.flush()
}

pub fn write_command_and_await_ok<X: BufRead + Write, C: FaktoryCommand>(
    x: &mut X,
    command: C,
) -> io::Result<()> {
    write_command(x, command)?;
    read_ok(x)
}
