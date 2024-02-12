use super::ProgressUpdate;
use crate::error::Error;
use crate::proto::single::FaktoryCommand;
use std::{fmt::Debug, io::Write};

#[derive(Debug, Clone)]
pub enum Track {
    Set(ProgressUpdate),
    Get(String),
}

impl FaktoryCommand for Track {
    fn issue<W: Write>(&self, w: &mut W) -> Result<(), Error> {
        match self {
            Self::Set(upd) => {
                w.write_all(b"TRACK SET ")?;
                serde_json::to_writer(&mut *w, upd).map_err(Error::Serialization)?;
                Ok(w.write_all(b"\r\n")?)
            }
            Self::Get(jid) => {
                w.write_all(b"TRACK GET ")?;
                w.write_all(jid.as_bytes())?;
                Ok(w.write_all(b"\r\n")?)
            }
        }
    }
}
