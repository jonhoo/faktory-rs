use crate::proto::single::FaktoryCommand;
use crate::{Batch, Error};
use std::io::Write;

impl FaktoryCommand for Batch {
    fn issue<W: Write>(&self, w: &mut W) -> Result<(), Error> {
        w.write_all(b"BATCH NEW ")?;
        serde_json::to_writer(&mut *w, self).map_err(Error::Serialization)?;
        Ok(w.write_all(b"\r\n")?)
    }
}

// ----------------------------------------------

pub struct CommitBatch(String);

impl From<String> for CommitBatch {
    fn from(value: String) -> Self {
        CommitBatch(value)
    }
}

impl FaktoryCommand for CommitBatch {
    fn issue<W: Write>(&self, w: &mut W) -> Result<(), Error> {
        w.write_all(b"BATCH COMMIT ")?;
        w.write_all(self.0.as_bytes())?;
        Ok(w.write_all(b"\r\n")?)
    }
}

// ----------------------------------------------

pub struct GetBatchStatus(String);

impl From<String> for GetBatchStatus {
    fn from(value: String) -> Self {
        GetBatchStatus(value)
    }
}

impl FaktoryCommand for GetBatchStatus {
    fn issue<W: Write>(&self, w: &mut W) -> Result<(), Error> {
        w.write_all(b"BATCH STATUS ")?;
        w.write_all(self.0.as_bytes())?;
        Ok(w.write_all(b"\r\n")?)
    }
}

// ----------------------------------------------

pub struct OpenBatch(String);

impl From<String> for OpenBatch {
    fn from(value: String) -> Self {
        OpenBatch(value)
    }
}

impl FaktoryCommand for OpenBatch {
    fn issue<W: Write>(&self, w: &mut W) -> Result<(), Error> {
        w.write_all(b"BATCH OPEN ")?;
        w.write_all(self.0.as_bytes())?;
        Ok(w.write_all(b"\r\n")?)
    }
}
