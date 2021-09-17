use super::Job;
use failure::Error;
use std::io::prelude::*;

pub trait FaktoryCommand {
    fn issue<W: Write>(&self, w: &mut dyn Write) -> Result<(), Error>;
}

/// Write queues as part of a command. They are written with a leading space
/// followed by space separated queue names.
fn write_queues<W, S>(w: &mut dyn Write, queues: &[S]) -> Result<(), serde_json::Error>
where
    W: Write,
    S: AsRef<str>,
{
    for q in queues {
        w.write_all(b" ").map_err(serde_json::Error::io)?;
        w.write_all(q.as_ref().as_bytes())
            .map_err(serde_json::Error::io)?;
    }

    Ok(())
}

// ----------------------------------------------

pub struct Info;

impl FaktoryCommand for Info {
    fn issue<W: Write>(&self, w: &mut dyn Write) -> Result<(), Error> {
        Ok(w.write_all(b"INFO\r\n").map_err(serde_json::Error::io)?)
    }
}

// ----------------------------------------------

#[derive(Serialize)]
pub struct Ack {
    #[serde(rename = "jid")]
    job_id: String,
}

impl FaktoryCommand for Ack {
    fn issue<W: Write>(&self, w: &mut dyn Write) -> Result<(), Error> {
        w.write_all(b"ACK ").map_err(serde_json::Error::io)?;
        serde_json::to_writer(&mut *w, self)?;
        Ok(w.write_all(b"\r\n").map_err(serde_json::Error::io)?)
    }
}

impl Ack {
    pub fn new<S: Into<String>>(job_id: S) -> Ack {
        Ack {
            job_id: job_id.into(),
        }
    }
}

// ----------------------------------------------

#[derive(Serialize)]
pub struct Heartbeat {
    wid: String,
}

impl FaktoryCommand for Heartbeat {
    fn issue<W: Write>(&self, w: &mut dyn Write) -> Result<(), Error> {
        w.write_all(b"BEAT ").map_err(serde_json::Error::io)?;
        serde_json::to_writer(&mut *w, self)?;
        Ok(w.write_all(b"\r\n").map_err(serde_json::Error::io)?)
    }
}

impl Heartbeat {
    pub fn new<S: Into<String>>(wid: S) -> Heartbeat {
        Heartbeat { wid: wid.into() }
    }
}

// ----------------------------------------------

#[derive(Serialize, Clone)]
pub struct Fail {
    #[serde(rename = "jid")]
    job_id: String,
    #[serde(rename = "errtype")]
    kind: String,
    message: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    backtrace: Vec<String>,
}

impl FaktoryCommand for Fail {
    fn issue<W: Write>(&self, w: &mut dyn Write) -> Result<(), Error> {
        w.write_all(b"FAIL ").map_err(serde_json::Error::io)?;
        serde_json::to_writer(&mut *w, self)?;
        Ok(w.write_all(b"\r\n").map_err(serde_json::Error::io)?)
    }
}

impl Fail {
    pub fn new<S1: Into<String>, S2: Into<String>, S3: Into<String>>(
        job_id: S1,
        kind: S2,
        message: S3,
    ) -> Self {
        Fail {
            job_id: job_id.into(),
            kind: kind.into(),
            message: message.into(),
            backtrace: Vec::new(),
        }
    }

    pub fn set_backtrace(&mut self, lines: Vec<String>) {
        self.backtrace = lines;
    }
}

// ----------------------------------------------

pub struct End;

impl FaktoryCommand for End {
    fn issue<W: Write>(&self, w: &mut dyn Write) -> Result<(), Error> {
        Ok(w.write_all(b"END\r\n").map_err(serde_json::Error::io)?)
    }
}

// ----------------------------------------------

pub struct Fetch<'a, S>
where
    S: AsRef<str>,
{
    queues: &'a [S],
}

impl<'a, S> FaktoryCommand for Fetch<'a, S>
where
    S: AsRef<str>,
{
    fn issue<W: Write>(&self, w: &mut dyn Write) -> Result<(), Error> {
        if self.queues.is_empty() {
            w.write_all(b"FETCH\r\n").map_err(serde_json::Error::io)?;
        } else {
            w.write_all(b"FETCH").map_err(serde_json::Error::io)?;
            write_queues::<W, _>(w, self.queues)?;
            w.write_all(b"\r\n").map_err(serde_json::Error::io)?;
        }
        Ok(())
    }
}

impl<'a, S> From<&'a [S]> for Fetch<'a, S>
where
    S: AsRef<str> + 'a,
{
    fn from(queues: &'a [S]) -> Self {
        Fetch { queues }
    }
}

// ----------------------------------------------

#[derive(Serialize)]
pub struct Hello {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hostname: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pid: Option<usize>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub labels: Vec<String>,

    #[serde(rename = "v")]
    version: usize,

    /// Hash is hex(sha256(password + salt))
    #[serde(rename = "pwdhash")]
    #[serde(skip_serializing_if = "Option::is_none")]
    password_hash: Option<String>,
}

impl Default for Hello {
    fn default() -> Self {
        Hello {
            hostname: None,
            wid: None,
            pid: None,
            labels: Vec::new(),
            password_hash: None,
            version: crate::proto::EXPECTED_PROTOCOL_VERSION,
        }
    }
}

impl Hello {
    pub fn set_password(&mut self, hi: &super::resp::Hi, password: &str) {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::default();
        hasher.update(password.as_bytes());
        hasher.update(hi.salt.as_ref().unwrap().as_bytes());
        let mut hash = hasher.finalize();
        for _ in 1..hi.iterations.unwrap_or(1) {
            hash = Sha256::digest(&hash[..]);
        }
        self.password_hash = Some(format!("{:x}", hash));
    }
}

impl FaktoryCommand for Hello {
    fn issue<W: Write>(&self, w: &mut dyn Write) -> Result<(), Error> {
        w.write_all(b"HELLO ").map_err(serde_json::Error::io)?;
        serde_json::to_writer(&mut *w, self)?;
        Ok(w.write_all(b"\r\n").map_err(serde_json::Error::io)?)
    }
}

// ----------------------------------------------

pub struct Push(Job);

use std::ops::Deref;
impl Deref for Push {
    type Target = Job;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<Job> for Push {
    fn from(j: Job) -> Self {
        Push(j)
    }
}

impl FaktoryCommand for Push {
    fn issue<W: Write>(&self, w: &mut dyn Write) -> Result<(), Error> {
        w.write_all(b"PUSH ").map_err(serde_json::Error::io)?;
        serde_json::to_writer(&mut *w, &**self)?;
        Ok(w.write_all(b"\r\n").map_err(serde_json::Error::io)?)
    }
}

// ----------------------------------------------

pub enum QueueAction {
    Pause,
    Resume,
}

pub struct QueueControl<'a, S>
where
    S: AsRef<str>,
{
    pub action: QueueAction,
    pub queues: &'a [S],
}

impl<S: AsRef<str>> FaktoryCommand for QueueControl<'_, S> {
    fn issue<W: Write>(&self, w: &mut dyn Write) -> Result<(), Error> {
        let command = match self.action {
            QueueAction::Pause => b"QUEUE PAUSE".as_ref(),
            QueueAction::Resume => b"QUEUE RESUME".as_ref(),
        };

        w.write_all(command).map_err(serde_json::Error::io)?;
        write_queues::<W, _>(w, self.queues)?;
        Ok(w.write_all(b"\r\n").map_err(serde_json::Error::io)?)
    }
}

impl<'a, S: AsRef<str>> QueueControl<'a, S> {
    pub fn new(action: QueueAction, queues: &'a [S]) -> Self {
        Self { action, queues }
    }
}
