use std::io::prelude::*;
use super::Job;
use serde_json;

pub trait FaktoryCommand {
    fn issue<W: Write>(&self, w: &mut Write) -> serde_json::Result<()>;
}

// ----------------------------------------------

pub struct Info;

impl FaktoryCommand for Info {
    fn issue<W: Write>(&self, w: &mut Write) -> serde_json::Result<()> {
        w.write_all(b"INFO\r\n").map_err(serde_json::Error::io)
    }
}

// ----------------------------------------------

#[derive(Serialize)]
pub struct Ack {
    #[serde(rename = "jid")] job_id: String,
}

impl FaktoryCommand for Ack {
    fn issue<W: Write>(&self, w: &mut Write) -> serde_json::Result<()> {
        w.write_all(b"ACK ").map_err(serde_json::Error::io)?;
        serde_json::to_writer(&mut *w, self)?;
        w.write_all(b"\r\n").map_err(serde_json::Error::io)
    }
}

impl Ack {
    pub fn new<S: ToString>(job_id: S) -> Ack {
        Ack {
            job_id: job_id.to_string(),
        }
    }
}

// ----------------------------------------------

#[derive(Serialize)]
pub struct Heartbeat {
    wid: String,
}

impl FaktoryCommand for Heartbeat {
    fn issue<W: Write>(&self, w: &mut Write) -> serde_json::Result<()> {
        w.write_all(b"BEAT ").map_err(serde_json::Error::io)?;
        serde_json::to_writer(&mut *w, self)?;
        w.write_all(b"\r\n").map_err(serde_json::Error::io)
    }
}

impl Heartbeat {
    pub fn new<S: ToString>(wid: S) -> Heartbeat {
        Heartbeat {
            wid: wid.to_string(),
        }
    }
}

// ----------------------------------------------

#[derive(Serialize, Clone)]
pub struct Fail {
    #[serde(rename = "jid")] job_id: String,
    #[serde(rename = "errtype")] kind: String,
    message: String,
    #[serde(skip_serializing_if = "Vec::is_empty")] backtrace: Vec<String>,
}

impl<'a> FaktoryCommand for &'a Fail {
    fn issue<W: Write>(&self, w: &mut Write) -> serde_json::Result<()> {
        w.write_all(b"FAIL ").map_err(serde_json::Error::io)?;
        serde_json::to_writer(&mut *w, self)?;
        w.write_all(b"\r\n").map_err(serde_json::Error::io)
    }
}

impl Fail {
    pub fn new<S1: ToString, S2: ToString, S3: ToString>(
        job_id: S1,
        kind: S2,
        message: S3,
    ) -> Self {
        Fail {
            job_id: job_id.to_string(),
            kind: kind.to_string(),
            message: message.to_string(),
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
    fn issue<W: Write>(&self, w: &mut Write) -> serde_json::Result<()> {
        w.write_all(b"END\r\n").map_err(serde_json::Error::io)
    }
}

// ----------------------------------------------

pub struct Fetch<'a, S>
where
    S: AsRef<str> + 'a,
{
    queues: &'a [S],
}

impl<'a, S> FaktoryCommand for Fetch<'a, S>
where
    S: AsRef<str> + 'a,
{
    fn issue<W: Write>(&self, w: &mut Write) -> serde_json::Result<()> {
        if self.queues.is_empty() {
            w.write_all(b"FETCH\r\n").map_err(serde_json::Error::io)?;
        } else {
            w.write_all(b"FETCH").map_err(serde_json::Error::io)?;
            for q in self.queues.into_iter() {
                w.write_all(b" ").map_err(serde_json::Error::io)?;
                w.write_all(q.as_ref().as_bytes())
                    .map_err(serde_json::Error::io)?;
            }
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
    #[serde(skip_serializing_if = "Option::is_none")] pub hostname: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")] pub wid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")] pub pid: Option<usize>,
    #[serde(skip_serializing_if = "Vec::is_empty")] pub labels: Vec<String>,

    #[serde(rename = "v")] version: usize,

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
            version: ::proto::EXPECTED_PROTOCOL_VERSION,
        }
    }
}

impl Hello {
    pub fn set_password(&mut self, hi: &super::resp::Hi, password: &str) {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::default();
        hasher.input(password.as_bytes());
        hasher.input(hi.salt.as_ref().unwrap().as_bytes());
        let mut hash = hasher.result();
        for _ in 1..hi.iterations.unwrap_or(1) {
            hash = Sha256::digest(&hash[..]);
        }
        self.password_hash = Some(format!("{:x}", hash));
    }
}

impl FaktoryCommand for Hello {
    fn issue<W: Write>(&self, w: &mut Write) -> serde_json::Result<()> {
        w.write_all(b"HELLO ").map_err(serde_json::Error::io)?;
        serde_json::to_writer(&mut *w, self)?;
        w.write_all(b"\r\n").map_err(serde_json::Error::io)
    }
}

// ----------------------------------------------

#[derive(Serialize, Deserialize, Debug)]
pub struct Failure {
    retry_count: usize,
    failed_at: String,
    #[serde(skip_serializing_if = "Option::is_none")] next_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")] message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "errtype")]
    kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")] backtrace: Option<Vec<String>>,
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
    fn issue<W: Write>(&self, w: &mut Write) -> serde_json::Result<()> {
        w.write_all(b"PUSH ").map_err(serde_json::Error::io)?;
        serde_json::to_writer(&mut *w, &**self)?;
        w.write_all(b"\r\n").map_err(serde_json::Error::io)
    }
}
