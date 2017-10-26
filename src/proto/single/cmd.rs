use std::io::prelude::*;
use std::collections::HashMap;
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
pub struct Fail {
    #[serde(rename = "jid")] job_id: String,
    #[serde(rename = "errtype")] kind: String,
    message: String,
    #[serde(skip_serializing_if = "Vec::is_empty")] backtrace: Vec<String>,
}

impl FaktoryCommand for Fail {
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
    hostname: String,
    wid: String,
    pid: usize,
    labels: Vec<String>,

    /// Hash is hex(sha256(password + salt))
    #[serde(rename = "pwdhash")]
    #[serde(skip_serializing_if = "Option::is_none")]
    password_hash: Option<String>,
}

impl Hello {
    pub fn new<S1, S2, S3>(hostname: S1, wid: S2, pid: usize, labels: &[S3]) -> Self
    where
        S1: ToString,
        S2: ToString,
        S3: ToString,
    {
        Hello {
            hostname: hostname.to_string(),
            wid: wid.to_string(),
            pid,
            labels: labels.iter().map(|s| s.to_string()).collect(),
            password_hash: None,
        }
    }

    pub fn set_password(&mut self, salt: &str, password: &str) {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::default();
        hasher.input(password.as_bytes());
        hasher.input(salt.as_bytes());
        self.password_hash = Some(format!("{:x}", hasher.result()));
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

#[derive(Serialize, Deserialize, Debug)]
pub struct Job {
    pub jid: String,
    pub queue: String,
    #[serde(rename = "jobtype")] pub kind: String,
    pub args: Vec<String>,

    #[serde(skip_serializing_if = "Option::is_none")] pub created_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")] pub enqueued_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")] pub at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")] pub reserve_for: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")] pub retry: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")] pub backtrace: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")] pub failure: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub custom: Option<HashMap<String, serde_json::Value>>,
}

impl Job {
    pub fn new<S, A, AI>(kind: S, args: A) -> Self
    where
        S: ToString,
        AI: ToString,
        A: IntoIterator<Item = AI>,
    {
        use rand::{thread_rng, Rng};
        let random_jid = thread_rng().gen_ascii_chars().take(16).collect();
        use chrono::prelude::*;
        let now = Utc::now();
        Job {
            jid: random_jid,
            queue: "default".into(),
            kind: kind.to_string(),
            args: args.into_iter().map(|s| s.to_string()).collect(),

            created_at: Some(now.to_rfc3339()),
            enqueued_at: None,
            at: None,
            reserve_for: None,
            retry: Some(25),
            backtrace: None,
            failure: None,
            custom: None,
        }
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
    fn issue<W: Write>(&self, w: &mut Write) -> serde_json::Result<()> {
        w.write_all(b"PUSH ").map_err(serde_json::Error::io)?;
        serde_json::to_writer(&mut *w, &**self)?;
        w.write_all(b"\r\n").map_err(serde_json::Error::io)
    }
}
