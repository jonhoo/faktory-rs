use crate::error::Error;
use crate::proto::{Job, JobId, WorkerId};
use std::error::Error as StdError;
use tokio::io::AsyncWriteExt;

#[async_trait::async_trait]
pub trait FaktoryCommand {
    async fn issue<W: AsyncWriteExt + Unpin + Send>(&self, w: &mut W) -> Result<(), Error>;
}

macro_rules! self_to_cmd {
    ($struct:ident, $cmd:expr) => {
        #[async_trait::async_trait]
        impl FaktoryCommand for $struct {
            async fn issue<W: AsyncWriteExt + Unpin + Send>(&self, w: &mut W) -> Result<(), Error> {
                let c = format!("{} ", $cmd);
                w.write_all(c.as_bytes()).await?;
                let r = serde_json::to_vec(self).map_err(Error::Serialization)?;
                w.write_all(&r).await?;
                Ok(w.write_all(b"\r\n").await?)
            }
        }
    };
}

/// Write queues as part of a command. They are written with a leading space
/// followed by space separated queue names.
async fn write_queues<W, S>(w: &mut W, queues: &[S]) -> Result<(), Error>
where
    W: AsyncWriteExt + Unpin + Send,
    S: AsRef<str>,
{
    for q in queues {
        w.write_all(b" ").await?;
        w.write_all(q.as_ref().as_bytes()).await?;
    }

    Ok(())
}

// -------------------- INFO ----------------------

pub struct Info;

#[async_trait::async_trait]
impl FaktoryCommand for Info {
    async fn issue<W: AsyncWriteExt + Unpin + Send>(&self, w: &mut W) -> Result<(), Error> {
        Ok(w.write_all(b"INFO\r\n").await?)
    }
}

// -------------------- ACK ----------------------

#[derive(Serialize)]
pub struct Ack {
    #[serde(rename = "jid")]
    job_id: JobId,
}

impl From<&JobId> for Ack {
    fn from(job_id: &JobId) -> Self {
        Ack {
            job_id: job_id.to_owned(),
        }
    }
}

self_to_cmd!(Ack, "ACK");

// -------------------- BEAT ------------------

#[derive(Serialize)]
pub struct Heartbeat {
    wid: WorkerId,
}

impl Heartbeat {
    pub fn new<S: Into<WorkerId>>(wid: S) -> Heartbeat {
        Heartbeat { wid: wid.into() }
    }
}

self_to_cmd!(Heartbeat, "BEAT");

// -------------------- FAIL ---------------------

#[derive(Serialize, Clone)]
pub struct Fail {
    #[serde(rename = "jid")]
    job_id: JobId,
    #[serde(rename = "errtype")]
    kind: String,
    message: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    backtrace: Vec<String>,
}

impl Fail {
    pub fn new(job_id: JobId, kind: impl Into<String>, message: impl Into<String>) -> Self {
        Fail {
            job_id,
            kind: kind.into(),
            message: message.into(),
            backtrace: Vec::new(),
        }
    }

    // "unknown" is the errtype used by the go library too
    pub fn generic<S: Into<String>>(job_id: JobId, message: S) -> Self {
        Fail::new(job_id, "unknown", message)
    }

    pub fn set_backtrace(&mut self, lines: Vec<String>) {
        self.backtrace = lines;
    }

    pub fn generic_with_backtrace<E>(jid: JobId, e: E) -> Self
    where
        E: StdError,
    {
        let mut f = Fail::generic(jid, format!("{}", e));
        let mut root = e.source();
        let mut lines = Vec::new();
        while let Some(r) = root.take() {
            lines.push(format!("{}", r));
            root = r.source();
        }
        f.set_backtrace(lines);
        f
    }
}

self_to_cmd!(Fail, "FAIL");

// ---------------------- END --------------------

pub struct End;

#[async_trait::async_trait]
impl FaktoryCommand for End {
    async fn issue<W: AsyncWriteExt + Unpin + Send>(&self, w: &mut W) -> Result<(), Error> {
        Ok(w.write_all(b"END\r\n").await?)
    }
}

// --------------------- FETCH --------------------

pub struct Fetch<'a, S>
where
    S: AsRef<str>,
{
    pub(crate) queues: &'a [S],
}

#[async_trait::async_trait]
impl<'a, Q> FaktoryCommand for Fetch<'a, Q>
where
    Q: AsRef<str> + Sync,
{
    async fn issue<W: AsyncWriteExt + Unpin + Send>(&self, w: &mut W) -> Result<(), Error> {
        w.write_all(b"FETCH").await?;
        write_queues(w, self.queues).await?;
        Ok(w.write_all(b"\r\n").await?)
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

// --------------------- HELLO --------------------

#[derive(Serialize)]
pub struct Hello {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hostname: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wid: Option<WorkerId>,
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

self_to_cmd!(Hello, "HELLO");

// --------------------- PUSH --------------------

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

#[async_trait::async_trait]
impl FaktoryCommand for Push {
    async fn issue<W: AsyncWriteExt + Unpin + Send>(&self, w: &mut W) -> Result<(), Error> {
        w.write_all(b"PUSH ").await?;
        let r = serde_json::to_vec(&**self).map_err(Error::Serialization)?;
        w.write_all(&r).await?;
        Ok(w.write_all(b"\r\n").await?)
    }
}

// ---------------------- QUEUE -------------------

pub struct PushBulk(Vec<Job>);

impl From<Vec<Job>> for PushBulk {
    fn from(jobs: Vec<Job>) -> Self {
        PushBulk(jobs)
    }
}

#[async_trait::async_trait]
impl FaktoryCommand for PushBulk {
    async fn issue<W: AsyncWriteExt + Unpin + Send>(&self, w: &mut W) -> Result<(), Error> {
        w.write_all(b"PUSHB ").await?;
        let r = serde_json::to_vec(&self.0).map_err(Error::Serialization)?;
        w.write_all(&r).await?;
        Ok(w.write_all(b"\r\n").await?)
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

#[async_trait::async_trait]
impl<Q> FaktoryCommand for QueueControl<'_, Q>
where
    Q: AsRef<str> + Sync,
{
    async fn issue<W: AsyncWriteExt + Unpin + Send>(&self, w: &mut W) -> Result<(), Error> {
        let command = match self.action {
            QueueAction::Pause => b"QUEUE PAUSE".as_ref(),
            QueueAction::Resume => b"QUEUE RESUME".as_ref(),
        };
        w.write_all(command).await?;
        write_queues(w, self.queues).await?;
        Ok(w.write_all(b"\r\n").await?)
    }
}
impl<'a, S: AsRef<str>> QueueControl<'a, S> {
    pub fn new(action: QueueAction, queues: &'a [S]) -> Self {
        Self { action, queues }
    }
}
