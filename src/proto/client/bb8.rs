use bb8::ManageConnection;
use super::{Client, HeartbeatStatus, utils::get_env_url};
use crate::{error::Protocol, Error};

pub type PooledClient = bb8::Pool<ClientConnectionManager>;

pub struct ClientConnectionManager {
    url: String,
}

impl ClientConnectionManager {
    pub fn new(url: &str) -> Self {
        Self {
            url: url.to_string()
        }
    }

    pub fn from_env() -> Self {
        Self {
            url: get_env_url()
        }
    }
}

impl ManageConnection for ClientConnectionManager {
    type Connection = Client;
    type Error = Error;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        Client::connect_to(self.url.as_str()).await
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        match conn.heartbeat().await? {
            HeartbeatStatus::Ok | HeartbeatStatus::Quiet => Ok(()),
            HeartbeatStatus::Terminate => Err(Error::Protocol(
                Protocol::Internal {
                    msg: "Connection terminated by server heartbeat".to_string(),
                }
            ))
        }
    }

    fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        false
    }
}