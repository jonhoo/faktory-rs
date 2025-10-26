use bb8::ManageConnection;
use super::{Client, utils::get_env_url};
use crate::Error;

/// A BB8 connection pool for Faktory clients.
pub type PooledClient = bb8::Pool<ClientConnectionManager>;

/// A connection manager for Faktory clients to be used with BB8.
pub struct ClientConnectionManager {
    url: String,
}

impl ClientConnectionManager {
    /// Create a new connection manager for the given URL.
    pub fn new(url: &str) -> Self {
        Self {
            url: url.to_string()
        }
    }

    /// Create a new connection manager using the URL from the `FAKTORY_PROVIDER`
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
        conn.current_info().await?;
        Ok(())
    }

    fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        false
    }
}