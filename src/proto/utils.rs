use crate::error::{self, Error};
use url::Url;

pub(crate) fn get_env_url() -> String {
    use std::env;
    let var = env::var("FAKTORY_PROVIDER").unwrap_or_else(|_| "FAKTORY_URL".to_string());
    env::var(var).unwrap_or_else(|_| "tcp://localhost:7419".to_string())
}

pub(crate) fn host_from_url(url: &Url) -> String {
    format!("{}:{}", url.host_str().unwrap(), url.port().unwrap_or(7419))
}

pub(crate) fn url_parse(url: &str) -> Result<Url, Error> {
    let url = Url::parse(url).map_err(error::Connect::ParseUrl)?;
    if url.scheme() != "tcp" {
        return Err(error::Connect::BadScheme {
            scheme: url.scheme().to_string(),
        }
        .into());
    }

    if url.host_str().is_none() || url.host_str().unwrap().is_empty() {
        return Err(error::Connect::MissingHostname.into());
    }

    Ok(url)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn correct_env_parsing() {
        use std::env;

        if env::var_os("FAKTORY_URL").is_some() {
            eprintln!("skipping test to avoid messing with user-set FAKTORY_URL");
            return;
        }

        assert_eq!(get_env_url(), "tcp://localhost:7419");

        env::set_var("FAKTORY_URL", "tcp://example.com:7500");
        assert_eq!(get_env_url(), "tcp://example.com:7500");

        env::set_var("FAKTORY_PROVIDER", "URL");
        env::set_var("URL", "tcp://example.com:7501");
        assert_eq!(get_env_url(), "tcp://example.com:7501");
    }

    #[test]
    fn url_port_default() {
        use url::Url;
        let url = Url::parse("tcp://example.com").unwrap();
        assert_eq!(host_from_url(&url), "example.com:7419");
    }

    #[test]
    fn url_requires_tcp() {
        url_parse("foobar").unwrap_err();
    }

    #[test]
    fn url_requires_host() {
        url_parse("tcp://:7419").unwrap_err();
    }

    #[test]
    fn url_doesnt_require_port() {
        url_parse("tcp://example.com").unwrap();
    }

    #[test]
    fn url_can_take_password_and_port() {
        url_parse("tcp://:foobar@example.com:7419").unwrap();
    }
}
