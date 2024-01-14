//! This is `faktory::proto::single::resp` adjusted for asynchrony;
//! The original module hosts Redis RESP implementation and deserialization
//! utilites which are in the center of our conversation with the Faktory server.

use tokio::io::{AsyncBufReadExt, AsyncReadExt};

use crate::{
    error,
    proto::{bad, Hi, RawResponse},
    Error,
};

pub async fn read_ok<R: AsyncBufReadExt + Unpin>(r: R) -> Result<(), Error> {
    let rr = read(r).await?;
    if let RawResponse::String(ref s) = rr {
        if s == "OK" {
            return Ok(());
        }
    }

    Err(bad("server ok", &rr).into())
}

pub async fn read_json<R: AsyncBufReadExt + Unpin, T: serde::de::DeserializeOwned>(
    r: R,
) -> Result<Option<T>, Error> {
    let rr = read(r).await?;
    match rr {
        RawResponse::String(ref s) if s == "OK" => {
            return Ok(None);
        }
        RawResponse::String(ref s) => {
            return serde_json::from_str(s)
                .map(Some)
                .map_err(Error::Serialization);
        }
        RawResponse::Blob(ref b) if b == b"OK" => {
            return Ok(None);
        }
        RawResponse::Blob(ref b) => {
            if b.is_empty() {
                return Ok(None);
            }
            return serde_json::from_slice(b)
                .map(Some)
                .map_err(Error::Serialization);
        }
        RawResponse::Null => return Ok(None),
        _ => {}
    };

    Err(bad("json", &rr).into())
}

pub async fn read_hi<R: AsyncBufReadExt + Unpin>(r: R) -> Result<Hi, Error> {
    let rr = read(r).await?;
    if let RawResponse::String(ref s) = rr {
        if let Some(s) = s.strip_prefix("HI ") {
            return serde_json::from_str(s).map_err(Error::Serialization);
        }
    }
    Err(bad("server hi", &rr).into())
}

async fn read<R>(mut r: R) -> Result<RawResponse, Error>
where
    R: AsyncReadExt + AsyncBufReadExt + Unpin,
{
    let mut cmdbuf = [0u8; 1];
    r.read_exact(&mut cmdbuf).await?;
    match cmdbuf[0] {
        b'+' => {
            // Simple String
            // https://redis.io/topics/protocol#resp-simple-strings
            let mut s = String::new();
            r.read_line(&mut s).await?;

            // remove newlines
            let l = s.len() - 2;
            s.truncate(l);

            Ok(RawResponse::String(s))
        }
        b'-' => {
            // Error
            // https://redis.io/topics/protocol#resp-errors
            let mut s = String::new();
            r.read_line(&mut s).await?;

            // remove newlines
            let l = s.len() - 2;
            s.truncate(l);

            Err(error::Protocol::new(s).into())
        }
        b':' => {
            // Integer
            // https://redis.io/topics/protocol#resp-integers
            let mut s = String::with_capacity(32);
            r.read_line(&mut s).await?;

            // remove newlines
            let l = s.len() - 2;
            s.truncate(l);

            match (*s).parse::<isize>() {
                Ok(i) => Ok(RawResponse::Number(i)),
                Err(_) => Err(error::Protocol::BadResponse {
                    typed_as: "integer",
                    error: "invalid integer value",
                    bytes: s.into_bytes(),
                }
                .into()),
            }
        }
        b'$' => {
            // Bulk String
            // https://redis.io/topics/protocol#resp-bulk-strings
            let mut bytes = Vec::with_capacity(32);
            r.read_until(b'\n', &mut bytes).await?;
            let s = std::str::from_utf8(&bytes[0..bytes.len() - 2]).map_err(|_| {
                error::Protocol::BadResponse {
                    typed_as: "bulk string",
                    error: "server bulk response contains non-utf8 size prefix",
                    bytes: bytes[0..bytes.len() - 2].to_vec(),
                }
            })?;

            let size = s
                .parse::<isize>()
                .map_err(|_| error::Protocol::BadResponse {
                    typed_as: "bulk string",
                    error: "server bulk response size prefix is not an integer",
                    bytes: s.as_bytes().to_vec(),
                })?;

            if size == -1 {
                Ok(RawResponse::Null)
            } else {
                let size = size as usize;
                let mut bytes = vec![0; size];
                r.read_exact(&mut bytes[..]).await?;
                r.read_exact(&mut [0u8; 2]).await?;
                Ok(RawResponse::Blob(bytes))
            }
        }
        b'*' => {
            // Arrays
            // https://redis.io/topics/protocol#resp-arrays
            //
            // not used in faktory.
            // *and* you can't really skip them unless you parse them.
            // *and* not parsing them would leave the stream in an inconsistent state.
            // so we'll just give up
            unimplemented!();
        }
        c => Err(error::Protocol::BadResponse {
            typed_as: "unknown",
            error: "invalid response type prefix",
            bytes: vec![c],
        }
        .into()),
    }
}
