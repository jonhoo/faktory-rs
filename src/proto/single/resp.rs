use std::io::prelude::*;
use serde_json;
use serde;
use std::io;
use std;

fn bad(expected: &str, got: &RawResponse) -> io::Error {
    use std;
    let stringy = match got {
        &RawResponse::String(ref s) => Some(&**s),
        &RawResponse::Blob(ref b) => if let Ok(s) = std::str::from_utf8(b) {
            Some(s)
        } else {
            None
        },
        _ => None,
    };

    match stringy {
        Some(s) => io::Error::new(
            io::ErrorKind::InvalidData,
            format!("expected {}, got '{}'", expected, s),
        ),
        None => io::Error::new(
            io::ErrorKind::InvalidData,
            format!("expected {}, got {:?}", expected, got),
        ),
    }
}

// ----------------------------------------------

pub fn read_json<R: BufRead, T: serde::de::DeserializeOwned>(
    r: R,
) -> serde_json::Result<Option<T>> {
    let rr = read(r).map_err(serde_json::Error::io)?;
    match rr {
        RawResponse::String(ref s) if s == "OK" => {
            return Ok(None);
        }
        RawResponse::String(ref s) => {
            return serde_json::from_str(s).map(|v| Some(v));
        }
        RawResponse::Blob(ref b) if b == b"OK" => {
            return Ok(None);
        }
        RawResponse::Blob(ref b) => {
            if b.is_empty() {
                return Ok(None);
            }
            return serde_json::from_slice(b).map(|v| Some(v));
        }
        RawResponse::Null => return Ok(None),
        _ => {}
    };

    Err(serde_json::Error::io(bad("json", &rr)))
}

// ----------------------------------------------

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Hi {
    #[serde(rename = "v")] pub version: usize,
    #[serde(rename = "i")] pub iterations: Option<usize>,
    #[serde(rename = "s")] pub salt: Option<String>,
}

pub fn read_hi<R: BufRead>(r: R) -> io::Result<Hi> {
    let rr = read(r)?;
    if let RawResponse::String(ref s) = rr {
        if s.starts_with("HI ") {
            return Ok(serde_json::from_str(&s[3..])?);
        }
    }

    Err(bad("server hi", &rr))
}

// ----------------------------------------------

pub fn read_ok<R: BufRead>(r: R) -> io::Result<()> {
    let rr = read(r)?;
    if let RawResponse::String(ref s) = rr {
        if s == "OK" {
            return Ok(());
        }
    }

    Err(bad("server ok", &rr))
}

// ----------------------------------------------
//
// below is the implementation of the Redis RESP protocol
//
// ----------------------------------------------

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
enum RawResponse {
    String(String),
    Blob(Vec<u8>),
    Number(isize),
    Null,
}

fn read<R: BufRead>(mut r: R) -> io::Result<RawResponse> {
    let mut cmdbuf = [0u8; 1];
    r.read_exact(&mut cmdbuf)?;
    match cmdbuf[0] {
        b'+' => {
            // Simple String
            // https://redis.io/topics/protocol#resp-simple-strings
            let mut s = String::new();
            r.read_line(&mut s)?;

            // remove newlines
            let l = s.len() - 2;
            s.truncate(l);

            Ok(RawResponse::String(s))
        }
        b'-' => {
            // Error
            // https://redis.io/topics/protocol#resp-errors
            let mut s = String::new();
            r.read_line(&mut s)?;

            // remove newlines
            let l = s.len() - 2;
            s.truncate(l);

            Err(io::Error::new(io::ErrorKind::InvalidInput, s))
        }
        b':' => {
            // Integer
            // https://redis.io/topics/protocol#resp-integers
            let mut s = String::with_capacity(32);
            r.read_line(&mut s)?;

            // remove newlines
            let l = s.len() - 2;
            s.truncate(l);

            match isize::from_str_radix(&*s, 10) {
                Ok(i) => Ok(RawResponse::Number(i)),
                Err(_) => Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Invalid integer response: {}", s),
                )),
            }
        }
        b'$' => {
            // Bulk String
            // https://redis.io/topics/protocol#resp-bulk-strings
            let mut bytes = Vec::with_capacity(32);
            r.read_until(b'\n', &mut bytes)?;
            let s = std::str::from_utf8(&bytes[0..bytes.len() - 2]).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "server bulk response contains non-utf8 size prefix",
                )
            })?;

            let size = isize::from_str_radix(s, 10).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "server buld response size prefix is not an integer: '{}'",
                        s
                    ),
                )
            })?;

            if size == -1 {
                Ok(RawResponse::Null)
            } else {
                let size = size as usize;
                let mut bytes = Vec::with_capacity(size);
                bytes.resize(size, 0u8);
                r.read_exact(&mut bytes[..])?;
                r.read_exact(&mut [0u8; 2])?;
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
        c => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Unexpected leading response byte: '{}'", c),
        )),
    }
}

// these are mostly for convenience for testing

impl<'a> From<&'a str> for RawResponse {
    fn from(s: &'a str) -> Self {
        RawResponse::String(s.to_string())
    }
}

impl From<isize> for RawResponse {
    fn from(i: isize) -> Self {
        RawResponse::Number(i)
    }
}

impl From<Vec<u8>> for RawResponse {
    fn from(b: Vec<u8>) -> Self {
        RawResponse::Blob(b)
    }
}

#[cfg(test)]
mod test {
    use std::io::{self, Cursor};
    use super::{read, RawResponse};
    use serde_json::{self, Map, Value};

    fn read_json<C: io::BufRead>(c: C) -> serde_json::Result<Option<Value>> {
        super::read_json(c)
    }

    #[test]
    fn it_parses_simple_strings() {
        let c = Cursor::new(b"+OK\r\n");
        assert_eq!(read(c).unwrap(), RawResponse::from("OK"));
    }

    #[test]
    fn it_parses_numbers() {
        let c = Cursor::new(b":1024\r\n");
        assert_eq!(read(c).unwrap(), RawResponse::from(1024));
    }

    #[test]
    fn it_errors_on_bad_numbers() {
        let c = Cursor::new(b":x\r\n");
        let r = read(c).unwrap_err();
        assert_eq!(r.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn it_parses_errors() {
        let c = Cursor::new(b"-ERR\r\n");
        let r = read(c).unwrap_err();
        assert_eq!(r.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(format!("{}", r.into_inner().unwrap()), "ERR");
    }

    #[test]
    #[should_panic]
    fn it_cant_do_arrays() {
        let c = Cursor::new(b"*\r\n");
        read(c).is_err();
    }

    #[test]
    fn it_parses_nills() {
        let c = Cursor::new(b"$-1\r\n");
        assert_eq!(read(c).unwrap(), RawResponse::Null);
    }

    #[test]
    fn it_errors_on_bad_sizes() {
        let c = Cursor::new(b"$x\r\n\r\n");
        let r = read(c).unwrap_err();
        assert_eq!(r.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn it_parses_empty_bulk() {
        let c = Cursor::new(b"$0\r\n\r\n");
        assert_eq!(read(c).unwrap(), RawResponse::from(vec![]));
    }

    #[test]
    fn it_parses_non_empty_bulk() {
        let c = Cursor::new(b"$11\r\nHELLO WORLD\r\n");
        assert_eq!(
            read(c).unwrap(),
            RawResponse::from(Vec::from(&b"HELLO WORLD"[..]))
        );
    }

    #[test]
    fn it_decodes_json_ok_string() {
        let c = Cursor::new(b"+OK\r\n");
        assert_eq!(read_json(c).unwrap(), None);
    }

    #[test]
    fn it_decodes_json_ok_blob() {
        let c = Cursor::new(b"$2\r\nOK\r\n");
        assert_eq!(read_json(c).unwrap(), None);
    }

    #[test]
    fn it_decodes_json_nill() {
        let c = Cursor::new(b"$-1\r\n");
        assert_eq!(read_json(c).unwrap(), None);
    }

    #[test]
    fn it_decodes_json_empty() {
        let c = Cursor::new(b"$0\r\n\r\n");
        assert_eq!(read_json(c).unwrap(), None);
    }

    #[test]
    fn it_decodes_string_json() {
        let c = Cursor::new(b"+{\"hello\":1}\r\n");
        let mut m = Map::new();
        m.insert("hello".to_string(), Value::from(1));
        assert_eq!(read_json(c).unwrap(), Some(Value::Object(m)));
    }

    #[test]
    fn it_decodes_blob_json() {
        let c = Cursor::new(b"$11\r\n{\"hello\":1}\r\n");
        let mut m = Map::new();
        m.insert("hello".to_string(), Value::from(1));
        assert_eq!(read_json(c).unwrap(), Some(Value::Object(m)));
    }

    #[test]
    fn it_errors_on_bad_json_blob() {
        let c = Cursor::new(b"$9\r\n{\"hello\"}\r\n");
        let r: io::Error = read_json(c).unwrap_err().into();
        assert_eq!(r.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn it_errors_on_bad_json_string() {
        let c = Cursor::new(b"+{\"hello\"}\r\n");
        let r: io::Error = read_json(c).unwrap_err().into();
        assert_eq!(r.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn json_error_on_number() {
        let c = Cursor::new(b":9\r\n");
        let r: io::Error = read_json(c).unwrap_err().into();
        assert_eq!(r.kind(), io::ErrorKind::InvalidData);
    }

}
