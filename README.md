# faktory

[![Crates.io](https://img.shields.io/crates/v/faktory.svg)](https://crates.io/crates/faktory)
[![Documentation](https://docs.rs/faktory/badge.svg)](https://docs.rs/faktory/)
[![Build Status](https://travis-ci.org/jonhoo/faktory-rs.svg?branch=master)](https://travis-ci.org/jonhoo/faktory-rs)

API bindings for Faktory workers and job producers.

This crate provides API bindings for the language-agnostic
[Faktory](https://github.com/contribsys/faktory) work server.

## Producing jobs

If you want to **submit** jobs to Faktory, use `Producer`.

```rust
use std::net::TcpStream;
let mut p = Producer::<TcpStream>::connect_env().unwrap();
p.enqueue(Job::new("foobar", vec!["z"])).unwrap();
```

## Consuming jobs (i.e., workers)

If you want to **accept** jobs from Faktory, use `Consumer`.

```rust
use std::io;
use std::net::TcpStream;
let mut c = ConsumerBuilder::default().connect_env::<TcpStream, _>().unwrap();
c.register("foobar", |job| -> io::Result<()> {
    println!("{:?}", job);
    Ok(())
});
if let Err(e) = c.run(&["default"]) {
    println!("worker failed: {}", e);
}
```
