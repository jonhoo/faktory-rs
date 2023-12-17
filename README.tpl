# {{crate}}-rs

[![Crates.io](https://img.shields.io/crates/v/faktory.svg)](https://crates.io/crates/faktory)
[![Documentation](https://docs.rs/faktory/badge.svg)](https://docs.rs/faktory/)
[![Codecov](https://codecov.io/github/jonhoo/faktory-rs/coverage.svg?branch=master)](https://codecov.io/gh/jonhoo/faktory-rs)
[![dependency status](https://deps.rs/repo/github/jonhoo/faktory-rs/status.svg)](https://deps.rs/repo/github/jonhoo/faktory-rs)

{{readme}}

## Run test suite locally

First ensure the "Factory" service is running and accepting connections on your machine. 
To launch it a [Factory](https://hub.docker.com/r/contribsys/faktory/) container with [docker](https://docs.docker.com/engine/install/), run:
```bash
docker run --rm -it -v faktory-data:/var/lib/faktory -p 127.0.0.1:7419:7419 -p 127.0.0.1:7420:7420 contribsys/faktory:latest /faktory -b :7419 -w :7420
```
After that run the tests:
```bash
FAKTORY_URL=tcp://127.0.0.1:7419 cargo test --all-features --locked --all-targets
```
Please note that setting "FAKTORY_URL" environment variable is required for e2e tests to not be skipped.
