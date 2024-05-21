# faktory-rs

[![Crates.io](https://img.shields.io/crates/v/faktory.svg)](https://crates.io/crates/faktory)
[![Documentation](https://docs.rs/faktory/badge.svg)](https://docs.rs/faktory/)
[![Codecov](https://codecov.io/github/jonhoo/faktory-rs/coverage.svg?branch=master)](https://codecov.io/gh/jonhoo/faktory-rs)
[![dependency status](https://deps.rs/repo/github/jonhoo/faktory-rs/status.svg)](https://deps.rs/repo/github/jonhoo/faktory-rs)

API bindings for Faktory workers and job producers.

This crate provides API bindings for the language-agnostic
[Faktory](https://github.com/contribsys/faktory) work server. For a more detailed system
overview of the work server, what jobs are, and how they are scheduled, see the Faktory docs.

## System overview

At a high level, Faktory has two primary concepts: jobs and workers. Jobs are pieces of work
that clients want to have executed, and workers are the things that eventually execute those
jobs. A client enqueues a job, Faktory sends the job to an available worker (and waits if
they're all busy), the worker executes the job, and eventually reports back to Faktory that the
job has completed.

Jobs are self-contained, and consist of a job _type_ (a string), arguments for the job, and
bits and pieces of metadata. When a job is scheduled for execution, the worker is given this
information, and uses the job type to figure out how to execute the job. You can think of job
execution as a remote function call (or RPC) where the job type is the name of the function,
and the job arguments are, perhaps unsuprisingly, the arguments to the function.

In this crate, you will find bindings both for submitting jobs (clients that _produce_ jobs)
and for executing jobs (workers that _consume_ jobs). The former can be done by making a
`Client`, whereas the latter is done with a `Worker`. See the documentation for each for
more details on how to use them.

## Encrypted connections (TLS)

To connect to a Faktory server hosted over TLS, add the `tls` feature, and see the
documentation for `TlsStream`, which can be supplied to `Client::connect_with` and
`WorkerBuilder::connect_with`.

## Examples

If you want to **submit** jobs to Faktory, use `Client`.

```rust
use faktory::{Client, Job};
let mut c = Client::connect(None).await.unwrap();
c.enqueue(Job::new("foobar", vec!["z"])).await.unwrap();
```

If you want to **accept** jobs from Faktory, use `Worker`.

```rust
use faktory::WorkerBuilder;
use std::io;
let mut w = WorkerBuilder::default();
w.register("foobar", |job| async move {
     println!("{:?}", job);
     Ok::<(), io::Error>(())
});
let mut w = w.connect(None).await.unwrap();
if let Err(e) = w.run(&["default"]).await {
    println!("worker failed: {}", e);
}
```

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

Provided you have [make](https://www.gnu.org/software/make/#download) installed and `docker` daemon running,
you can launch a `Faktory` container with `make faktory` command. After that, hit `make test/e2e` to run the end-to-end test suite.
Remove the container with `make faktory/kill`, if it's no longer needed.

To run end-to-end tests for the crate's `tls` feature, ensure you've got the [`compose`](https://docs.docker.com/compose/install/) docker plugin installed.
Run `make faktory/tls` to spin up `Faktory` behind `NGINX` with ssl termination, then run `make test/e2e/tls`. To remove the containers, hit `make faktory/tls/kill`.
