//! API bindings for Faktory workers and job producers.
//!
//! This crate provides API bindings for the language-agnostic
//! [Faktory](https://github.com/contribsys/faktory) work server. For a more detailed system
//! overview of the work server, what jobs are, and how they are scheduled, see the Faktory docs.
//!
//! # System overview
//!
//! At a high level, Faktory has two primary concepts: jobs and workers. Jobs are pieces of work
//! that clients want to have executed, and workers are the things that eventually execute those
//! jobs. A client enqueues a job, Faktory sends the job to an available worker (and waits if
//! they're all busy), the worker executes the job, and eventually reports back to Faktory that the
//! job has completed.
//!
//! Jobs are self-contained, and consist of a job *type* (a string), arguments for the job, and
//! bits and pieces of metadata. When a job is scheduled for execution, the worker is given this
//! information, and uses the job type to figure out how to execute the job. You can think of job
//! execution as a remote function call (or RPC) where the job type is the name of the function,
//! and the job arguments are, perhaps unsuprisingly, the arguments to the function.
//!
//! In this crate, you will find bindings both for submitting jobs (clients that *produce* jobs)
//! and for executing jobs (workers that *consume* jobs). The former can be done by making a
//! `Producer`, whereas the latter is done with a `Consumer`. See the documentation for each for
//! more details on how to use them.
//!
//! # Examples
//!
//! If you want to **submit** jobs to Faktory, use `Producer`.
//!
//! ```no_run
//! use faktory::{Producer, Job, TcpEstablisher};
//! let mut p = Producer::connect_env(TcpEstablisher).unwrap();
//! p.enqueue(Job::new("foobar", vec!["z"])).unwrap();
//! ```
//!
//! If you want to **accept** jobs from Faktory, use `Consumer`.
//!
//! ```no_run
//! use faktory::{ConsumerBuilder, TcpEstablisher};
//! use std::io;
//! let mut c = ConsumerBuilder::default();
//! c.register("foobar", |job| -> io::Result<()> {
//!     println!("{:?}", job);
//!     Ok(())
//! });
//! let mut c = c.connect_env(TcpEstablisher).unwrap();
//! if let Err(e) = c.run(&["default"]) {
//!     println!("worker failed: {}", e);
//! }
//! ```
#![deny(missing_docs)]

extern crate atomic_option;
extern crate bufstream;
extern crate chrono;
extern crate fnv;
extern crate hostname;
extern crate libc;
extern crate native_tls;
extern crate rand;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate sha2;
extern crate url;

mod producer;
mod consumer;
mod proto;

pub use consumer::{Consumer, ConsumerBuilder};
pub use producer::Producer;
pub use proto::Job;
pub use proto::{FromUrl, StreamConnector, TcpEstablisher, TlsEstablisher};
