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
//! # Encrypted connections (TLS)
//!
//! To connect to a Faktory server hosted over TLS, add the `tls` feature, and see the
//! documentation for `TlsStream`, which can be supplied to `Producer::connect_with` and
//! `Consumer::connect_with`.
//!
//! # Examples
//!
//! If you want to **submit** jobs to Faktory, use `Producer`.
//!
//! ```no_run
//! use faktory::{Producer, Job};
//! let mut p = Producer::connect(None).unwrap();
//! p.enqueue(Job::new("foobar", vec!["z"])).unwrap();
//!
//! let (enqueued_count, errors) = p.enqueue_many(vec![Job::new("foobar", vec!["z"]), Job::new("foobar", vec!["z"])]).unwrap();
//! assert_eq!(enqueued_count, 2);
//! assert_eq!(errors, None);
//! ```
//!
//! If you want to **accept** jobs from Faktory, use `Consumer`.
//!
//! ```no_run
//! use faktory::ConsumerBuilder;
//! use std::io;
//! let mut c = ConsumerBuilder::default();
//! c.register("foobar", |job| -> io::Result<()> {
//!     println!("{:?}", job);
//!     Ok(())
//! });
//! let mut c = c.connect(None).unwrap();
//! if let Err(e) = c.run(&["default"]) {
//!     println!("worker failed: {}", e);
//! }
//! ```
#![deny(missing_docs)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(rust_2018_idioms)]

#[macro_use]
extern crate serde_derive;

pub mod error;

mod consumer;
mod producer;
mod proto;

pub use crate::consumer::{Consumer, ConsumerBuilder, JobRunner};
pub use crate::error::Error;
pub use crate::producer::Producer;
pub use crate::proto::{Client, Job, JobBuilder, Reconnect};

#[cfg(feature = "ent")]
#[cfg_attr(docsrs, doc(cfg(feature = "ent")))]
/// Constructs only available with the enterprise version of Faktory.
pub mod ent {
    pub use crate::proto::{
        Batch, BatchBuilder, BatchHandle, BatchStatus, CallbackState, JobState, Progress,
        ProgressUpdate, ProgressUpdateBuilder,
    };
}

#[cfg(feature = "tls")]
#[cfg_attr(docsrs, doc(cfg(feature = "tls")))]
mod tls;

#[cfg(feature = "tls")]
#[cfg_attr(docsrs, doc(cfg(feature = "tls")))]
pub use tls::TlsStream;
