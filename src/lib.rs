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
//! `Client`, whereas the latter is done with a `Worker`. See the documentation for each for
//! more details on how to use them.
//!
//! # Encrypted connections (TLS)
//!
//! To connect to a Faktory server hosted over TLS, add the `tls` feature, and see the
//! documentation for `TlsStream`, which can be supplied to [`Client::connect_with`] and
//! [`WorkerBuilder::connect_with`].
//!
//! # Examples
//!
//! If you want to **submit** jobs to Faktory, use `Client`.
//!
//! ```no_run
//! # tokio_test::block_on(async {
//! use faktory::{Client, Job};
//! let mut client = Client::connect(None).await.unwrap();
//! client.enqueue(Job::new("foobar", vec!["z"])).await.unwrap();
//!
//! let (enqueued_count, errors) = client.enqueue_many([Job::new("foobar", vec!["z"]), Job::new("foobar", vec!["z"])]).await.unwrap();
//! assert_eq!(enqueued_count, 2);
//! assert_eq!(errors, None);
//! });
//! ```
//! If you want to **accept** jobs from Faktory, use `Worker`.
//!
//! ```no_run
//! # tokio_test::block_on(async {
//! use faktory::WorkerBuilder;
//! use std::io;
//! let mut w = WorkerBuilder::default();
//! w.register("foobar", |job| async move {
//!     println!("{:?}", job);
//!     Ok::<(), io::Error>(())
//! });
//! let mut w = w.connect(None).await.unwrap();
//! if let Err(e) = w.run(&["default"], None).await {
//!     println!("worker failed: {}", e);
//! }
//! # });
//! ```
#![deny(missing_docs)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(rust_2018_idioms)]

#[macro_use]
extern crate serde_derive;

pub mod error;

mod proto;
mod worker;

pub use crate::error::Error;
pub use crate::proto::{Client, Job, JobBuilder, JobId, Reconnect, WorkerId};
pub use crate::worker::{JobRunner, Worker, WorkerBuilder};

#[cfg(feature = "ent")]
#[cfg_attr(docsrs, doc(cfg(feature = "ent")))]
/// Constructs only available with the enterprise version of Faktory.
pub mod ent {
    pub use crate::proto::{
        Batch, BatchBuilder, BatchHandle, BatchId, BatchStatus, CallbackState, JobState, Progress,
        ProgressUpdate, ProgressUpdateBuilder,
    };
}

#[cfg(feature = "tls")]
mod tls;

#[cfg(feature = "tls")]
#[cfg_attr(docsrs, doc(cfg(feature = "tls")))]
pub use tls::TlsStream;

// 3rd-party libs re-exports
pub use async_trait::async_trait;
pub use tokio::main;
#[cfg(feature = "tls")]
#[cfg_attr(docsrs, doc(cfg(feature = "tls")))]
pub use tokio_rustls::rustls;
