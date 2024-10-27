# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `Client::current_info` and `FaktoryState` struct
- `Client` method for pausing, resuming, and removing queues
- TLS configurations options to `WorkerBuilder`
- `Error::Stream` for underlying 'native_tls' and 'rustls' errors
- Shutdown signal via `WorkerBuilder::with_graceful_shutdown`
- Shutdown timeout via `WorkerBuilder::shutdown_timeout`
- [Faktory Enterprise Edition] Batch jobs (`Batch`, `BatchId`, `BatchStatus`)
- [Faktory Enterprise Edition] Setting and getting a job's progress

### Changed

- Made the bindings async (supporting sync job handlers though)
- What used to be `Consumer` and `Producer` is now `Worker` and `Client`
- Instantiate `Client` with a more ergonomic `Client::connect`
- `Client` now holds a `Box<dyn Connection>` for stream instead of generic `S`
- Use new types for ids (`JobId` and `WorkerId`) instead of strings
- Use optional `Duration` for `Job::reserve_for` instead of usize
- `Worker::run` now returns a result with `StopDetails`
- `Faktory` image version bumped from `1.8.0` to `1.9.1`


## [0.12.5] - 2024-02-18

### Added

- `JobRunner` trait and `ConsumerBuilder::register_runner`
- Support for enqueuing numerous jobs with `Producer::enqueue_many`
- [Faktory Enterprise Edition] Batch jobs (`Batch`, `BatchId`, `BatchStatus`)
- [Faktory Enterprise Edition] Setting and getting a job's progress


[unreleased]: https://github.com/jonhoo/faktory-rs/compare/ff2a27bd841b097c1db0208ad03d9363d89d21c1...release-0.13
[0.12.5]: https://github.com/jonhoo/faktory-rs/compare/b5ddd2130661ef8aa72790db3f2868706fc7408c...ff2a27bd841b097c1db0208ad03d9363d89d21c1
