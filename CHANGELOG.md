# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

### Changed

- Update dependencies: `hostname`, `tokio-rustls`, `thiserror` ([#82])
- rustls: use `rustls-platform-verifier` to load certificates ([#82])

### Deprecated

### Removed

- rustls: dependency on `rustls-native-certs` ([#82])

### Fixed

### Security

[#82]: https://github.com/jonhoo/faktory-rs/pull/82

## [0.13.0] - 2024-10-27

### Added

- rustls, native_tls: `Error::Stream` for underlying `native_tls` and `rustls` errors ([#49])
- Shutdown signal via `WorkerBuilder::with_graceful_shutdown` ([#57])
- Shutdown timeout via `WorkerBuilder::shutdown_timeout` ([#57])
- `Client` method for pausing, resuming, and removing queues ([#59])
- `Client::current_info` and `FaktoryState` struct ([#63])
- rustls, native_tls: TLS configurations options to `WorkerBuilder` ([#74])

### Changed

- Made the bindings async (supporting sync job handlers though) ([#49])
- What used to be `Consumer` and `Producer` is now `Worker` and `Client`([#49])
- Instantiate `Client` with a more ergonomic `Client::connect` ([#79])
- `Client` now holds a `Box<dyn Connection>` for stream instead of generic `S` ([#64])
- Use new types for ids (`JobId` and `WorkerId`) instead of strings ([#49])
- Use optional `Duration` for `Job::reserve_for` instead of usize ([#79])
- `Worker::run` now returns a result with `StopDetails` ([#57])
- `Faktory` image version bumped from `1.8.0` to `1.9.1` ([#72], [#80])

[#49]: https://github.com/jonhoo/faktory-rs/pull/49
[#57]: https://github.com/jonhoo/faktory-rs/pull/57
[#59]: https://github.com/jonhoo/faktory-rs/pull/59
[#63]: https://github.com/jonhoo/faktory-rs/pull/63
[#64]: https://github.com/jonhoo/faktory-rs/pull/64
[#72]: https://github.com/jonhoo/faktory-rs/pull/72
[#74]: https://github.com/jonhoo/faktory-rs/pull/74
[#79]: https://github.com/jonhoo/faktory-rs/pull/79
[#80]: https://github.com/jonhoo/faktory-rs/pull/80

## [0.12.5] - 2024-02-18

### Added

- `JobRunner` trait and `ConsumerBuilder::register_runner` ([#51])
- Support for enqueuing numerous jobs with `Producer::enqueue_many` ([#54])
- ent: Batch jobs (`Batch`, `BatchId`, `BatchStatus`) ([#48])
- ent: Setting and getting a job's progress ([#48])

[#48]: https://github.com/jonhoo/faktory-rs/pull/48
[#51]: https://github.com/jonhoo/faktory-rs/pull/51
[#54]: https://github.com/jonhoo/faktory-rs/pull/54

[unreleased]: https://github.com/jonhoo/faktory-rs/compare/v0.13.0...HEAD
[0.13.0]: https://github.com/jonhoo/faktory-rs/compare/v0.12.5...v0.13.0
[0.12.5]: https://github.com/jonhoo/faktory-rs/compare/v0.12.4...v0.12.5
