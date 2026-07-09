# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.3] - 2026-07-09

### Fixed

- **Worker: use-after-free on hub reconnect.** Request handlers run as
  detached tasks; on reconnect the worker replaced its shared-memory region
  while in-flight response tasks could still be writing into the old one —
  an access violation (0xC0000005) in `write_response`. The worker now tracks
  in-flight response tasks and drains them (bounded 5s) before tearing down
  the region and reconnecting.

### Changed

- Minimum `slotbus` requirement raised to 0.1.3, which fixes an overflow-region
  size-collision crash and adds bounds-checked heap access (see the
  [slotbus 0.1.3 changelog](https://github.com/JustMaier/slotbus/blob/main/CHANGELOG.md))

## [0.1.0] - 2026-02-16

### Added

- HTTP-to-SHM proxy router with dynamic worker registration
- Worker route matching with path parameter extraction
- SSE event multiplexing across all connected workers
- Health endpoint with worker status reporting
- Stale worker detection and automatic cleanup on re-register
- CLI with configurable port and request timeout
- CORS enabled for browser clients

[Unreleased]: https://github.com/JustMaier/slotbus-hub/compare/v0.1.3...HEAD
[0.1.3]: https://github.com/JustMaier/slotbus-hub/compare/v0.1.0...v0.1.3
[0.1.0]: https://github.com/JustMaier/slotbus-hub/releases/tag/v0.1.0
