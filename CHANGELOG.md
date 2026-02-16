# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-02-16

### Added

- HTTP-to-SHM proxy router with dynamic worker registration
- Worker route matching with path parameter extraction
- SSE event multiplexing across all connected workers
- Health endpoint with worker status reporting
- Stale worker detection and automatic cleanup on re-register
- CLI with configurable port and request timeout
- CORS enabled for browser clients

[Unreleased]: https://github.com/jzipperle/slotbus-hub/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/jzipperle/slotbus-hub/releases/tag/v0.1.0
