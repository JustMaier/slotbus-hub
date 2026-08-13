# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.1] - 2026-08-12

Both fixes are in the same place: what happens to a worker's shared-memory
region when that worker restarts and a replacement registers under the same
name. Nothing here changes the wire protocol, so 0.2.1 drops into a 0.2.0
deployment without rebuilding workers.

### Fixed

- **Requests hung for the full request timeout across a worker restart.**
  Two distinct causes, both from a replacement re-creating a region the
  previous generation was still attached to.

  Requests already parked on the old worker's region had nothing to wake
  them. Removing the worker record does not: a request clones the
  `Arc<SlotBus>` before dispatching and holds it while it waits, and the bus
  owns the oneshot sender it waits on — so the waiter keeps alive the very
  channel whose closure would have released it. Each worker record now
  carries a `gone` notification, fired on both removal paths, which the
  request selects on alongside the response. It is subscribed under the
  workers read lock — eviction needs the write lock — so a departure cannot
  land between lookup and subscribe.

  More seriously, the old bus's response-watcher thread kept scanning a
  region the new bus had re-created. Two watchers then shared one region, and
  whichever won the `Done → Free` transition consumed the response; the stale
  one has no pending entry for that request id, so it discarded the payload.
  The new worker appeared to answer nothing and the caller waited out the
  timeout. Eviction now stops the old bus before a replacement can share its
  region.

  Measured with a worker restarted under a request fired every 125ms across
  the 3s transition window, 25 requests per restart: before, 2 of 6 restarts
  stalled with six consecutive 30s requests each; after, 8 of 8 restarts
  clean over 200 requests, worst case 1.11s — and that worst case is a prompt
  502 rather than a hang.

- **SSE replay gave up partway through on workers with many connections.**
  On re-registration the hub re-points a worker's stale SSE connections at
  the new worker id and notifies it one connection at a time. Each
  notification waited up to 5s for an acknowledgement against a 30s budget
  for the whole batch, so past roughly a dozen connections the loop hit its
  deadline and logged `skipping remaining` — leaving the rest of the
  connections attached to a worker id that no longer existed.

### Documentation

- README documents the slotbus 0.2 pairing and how to size regions.

## [0.2.0] - 2026-08-01

**Requires slotbus 0.2.** That release breaks the shared-memory wire protocol
(`SHM_VERSION` 1 → 2), so the hub and every worker sharing a region must be
rebuilt together. A mismatched peer is rejected at startup with
`bad version: expected 2, got 1` rather than failing later on a large payload.

### Security

- **Reject worker names that could steer a filesystem path.**
  `POST /internal/register` passed the caller-supplied `name` straight into the
  region name, which slotbus 0.2 uses to locate a backing file it may delete
  when reclaiming an orphan. A name containing `..` or a path separator could
  therefore reach a file outside the backing directory. Names containing path
  separators, `..`, `:` or NUL are now rejected with 400. slotbus 0.2 enforces
  the same restriction independently; this is the outer of two layers.
  Loopback registration is auth-exempt, so the exposure was to non-loopback
  peers authenticated with `X-Hub-Secret`.

### Changed

- `slotbus` now resolves from crates.io rather than a sibling path dependency,
  so the repository builds from a plain clone. To develop against a local
  slotbus checkout, see "Developing against a local slotbus" in the README.
- LAN binding and shared-secret auth: `--bind` accepts a non-loopback address
  only when `HUB_SECRET` is set, and non-loopback peers must present
  `X-Hub-Secret`. Loopback is exempt, so local clients are unaffected.

### Fixed

- SSE connections are swept for liveness, and a disconnect now logs how long
  the connection had been open.

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

[Unreleased]: https://github.com/JustMaier/slotbus-hub/compare/v0.2.1...HEAD
[0.2.1]: https://github.com/JustMaier/slotbus-hub/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/JustMaier/slotbus-hub/compare/v0.1.3...v0.2.0
[0.1.3]: https://github.com/JustMaier/slotbus-hub/compare/v0.1.0...v0.1.3
[0.1.0]: https://github.com/JustMaier/slotbus-hub/releases/tag/v0.1.0
