# slotbus-hub

**HTTP-to-shared-memory router for same-machine microservices.** Workers register routes via HTTP. Clients send normal HTTP requests. The hub dispatches them through shared memory with sub-millisecond round trips. Multiple workers behind a single port, zero HTTP overhead on the hot path.

[![CI](https://github.com/JustMaier/slotbus-hub/actions/workflows/ci.yml/badge.svg)](https://github.com/JustMaier/slotbus-hub/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/slotbus-hub.svg)](https://crates.io/crates/slotbus-hub)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE-MIT)

Built on [slotbus](https://github.com/JustMaier/slotbus), a lock-free shared memory IPC library for Rust.

## Why slotbus-hub?

You have services running on the same machine that communicate over HTTP. Each request pays for TCP handshakes, socket copies, HTTP parsing, and JSON serialization — overhead that adds 5-20ms per round trip to what should be a local function call.

slotbus-hub replaces that hot path with shared memory. From the outside, it looks like a normal HTTP server. On the inside, requests are dispatched to workers through memory-mapped slots with **0.1-0.8ms round trips** — 10-50x faster than localhost HTTP.

- **Drop-in HTTP gateway** — clients don't know they're using shared memory
- **Dynamic route registration** — workers register routes at startup, no config files
- **Multiple workers** — route different paths to different worker processes
- **SSE event stream** — workers push events, clients subscribe via `/events`
- **Sub-millisecond dispatch** — the entire SHM round trip takes 0.1-0.8ms

## Installation

```bash
cargo install slotbus-hub
```

## Quick Start

```bash
# Start the hub on the default port (3200)
slotbus-hub

# Custom port and timeout
slotbus-hub --port 8080 --timeout 60

# With latency instrumentation
slotbus-hub --instrumentation
```

### CLI Flags

| Flag | Default | Description |
|---|---|---|
| `--port` | `3200` | HTTP listen port |
| `--bind` | `127.0.0.1` | IP address to bind (env: `HUB_BIND`). Any non-loopback value requires `HUB_SECRET` — see [Network Exposure](#network-exposure). |
| `--timeout` | `30` | Request timeout in seconds. Returns `504 Gateway Timeout` if a worker doesn't respond in time. |
| `--slots` | `32` | Number of SHM slots per worker. More slots = more concurrent in-flight requests. |
| `--region-size` | `1048576` | SHM region size per worker in bytes (default 1 MB). See the sizing note below — this interacts with `--slots`. |
| `--instrumentation` | `false` | Enable latency instrumentation logging via `tracing`. |

Logging is controlled by the `RUST_LOG` environment variable (default: `info`).

### Sizing `--region-size` against `--slots`

slotbus 0.2 gives every slot a fixed arena, split into a request half and a
response half, so the largest payload that stays inline is roughly:

```
region_size / slots / 2
```

Raising `--slots` therefore *lowers* the inline ceiling unless you raise
`--region-size` with it. At the defaults (32 slots, 1 MB) the ceiling is ~16 KB
per direction; at 64 slots on the same region it is ~8 KB.

Payloads above the ceiling spill to an overflow region. That path is correct and
supported, but releasing an overflow mapping costs on the order of tens of
milliseconds — enough to dominate a round trip that is otherwise sub-millisecond.
If your responses are consistently larger than the ceiling, raise the region
rather than paying that on every request. A worker handling ~76 KB responses at
64 slots wants roughly a 16 MB region.

## Network Exposure

By default the hub binds `127.0.0.1` and is unauthenticated, which is safe
because nothing off-box can reach it. To serve clients on other machines, bind a
reachable address and set a shared secret:

```bash
HUB_SECRET='<long random string>' slotbus-hub --bind 0.0.0.0
```

The rules:

- **Loopback peers are exempt.** Requests from `127.0.0.0/8` or `::1` never need
  a header, so existing local clients keep working unchanged.
- **Everyone else must send `X-Hub-Secret`** matching `HUB_SECRET`, or the
  request gets `401`. This covers every route, including `/health`, the
  `/internal/*` control plane, both SSE streams, and all worker-registered paths
  served through the fallback proxy.
- **The peer address comes from the TCP connection**, never from a header.
  `X-Forwarded-For` and friends are ignored — put the hub behind a reverse proxy
  only if you also terminate auth there.
- **Startup fails closed.** A non-loopback `--bind` with `HUB_SECRET` unset,
  empty, or whitespace-only refuses to start rather than exposing an open API.
- **Secrets must be at least 16 characters** after trimming, on any bind. A
  shorter one is a config error and the hub refuses to start.

Surrounding whitespace is stripped from `HUB_SECRET`, because HTTP strips
whitespace around header values in transit — an untrimmed secret could never be
sent intact. On Windows, `--bind ::` is IPv6-only and will not accept IPv4
connections, so local clients using `127.0.0.1` would break; use `0.0.0.0` or a
specific address instead.

Secrets are compared in constant time. Bind to the narrowest address that works
(a specific LAN IP beats `0.0.0.0`) and firewall the port to your local subnet.

## How It Works

```
  Client                      Hub                         Worker
    |                          |                            |
    |  HTTP GET /api/items/42  |                            |
    | -----------------------> |                            |
    |                          |  match route, find worker  |
    |                          |  write to SHM slot         |
    |                          |  signal req event          |
    |                          | -------------------------> |
    |                          |                            |  claim slot
    |                          |                            |  read request
    |                          |                            |  process...
    |                          |                            |  write response
    |                          |        signal rsp event    |  to SHM slot
    |                          | <------------------------- |
    |                          |  read response from slot   |
    |   HTTP 200 OK            |                            |
    | <----------------------- |                            |
    |                          |                            |
    |       total: 0.1-0.8ms   |                            |
```

1. **Client** sends a standard HTTP request to the hub.
2. **Hub** matches the path against registered worker routes.
3. **Hub** serializes the request metadata (path, headers, query params, path params) and body into a shared memory slot.
4. **Hub** signals the worker via an OS-level named event (sub-microsecond wake).
5. **Worker** claims the slot, processes the request, writes the response back to shared memory.
6. **Worker** signals the hub. The hub reads the response and returns it to the client as a normal HTTP response.

The entire SHM round trip takes 0.1-0.8 ms. See the [slotbus](https://github.com/JustMaier/slotbus) library for details on the shared memory transport, slot state machine, and overflow handling.

## Worker Registration

Workers register their routes on startup via `POST /internal/register`:

```bash
curl -X POST http://localhost:3200/internal/register \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "my-worker",
    "routes": [
      {"method": "GET", "path": "/api/status"},
      {"method": "POST", "path": "/api/process"},
      {"method": "GET", "path": "/api/items/{id}"}
    ]
  }'
```

Response:

```json
{
  "worker_id": "550e8400-e29b-41d4-a716-446655440000",
  "shm_name": "hub-my-worker"
}
```

The hub creates a shared memory region (`hub-my-worker`) and named events for the worker. The worker opens the region using the returned `shm_name` and starts its receive loop via the `slotbus` crate.

### Path Parameters

Routes support `{param}` placeholders. When `GET /api/items/42` matches the pattern `/api/items/{id}`, the hub extracts `{"id": "42"}` and includes it in the request metadata dispatched to the worker.

### Stale Worker Cleanup

When a worker re-registers with the same `name`, the hub automatically removes the old registration and creates a fresh SHM region. This handles worker restarts without manual cleanup.

## SSE Event Stream

The hub provides a unified Server-Sent Events stream at `GET /events`. Workers push events via `POST /internal/emit`, and the hub multiplexes them into a single stream tagged with the worker's `source` field.

```bash
# Subscribe to all worker events
curl -N http://localhost:3200/events
```

Workers emit events like this:

```bash
curl -X POST http://localhost:3200/internal/emit \
  -H 'Content-Type: application/json' \
  -d '{
    "source": "my-worker",
    "event_type": "task-complete",
    "data": "{\"task_id\": 123}"
  }'
```

The SSE stream uses keep-alive to maintain the connection. Events arrive as standard SSE with the `event` field set to `event_type` and the `data` field containing the full JSON payload including the `source`.

## Health Check

```bash
curl http://localhost:3200/health
```

```json
{
  "ok": true,
  "workers": [
    {
      "name": "my-worker",
      "worker_id": "550e8400-...",
      "route_count": 3,
      "transport": "shm:hub-my-worker"
    }
  ]
}
```

## API Reference

| Endpoint | Method | Description |
|---|---|---|
| `/internal/register` | `POST` | Register a worker with name and routes. Returns `worker_id` and `shm_name`. |
| `/internal/emit` | `POST` | Push an event from a worker for SSE broadcast. |
| `/events` | `GET` | Unified SSE stream of all worker events. |
| `/health` | `GET` | List connected workers and their route counts. |
| `*` (all other paths) | any | Proxied to the matching worker via SHM. Returns `502` if no route matches, `504` on timeout. |

## Building Workers

The hub is just the router. To build a worker that connects to it, use the [slotbus](https://github.com/JustMaier/slotbus) library crate:

```toml
[dependencies]
slotbus = "0.2"
```

The versions must match. slotbus 0.2 changed the shared-memory wire protocol
(`SHM_VERSION` 1 → 2), so a 0.1 worker and a 0.2 hub cannot share a region — the
mismatch is rejected at startup with `bad version: expected 2, got 1`.

A minimal worker: register routes with the hub via HTTP, open the SHM region using the returned `shm_name`, and start the receive loop. See the [slotbus README](https://github.com/JustMaier/slotbus#worker-side--connect-and-handle-requests) for the worker-side API.

## Developing against a local slotbus

`slotbus` resolves from crates.io, so `git clone && cargo build` works with
nothing else on disk. To build against a sibling checkout of the
[slotbus](https://github.com/JustMaier/slotbus) repo instead — when changing
both crates together — create `.cargo/config.toml` (gitignored):

```toml
[patch.crates-io]
slotbus = { path = "../slotbus/slotbus" }
```

The patch applies to the whole workspace, including `slotbus-ffi`. Delete the
file to go back to the published crate.

## Platform Support

slotbus-hub inherits platform support from the slotbus library:

| Platform | Status |
|---|---|
| **Windows** | Supported |
| **Linux** | Supported |
| **macOS** | Supported |

## Minimum Supported Rust Version

1.75

## License

Licensed under the [MIT License](LICENSE-MIT).
