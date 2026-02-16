# slotbus-hub

**HTTP-to-SHM router for [slotbus](https://github.com/jzipperle/slotbus) workers.**

[![Crates.io](https://img.shields.io/crates/v/slotbus-hub.svg)](https://crates.io/crates/slotbus-hub)
[![docs.rs](https://img.shields.io/docsrs/slotbus-hub)](https://docs.rs/slotbus-hub)
[![CI](https://github.com/jzipperle/slotbus-hub/actions/workflows/ci.yml/badge.svg)](https://github.com/jzipperle/slotbus-hub/actions/workflows/ci.yml)
[![License](https://img.shields.io/crates/l/slotbus-hub.svg)](LICENSE-MIT)

Workers register routes via HTTP. Clients send normal HTTP requests. The hub dispatches them through shared memory with sub-millisecond round trips and returns the response. Multiple workers behind a single port, no HTTP overhead on the hot path.

## Installation

```bash
cargo install slotbus-hub
```

## Usage

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
| `--timeout` | `30` | Request timeout in seconds. Returns `504 Gateway Timeout` if a worker doesn't respond in time. |
| `--slots` | `32` | Number of SHM slots per worker. More slots = more concurrent in-flight requests. |
| `--region-size` | `1048576` | SHM region size per worker in bytes (default 1 MB). |
| `--instrumentation` | `false` | Enable latency instrumentation logging via `tracing`. |

Logging is controlled by the `RUST_LOG` environment variable (default: `info`).

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

The entire SHM round trip takes 0.1--0.8 ms. See the [slotbus](https://github.com/jzipperle/slotbus) library for details on the shared memory transport, slot state machine, and overflow handling.

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

The hub is just the router. To build a worker that connects to it, use the [slotbus](https://github.com/jzipperle/slotbus) library crate:

```toml
[dependencies]
slotbus = "0.1"
```

A minimal worker: register routes with the hub via HTTP, open the SHM region using the returned `shm_name`, and start the receive loop. See the [slotbus README](https://github.com/jzipperle/slotbus#worker-side--connect-and-handle-requests) for the worker-side API.

## Platform Support

slotbus-hub inherits platform support from the slotbus library:

| Platform | Status |
|---|---|
| **Windows** | Supported |
| **Linux** | Planned |
| **macOS** | Planned |

## Minimum Supported Rust Version

1.75

## License

Licensed under either of

- [Apache License, Version 2.0](LICENSE-APACHE)
- [MIT License](LICENSE-MIT)

at your option.
