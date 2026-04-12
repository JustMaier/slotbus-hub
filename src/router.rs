//! Hub router: registration, SHM request proxying, SSE delegation, health, events.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use tokio::sync::{broadcast, mpsc, RwLock};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tower_http::cors::CorsLayer;

use slotbus::types::RequestMeta;
use slotbus::{SlotBus, SlotBusConfig};

use crate::events;
use slotbus_hub::types::*;

// ---- Configuration -----------------------------------------------------------

/// Default SSE sweep interval.
///
/// axum's built-in `KeepAlive::default()` emits a comment frame every 15s.
/// We run an independent sweeper at a shorter interval that probes every
/// connection's `mpsc::Sender` with `try_send`. The sweeper detects two
/// things:
///   - Closed channels — axum already dropped the receiver (e.g., after a
///     keepalive write failed or the response finished cleanly) but nobody
///     has run cleanup yet. The sweeper cleans up and notifies the worker.
///   - Forced stream pumping — a successful `try_send` queues a real event
///     for the mapped stream to pull, which asks axum to write bytes to the
///     TCP socket. That write can surface a dead peer sooner than axum's
///     idle keepalive timer would on its own.
///
/// It doesn't eliminate dependence on axum's pipeline running at all — a
/// stream stuck before it sees the ping will still linger — but in practice
/// it shortens detection latency for the common failure modes.
pub const DEFAULT_SSE_SWEEP_INTERVAL_MS: u64 = 5_000;

/// Hub-level configuration (from CLI args).
pub struct HubConfig {
    pub timeout_secs: u64,
    pub num_slots: usize,
    pub region_size: usize,
    pub instrumentation: bool,
    /// How often the SSE liveness sweeper runs, in milliseconds. A value of
    /// `0` disables the sweeper entirely (falls back to axum's built-in
    /// `KeepAlive` + drop-guard detection).
    pub sse_sweep_interval_ms: u64,
}

// ---- SSE connection state ----------------------------------------------------

/// An SSE event pushed to a hub-managed stream.
pub struct SseEvent {
    pub event_type: String,
    pub data: String,
}

/// Info about an active hub-managed SSE connection.
pub struct SseConnectionInfo {
    pub connection_id: String,
    pub worker_id: String,
    pub path_pattern: String,
    pub params: HashMap<String, String>,
    pub sender: mpsc::Sender<SseEvent>,
    pub connected_at: String,
}

// ---- State -------------------------------------------------------------------

pub struct HubState {
    /// worker_id -> WorkerRecord
    workers: RwLock<HashMap<String, WorkerRecord>>,
    /// All registered routes: (method, pattern, worker_id, sse)
    route_table: RwLock<Vec<RouteEntry>>,
    /// Broadcast channel for unified event stream
    pub event_tx: broadcast::Sender<HubEvent>,
    /// Active SSE connections managed by the hub on behalf of workers.
    /// Key: full matched path (e.g. "/agent/events/abc-123")
    pub sse_connections: RwLock<HashMap<String, SseConnectionInfo>>,
    /// Hub configuration
    config: HubConfig,
}

struct WorkerRecord {
    name: String,
    routes: Vec<RouteRegistration>,
    bus: Arc<SlotBus>,
}

/// Internal route table entry.
struct RouteEntry {
    method: String,
    pattern: String,
    worker_id: String,
    sse: bool,
}

// ---- Build router ------------------------------------------------------------

pub fn build_router(config: HubConfig) -> Router {
    let (event_tx, _) = broadcast::channel::<HubEvent>(256);

    let sweep_interval_ms = config.sse_sweep_interval_ms;

    let state = Arc::new(HubState {
        workers: RwLock::new(HashMap::new()),
        route_table: RwLock::new(Vec::new()),
        event_tx,
        sse_connections: RwLock::new(HashMap::new()),
        config,
    });

    // Kick off the SSE liveness sweeper. Detects dead clients whose TCP
    // disconnect hasn't propagated through axum (notably idle Windows streams
    // that get force-killed).
    if sweep_interval_ms > 0 {
        spawn_sse_sweeper(Arc::clone(&state), Duration::from_millis(sweep_interval_ms));
    }

    Router::new()
        .route("/internal/register", post(register_worker))
        .route("/internal/emit", post(events::emit_event))
        .route("/internal/sse-push", post(events::sse_push))
        .route("/internal/slots", get(slot_diagnostics))
        .route("/events", get(events::unified_sse))
        .route("/events/{channel}", get(events::scoped_sse))
        .route("/health", get(health))
        .fallback(proxy_handler)
        .layer(CorsLayer::permissive())
        .with_state(state)
}

// ---- SSE liveness sweeper ----------------------------------------------------

/// Spawn a background task that probes every hub-managed SSE connection on
/// a fixed interval. When a probe reveals that the client's `mpsc` receiver
/// has been dropped, we route through the shared cleanup helper which
/// removes the map entry and dispatches `sse_lifecycle: "disconnect"` to the
/// worker.
///
/// Why this exists: axum's `KeepAlive` plus the drop-guard pattern is the
/// primary disconnect mechanism, but in practice a hard-killed client on
/// Windows can leave an idle stream stuck in a state where axum's 15s idle
/// keepalive timer doesn't surface a failed write quickly. The sweeper runs
/// at a shorter cadence and queues a real event for the mapped stream to
/// pull, which asks axum to write bytes to the socket sooner than it would
/// otherwise. It also catches channels whose receiver has already been
/// dropped but haven't been cleaned up yet.
fn spawn_sse_sweeper(state: Arc<HubState>, interval: Duration) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        // If a tick is delayed (e.g., the runtime was busy), skip the
        // backlog rather than fire N ticks back-to-back.
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // Skip the immediate first tick — nothing to sweep at startup.
        ticker.tick().await;

        tracing::info!(
            interval_ms = interval.as_millis() as u64,
            "SSE sweeper started"
        );

        loop {
            ticker.tick().await;
            sweep_once(&state).await;
        }
    });
}

/// Single sweep iteration. Separated so it can be called directly from
/// tests or debug tools.
async fn sweep_once(state: &Arc<HubState>) {
    // Snapshot (path, connection_id, sender) under a read lock so we release
    // the lock before doing any async work. We clone the Sender (cheap — it's
    // an `Arc` internally) so each probe attempt has its own handle, and we
    // capture the connection_id so `cleanup_sse_connection` can no-op if the
    // entry has been replaced by a new client on the same path between
    // probe and cleanup.
    let probes: Vec<(String, String, mpsc::Sender<SseEvent>)> = {
        let sse = state.sse_connections.read().await;
        sse.iter()
            .map(|(path, info)| {
                (
                    path.clone(),
                    info.connection_id.clone(),
                    info.sender.clone(),
                )
            })
            .collect()
    };

    if probes.is_empty() {
        return;
    }

    // Queue a liveness probe on each connection. `sse_ping` is a reserved
    // internal event type that clients are expected to ignore; it exists so
    // that the stream has traffic to drive the axum writer on otherwise
    // idle connections. A failed write on the underlying socket will abort
    // the stream, drop the receiver, and fire our drop guard — which is
    // handled by the normal cleanup path below.
    let mut closed: Vec<(String, String)> = Vec::new();
    for (path, connection_id, sender) in &probes {
        match sender.try_send(SseEvent {
            event_type: "sse_ping".to_string(),
            data: String::new(),
        }) {
            Ok(()) => {
                // Event queued. Axum's stream pipeline will pull it on its
                // next poll and attempt to write to the socket.
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                // Buffer is backpressured — receiver still exists but hasn't
                // drained 256 queued events. We don't treat this as dead;
                // if the client is genuinely stuck the OS-level TCP stack
                // will eventually surface the failure via the retransmit
                // timeout, at which point axum drops the stream and we'll
                // see Closed on the next sweep.
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                closed.push((path.clone(), connection_id.clone()));
            }
        }
    }

    if closed.is_empty() {
        return;
    }

    tracing::debug!(count = closed.len(), "SSE sweeper found closed channels");

    // Spawn cleanups concurrently. `dispatch_sse_lifecycle` can block on
    // SHM dispatch to a worker; if one worker is wedged we do NOT want that
    // to stall sweeps for every other worker's connections.
    for (path, connection_id) in closed {
        let state = Arc::clone(state);
        tokio::spawn(async move {
            cleanup_sse_connection(&state, &path, "sweeper", &connection_id).await;
        });
    }
}

// ---- POST /internal/register -------------------------------------------------

async fn register_worker(
    State(state): State<Arc<HubState>>,
    Json(req): Json<RegisterRequest>,
) -> impl IntoResponse {
    let worker_id = uuid::Uuid::new_v4().to_string();
    let route_count = req.routes.len();

    // Collect stale worker IDs for SSE replay
    let stale_ids: Vec<String>;

    // Remove stale workers with the same name
    {
        let mut workers = state.workers.write().await;
        stale_ids = workers
            .iter()
            .filter(|(_, record)| record.name == req.name)
            .map(|(id, _)| id.clone())
            .collect();

        if !stale_ids.is_empty() {
            let mut table = state.route_table.write().await;
            for stale_id in &stale_ids {
                workers.remove(stale_id);
                table.retain(|entry| entry.worker_id != *stale_id);
            }
            tracing::info!(
                name = req.name,
                count = stale_ids.len(),
                "removed stale worker(s)"
            );
        }
    }

    // Create SlotBus for this worker
    let config = SlotBusConfig::builder()
        .name(&req.name)
        .prefix("hub")
        .num_slots(state.config.num_slots)
        .region_size(state.config.region_size)
        .instrumentation(state.config.instrumentation)
        .build();

    let bus = match SlotBus::create(config) {
        Ok(bus) => Arc::new(bus),
        Err(e) => {
            tracing::error!(name = req.name, error = %e, "failed to create SlotBus");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to create SHM: {e}"),
            )
                .into_response();
        }
    };

    // Start response watcher
    bus.start_response_watcher();

    let shm_name = bus.region_name();

    // Add routes
    {
        let mut table = state.route_table.write().await;
        for route in &req.routes {
            table.push(RouteEntry {
                method: route.method.clone(),
                pattern: route.path.clone(),
                worker_id: worker_id.clone(),
                sse: route.sse,
            });
        }
    }

    // Store record
    {
        let mut workers = state.workers.write().await;
        workers.insert(
            worker_id.clone(),
            WorkerRecord {
                name: req.name.clone(),
                routes: req.routes,
                bus,
            },
        );
    }

    // ── SSE replay: re-associate stale connections with new worker ────────
    if !stale_ids.is_empty() {
        let replay = {
            let mut sse = state.sse_connections.write().await;
            let mut to_replay = Vec::new();

            for (path, info) in sse.iter_mut() {
                if stale_ids.contains(&info.worker_id) {
                    to_replay.push((
                        path.clone(),
                        info.params.clone(),
                        info.path_pattern.clone(),
                    ));
                    info.worker_id = worker_id.clone();
                }
            }

            to_replay
        };

        if !replay.is_empty() {
            let replay_count = replay.len();
            let replay_state = Arc::clone(&state);
            let replay_worker_id = worker_id.clone();
            let replay_name = req.name.clone();

            tracing::info!(
                name = req.name,
                worker_id,
                count = replay_count,
                "replaying SSE connections to new worker (background)"
            );

            // Spawn replay in the background so registration returns immediately.
            // Overall timeout prevents indefinite blocking if the worker is slow.
            tokio::spawn(async move {
                let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
                for (path, params, pattern) in replay {
                    if tokio::time::Instant::now() >= deadline {
                        tracing::warn!(
                            name = replay_name,
                            worker_id = replay_worker_id,
                            "SSE replay timed out (30s overall), skipping remaining"
                        );
                        break;
                    }
                    dispatch_sse_lifecycle(
                        &replay_state,
                        &replay_worker_id,
                        "connect",
                        &path,
                        &pattern,
                        &params,
                    )
                    .await;
                }
            });
        }
    }

    tracing::info!(
        name = req.name,
        worker_id,
        route_count,
        shm_name,
        "registered worker"
    );

    Json(RegisterResponse {
        worker_id,
        shm_name,
    })
    .into_response()
}

// ---- GET /health -------------------------------------------------------------

async fn health(State(state): State<Arc<HubState>>) -> impl IntoResponse {
    let workers = state.workers.read().await;
    let worker_info: Vec<WorkerInfo> = workers
        .iter()
        .map(|(id, record)| WorkerInfo {
            name: record.name.clone(),
            worker_id: id.clone(),
            route_count: record.routes.len(),
            transport: format!("shm:{}", record.bus.region_name()),
        })
        .collect();

    Json(HealthResponse {
        ok: true,
        workers: worker_info,
    })
}

// ---- GET /internal/slots -----------------------------------------------------

async fn slot_diagnostics(State(state): State<Arc<HubState>>) -> impl IntoResponse {
    let workers = state.workers.read().await;
    let mut entries = Vec::new();

    for record in workers.values() {
        let diag = record.bus.slot_diagnostics();
        let total = diag.len();
        let mut free = 0usize;
        let mut in_use = 0usize;
        let mut slots = Vec::with_capacity(total);

        for (index, raw) in &diag {
            let label = match *raw {
                0 => "free",
                1 => "ready",
                2 => "claimed",
                3 => "done",
                4 => "writing",
                _ => "unknown",
            };
            if *raw == 0 {
                free += 1;
            } else {
                in_use += 1;
            }
            slots.push(serde_json::json!({ "index": index, "state": label }));
        }

        entries.push(serde_json::json!({
            "name": record.name,
            "total_slots": total,
            "free": free,
            "in_use": in_use,
            "slots": slots,
        }));
    }

    Json(serde_json::json!({ "workers": entries }))
}

// ---- Fallback: proxy handler -------------------------------------------------

async fn proxy_handler(State(state): State<Arc<HubState>>, request: Request) -> impl IntoResponse {
    let method = request.method().to_string();
    let path = request.uri().path().to_string();
    let query = request.uri().query().map(|q| q.to_string());
    let headers: Vec<(String, String)> = request
        .headers()
        .iter()
        .filter_map(|(k, v)| {
            let key = k.as_str().to_string();
            let val = v.to_str().ok()?.to_string();
            Some((key, val))
        })
        .collect();

    // Read body
    let body_bytes = match axum::body::to_bytes(request.into_body(), 10 * 1024 * 1024).await {
        Ok(b) => b,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                format!("Failed to read request body: {e}"),
            )
                .into_response();
        }
    };

    // Find matching route
    let route_table = state.route_table.read().await;
    let matched = route_table
        .iter()
        .find(|entry| entry.method == method && match_route(&entry.pattern, &path).is_some());

    let (route_pattern, worker_id, is_sse) = match matched {
        Some(entry) => (entry.pattern.clone(), entry.worker_id.clone(), entry.sse),
        None => {
            return (
                StatusCode::BAD_GATEWAY,
                format!("No worker registered for {method} {path}"),
            )
                .into_response();
        }
    };
    drop(route_table);

    let path_params = match_route(&route_pattern, &path).unwrap_or_default();

    // ── SSE delegation: hub manages the connection ───────────────────────
    if is_sse {
        return handle_sse_delegation(state, worker_id, path, route_pattern, path_params)
            .await
            .into_response();
    }

    // ── Normal SHM proxy ─────────────────────────────────────────────────

    // Look up worker
    let workers = state.workers.read().await;
    let bus = match workers.get(&worker_id) {
        Some(record) => Arc::clone(&record.bus),
        None => {
            return (
                StatusCode::BAD_GATEWAY,
                format!("Worker {worker_id} not found"),
            )
                .into_response();
        }
    };
    drop(workers);

    // Build request metadata
    let meta = RequestMeta {
        path: path.clone(),
        route_pattern,
        path_params: path_params.into_iter().collect(),
        query,
        headers,
    };

    // Dispatch via SHM
    let req_id = uuid::Uuid::new_v4().to_string();
    let rx = match bus.dispatch(&req_id, &method, &meta, &body_bytes) {
        Ok(rx) => rx,
        Err(e) => {
            return (StatusCode::BAD_GATEWAY, format!("SHM dispatch failed: {e}")).into_response();
        }
    };

    // Wait for response
    let timeout = Duration::from_secs(state.config.timeout_secs);
    match tokio::time::timeout(timeout, rx).await {
        Ok(Ok(resp)) => {
            let status =
                StatusCode::from_u16(resp.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
            let mut response = axum::response::Response::builder().status(status);
            response = response.header("content-type", &resp.content_type);
            for (key, value) in &resp.headers {
                response = response.header(key.as_str(), value.as_str());
            }
            response
                .body(axum::body::Body::from(resp.body))
                .unwrap()
                .into_response()
        }
        Ok(Err(_)) => (
            StatusCode::BAD_GATEWAY,
            "Worker dropped the request".to_string(),
        )
            .into_response(),
        Err(_) => (
            StatusCode::GATEWAY_TIMEOUT,
            format!(
                "Request to {path} timed out after {}s",
                state.config.timeout_secs
            ),
        )
            .into_response(),
    }
}

// ---- SSE delegation ----------------------------------------------------------

/// Drop guard that cleans up SSE connection state and notifies the worker
/// when axum finishes serving the response (TCP close, keepalive write
/// failure, etc.). Uses `connection_id` to avoid removing a replacement
/// entry on the same path (race: Client A disconnects after Client B
/// already connected with the same path).
struct SseDropGuard {
    connection_id: String,
    path: String,
    state: Arc<HubState>,
}

impl Drop for SseDropGuard {
    fn drop(&mut self) {
        let connection_id = self.connection_id.clone();
        let path = self.path.clone();
        let state = Arc::clone(&self.state);

        tokio::spawn(async move {
            cleanup_sse_connection(&state, &path, "drop_guard", &connection_id).await;
        });
    }
}

/// Handle an SSE-delegated route: create the stream, store the sender,
/// notify the worker, and return the SSE response.
async fn handle_sse_delegation(
    state: Arc<HubState>,
    worker_id: String,
    path: String,
    route_pattern: String,
    path_params: HashMap<String, String>,
) -> impl IntoResponse {
    let (tx, rx) = mpsc::channel::<SseEvent>(256);
    let connection_id = uuid::Uuid::new_v4().to_string();

    // Generate ISO timestamp
    let connected_at = {
        use std::time::SystemTime;
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default();
        let secs = now.as_secs();
        // Simple ISO-ish timestamp (good enough for diagnostics)
        format!("{}Z", secs)
    };

    // Store the connection
    {
        let mut sse = state.sse_connections.write().await;
        sse.insert(
            path.clone(),
            SseConnectionInfo {
                connection_id: connection_id.clone(),
                worker_id: worker_id.clone(),
                path_pattern: route_pattern.clone(),
                params: path_params.clone(),
                sender: tx,
                connected_at,
            },
        );
    }

    tracing::info!(path, worker_id, connection_id, "SSE client connected (delegated)");

    // Notify worker of the connection via SHM
    dispatch_sse_lifecycle(&state, &worker_id, "connect", &path, &route_pattern, &path_params)
        .await;

    // Build the SSE stream with a drop guard for disconnect detection.
    // The guard only needs the connection_id + path + state; `cleanup_sse_connection`
    // looks up worker_id/pattern/params from the live map entry so we don't drift
    // if something reassigns the entry behind us.
    let drop_guard = SseDropGuard {
        connection_id,
        path: path.clone(),
        state: Arc::clone(&state),
    };

    let stream = ReceiverStream::new(rx).map(move |sse_event| {
        // Keep drop_guard alive as long as the stream is alive.
        let _ = &drop_guard;
        Ok::<_, std::convert::Infallible>(
            Event::default()
                .event(&sse_event.event_type)
                .data(sse_event.data),
        )
    });

    Sse::new(stream).keep_alive(KeepAlive::default())
}

/// Remove an SSE connection from the active map and notify the worker of
/// the disconnect via SHM. Single place where all dead-client cleanup goes:
///   - `SseDropGuard::drop` (axum detected TCP failure via keepalive or data write)
///   - `sse_push` (a worker push returned TrySendError::Closed)
///   - `sweep_once` (periodic liveness probe returned TrySendError::Closed)
///
/// `reason` is a short tag threaded into the log line for diagnostics — it
/// does NOT get sent to the worker, which only receives the standard
/// `sse_lifecycle: "disconnect"` notification regardless of how we detected
/// the dead client.
///
/// `expected_connection_id` is mandatory. Every caller captures the id of
/// the specific connection they observed dying, and we only remove the map
/// entry if its `connection_id` still matches. This is load-bearing: the
/// map is keyed by path, and a new client can take the same path between
/// the moment we observed the old one dying and the moment we run cleanup.
/// Without the id check we'd evict the replacement and dispatch a spurious
/// disconnect for it.
pub(crate) async fn cleanup_sse_connection(
    state: &Arc<HubState>,
    path: &str,
    reason: &'static str,
    expected_connection_id: &str,
) {
    // Remove the entry from the map and capture what we need for the SHM
    // dispatch. Bail out early if the entry is already gone (double cleanup)
    // or belongs to a newer connection (race with a replacement on the same
    // path). The compare-and-swap runs under the write lock so no one can
    // slip a replacement in between our check and the remove.
    let (worker_id, path_pattern, params) = {
        let mut sse = state.sse_connections.write().await;
        let info = match sse.get(path) {
            Some(info) => info,
            None => return,
        };
        if info.connection_id != expected_connection_id {
            tracing::debug!(
                path,
                old_conn = expected_connection_id,
                current_conn = %info.connection_id,
                reason,
                "SSE cleanup skipped: entry belongs to a newer connection"
            );
            return;
        }
        let snapshot = (
            info.worker_id.clone(),
            info.path_pattern.clone(),
            info.params.clone(),
        );
        sse.remove(path);
        snapshot
    };

    tracing::info!(path, worker_id, reason, "SSE client disconnected");

    dispatch_sse_lifecycle(
        state,
        &worker_id,
        "disconnect",
        path,
        &path_pattern,
        &params,
    )
    .await;
}

/// Dispatch an SSE lifecycle notification (connect/disconnect) to a worker via SHM.
async fn dispatch_sse_lifecycle(
    state: &HubState,
    worker_id: &str,
    lifecycle: &str,
    path: &str,
    route_pattern: &str,
    params: &HashMap<String, String>,
) {
    let workers = state.workers.read().await;
    let bus = match workers.get(worker_id) {
        Some(record) => Arc::clone(&record.bus),
        None => {
            tracing::warn!(
                worker_id,
                lifecycle,
                path,
                "cannot send SSE lifecycle: worker not found"
            );
            return;
        }
    };
    drop(workers);

    let body = SseLifecycle {
        sse_lifecycle: lifecycle.to_string(),
        params: params.clone(),
    };
    let body_bytes = serde_json::to_vec(&body).unwrap_or_default();

    let meta = RequestMeta {
        path: path.to_string(),
        route_pattern: route_pattern.to_string(),
        path_params: params.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
        query: None,
        headers: Vec::new(),
    };

    let req_id = uuid::Uuid::new_v4().to_string();
    // Use GET to match the registered SSE route method. The worker handler
    // detects lifecycle events by checking for `sse_lifecycle` in the body.
    match bus.dispatch(&req_id, "GET", &meta, &body_bytes) {
        Ok(rx) => {
            // For connect: wait briefly for initial response (worker can acknowledge).
            // For disconnect: fire-and-forget.
            if lifecycle == "connect" {
                let timeout = Duration::from_secs(5);
                match tokio::time::timeout(timeout, rx).await {
                    Ok(Ok(resp)) => {
                        tracing::debug!(
                            path,
                            status = resp.status,
                            "SSE connect acknowledged by worker"
                        );
                    }
                    Ok(Err(_)) => {
                        tracing::warn!(path, "Worker dropped SSE connect notification");
                    }
                    Err(_) => {
                        tracing::warn!(path, "SSE connect notification timed out (5s)");
                    }
                }
            } else {
                // Fire-and-forget for disconnect: just drop the rx
                drop(rx);
            }
        }
        Err(e) => {
            tracing::warn!(
                path,
                lifecycle,
                error = %e,
                "failed to dispatch SSE lifecycle via SHM"
            );
        }
    }
}

// ---- Route matching ----------------------------------------------------------

fn match_route(pattern: &str, path: &str) -> Option<HashMap<String, String>> {
    let pattern_parts: Vec<&str> = pattern.split('/').collect();
    let path_parts: Vec<&str> = path.split('/').collect();
    if pattern_parts.len() != path_parts.len() {
        return None;
    }
    let mut params = HashMap::new();
    for (pat, actual) in pattern_parts.iter().zip(path_parts.iter()) {
        if pat.starts_with('{') && pat.ends_with('}') {
            params.insert(pat[1..pat.len() - 1].to_string(), actual.to_string());
        } else if pat != actual {
            return None;
        }
    }
    Some(params)
}

/// Resolve an SSE connection path from either an exact path or pattern + params.
pub fn resolve_sse_path(
    path: Option<&str>,
    pattern: Option<&str>,
    params: Option<&HashMap<String, String>>,
) -> Option<String> {
    if let Some(p) = path {
        return Some(p.to_string());
    }
    if let (Some(pat), Some(par)) = (pattern, params) {
        let mut resolved = pat.to_string();
        for (key, value) in par {
            resolved = resolved.replace(&format!("{{{key}}}"), value);
        }
        // Verify all placeholders were resolved
        if !resolved.contains('{') {
            return Some(resolved);
        }
    }
    None
}

// ---- Tests -------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal HubState with empty worker / route tables. Tests that
    /// only exercise SSE cleanup never hit the SHM dispatch path — when our
    /// cleanup calls `dispatch_sse_lifecycle` with a worker_id that isn't in
    /// `state.workers`, `dispatch_sse_lifecycle` logs a warning and returns
    /// without trying to touch SHM. That's exactly what we want here.
    fn make_state() -> Arc<HubState> {
        let (event_tx, _rx) = broadcast::channel::<HubEvent>(16);
        Arc::new(HubState {
            workers: RwLock::new(HashMap::new()),
            route_table: RwLock::new(Vec::new()),
            event_tx,
            sse_connections: RwLock::new(HashMap::new()),
            config: HubConfig {
                timeout_secs: 30,
                num_slots: 32,
                region_size: 1024 * 1024,
                instrumentation: false,
                // 0 disables the background sweeper — tests invoke sweep_once
                // directly to keep behavior deterministic.
                sse_sweep_interval_ms: 0,
            },
        })
    }

    /// Insert a dummy SSE connection and return the receiver so the test can
    /// decide when to drop it.
    async fn insert_conn(
        state: &HubState,
        path: &str,
        connection_id: &str,
    ) -> mpsc::Receiver<SseEvent> {
        let (tx, rx) = mpsc::channel::<SseEvent>(16);
        let mut sse = state.sse_connections.write().await;
        sse.insert(
            path.to_string(),
            SseConnectionInfo {
                connection_id: connection_id.to_string(),
                worker_id: "test-worker".to_string(),
                path_pattern: path.to_string(),
                params: HashMap::new(),
                sender: tx,
                connected_at: "0Z".to_string(),
            },
        );
        rx
    }

    async fn assert_present(state: &HubState, path: &str) {
        let sse = state.sse_connections.read().await;
        assert!(
            sse.contains_key(path),
            "expected entry present for {path}, but was missing"
        );
    }

    async fn assert_missing(state: &HubState, path: &str) {
        let sse = state.sse_connections.read().await;
        assert!(
            !sse.contains_key(path),
            "expected entry absent for {path}, but it was present"
        );
    }

    /// Wait a short, generous interval for any cleanup tasks spawned by
    /// `sweep_once` to complete. We can't deterministically await spawned
    /// tasks, so this sleep is the pragmatic substitute; 200ms is plenty
    /// for a hash-map mutation under light load.
    async fn drain_spawned_cleanup() {
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    #[tokio::test]
    async fn cleanup_with_matching_connection_id_removes_entry() {
        let state = make_state();
        let path = "/agent/events/session-a";
        let _rx = insert_conn(&state, path, "conn-1").await;

        cleanup_sse_connection(&state, path, "test", "conn-1").await;

        assert_missing(&state, path).await;
    }

    #[tokio::test]
    async fn cleanup_with_mismatched_connection_id_preserves_entry() {
        // The race this protects against:
        //   1. Client A is registered with connection_id "A".
        //   2. Client A's stream dies; somebody queues cleanup with id "A".
        //   3. Before the cleanup runs, Client B connects on the SAME path
        //      and replaces the map entry (connection_id "B").
        //   4. Cleanup runs with id "A" — we must NOT evict Client B.
        let state = make_state();
        let path = "/agent/events/session-collision";

        // Client B is the current holder of this path.
        let _rx = insert_conn(&state, path, "conn-B").await;

        // Stale cleanup request from Client A.
        cleanup_sse_connection(&state, path, "test", "conn-A").await;

        // Client B's entry is still there.
        assert_present(&state, path).await;
        let sse = state.sse_connections.read().await;
        assert_eq!(sse.get(path).unwrap().connection_id, "conn-B");
    }

    #[tokio::test]
    async fn cleanup_on_missing_path_is_noop() {
        let state = make_state();
        // Never inserted — cleanup must not panic or otherwise misbehave.
        cleanup_sse_connection(&state, "/nothing/here", "test", "conn-X").await;
        let sse = state.sse_connections.read().await;
        assert!(sse.is_empty());
    }

    #[tokio::test]
    async fn sweep_removes_closed_channel() {
        let state = make_state();
        let path = "/agent/events/dead-client";

        // Insert a connection and immediately drop the receiver, simulating
        // axum having torn down the stream after a keepalive write failed.
        let rx = insert_conn(&state, path, "conn-dead").await;
        drop(rx);

        sweep_once(&state).await;
        drain_spawned_cleanup().await;

        assert_missing(&state, path).await;
    }

    #[tokio::test]
    async fn sweep_keeps_live_channel() {
        let state = make_state();
        let path = "/agent/events/live-client";

        // Hold onto the receiver so the channel stays open.
        let _rx = insert_conn(&state, path, "conn-live").await;

        sweep_once(&state).await;
        drain_spawned_cleanup().await;

        assert_present(&state, path).await;
    }

    #[tokio::test]
    async fn sweep_ignores_full_buffer_as_alive() {
        // A client whose receiver is still alive but whose buffer is full
        // must not be treated as dead. We force that state by filling the
        // 16-slot channel to capacity and then running a sweep. The
        // TrySendError::Full branch should NOT push the path into `closed`,
        // so the entry survives.
        let state = make_state();
        let path = "/agent/events/slow-client";
        let _rx = insert_conn(&state, path, "conn-slow").await;

        // Pre-fill the buffer. The sender was stashed in the map, so pull
        // it out and flood it to the brim without draining.
        {
            let sse = state.sse_connections.read().await;
            let sender = sse.get(path).unwrap().sender.clone();
            for i in 0..16 {
                sender
                    .try_send(SseEvent {
                        event_type: "filler".into(),
                        data: format!("{i}"),
                    })
                    .expect("pre-fill should succeed");
            }
            // Next try_send from within sweep_once will hit Full.
        }

        sweep_once(&state).await;
        drain_spawned_cleanup().await;

        assert_present(&state, path).await;
    }

    #[tokio::test]
    async fn sweep_cleans_only_closed_entries() {
        // Mixed state: one dead, one alive, one slow. Sweep must remove
        // exactly the dead one.
        let state = make_state();

        let _alive_rx = insert_conn(&state, "/agent/events/alive", "conn-alive").await;
        let dead_rx = insert_conn(&state, "/agent/events/dead", "conn-dead").await;
        let _slow_rx = insert_conn(&state, "/agent/events/slow", "conn-slow").await;
        drop(dead_rx);

        sweep_once(&state).await;
        drain_spawned_cleanup().await;

        assert_present(&state, "/agent/events/alive").await;
        assert_missing(&state, "/agent/events/dead").await;
        assert_present(&state, "/agent/events/slow").await;
    }
}
