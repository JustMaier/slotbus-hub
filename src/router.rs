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

/// Hub-level configuration (from CLI args).
pub struct HubConfig {
    pub timeout_secs: u64,
    pub num_slots: usize,
    pub region_size: usize,
    pub instrumentation: bool,
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

    let state = Arc::new(HubState {
        workers: RwLock::new(HashMap::new()),
        route_table: RwLock::new(Vec::new()),
        event_tx,
        sse_connections: RwLock::new(HashMap::new()),
        config,
    });

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

/// Drop guard that cleans up SSE connection state and notifies the worker.
/// Uses `connection_id` to avoid removing a replacement entry (race condition:
/// Client A disconnects after Client B connects on the same path).
struct SseDropGuard {
    connection_id: String,
    path: String,
    state: Arc<HubState>,
    worker_id: String,
    path_pattern: String,
    params: HashMap<String, String>,
}

impl Drop for SseDropGuard {
    fn drop(&mut self) {
        let connection_id = self.connection_id.clone();
        let path = self.path.clone();
        let state = Arc::clone(&self.state);
        let worker_id = self.worker_id.clone();
        let path_pattern = self.path_pattern.clone();
        let params = self.params.clone();

        tokio::spawn(async move {
            // Only remove if this connection still owns the entry
            let removed = {
                let mut sse = state.sse_connections.write().await;
                if let Some(info) = sse.get(&path) {
                    if info.connection_id == connection_id {
                        sse.remove(&path);
                        true
                    } else {
                        tracing::debug!(
                            path,
                            old_conn = connection_id,
                            "SSE drop guard skipped: entry belongs to a newer connection"
                        );
                        false
                    }
                } else {
                    false
                }
            };

            if removed {
                tracing::info!(path, worker_id, "SSE client disconnected");

                // Notify worker of disconnect (fire-and-forget)
                dispatch_sse_lifecycle(
                    &state,
                    &worker_id,
                    "disconnect",
                    &path,
                    &path_pattern,
                    &params,
                )
                .await;
            }
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

    // Build the SSE stream with a drop guard for disconnect detection
    let drop_guard = SseDropGuard {
        connection_id,
        path: path.clone(),
        state: Arc::clone(&state),
        worker_id,
        path_pattern: route_pattern,
        params: path_params,
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
