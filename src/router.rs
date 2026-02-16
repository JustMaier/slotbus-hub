//! Hub router: registration, SHM request proxying, health, events.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use tokio::sync::{broadcast, RwLock};
use tower_http::cors::CorsLayer;

use slotbus::types::RequestMeta;
use slotbus::{SlotBus, SlotBusConfig};

use crate::events;
use crate::types::*;

// ---- Configuration -----------------------------------------------------------

/// Hub-level configuration (from CLI args).
pub struct HubConfig {
    pub timeout_secs: u64,
    pub num_slots: usize,
    pub region_size: usize,
    pub instrumentation: bool,
}

// ---- State -------------------------------------------------------------------

pub struct HubState {
    /// worker_id -> WorkerRecord
    workers: RwLock<HashMap<String, WorkerRecord>>,
    /// All registered routes: (method, pattern, worker_id)
    route_table: RwLock<Vec<(String, String, String)>>,
    /// Broadcast channel for unified event stream
    pub event_tx: broadcast::Sender<HubEvent>,
    /// Hub configuration
    config: HubConfig,
}

struct WorkerRecord {
    name: String,
    routes: Vec<RouteRegistration>,
    bus: Arc<SlotBus>,
}

// ---- Build router ------------------------------------------------------------

pub fn build_router(config: HubConfig) -> Router {
    let (event_tx, _) = broadcast::channel::<HubEvent>(256);

    let state = Arc::new(HubState {
        workers: RwLock::new(HashMap::new()),
        route_table: RwLock::new(Vec::new()),
        event_tx,
        config,
    });

    Router::new()
        .route("/internal/register", post(register_worker))
        .route("/internal/emit", post(events::emit_event))
        .route("/events", get(events::unified_sse))
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

    // Remove stale workers with the same name
    {
        let mut workers = state.workers.write().await;
        let stale_ids: Vec<String> = workers
            .iter()
            .filter(|(_, record)| record.name == req.name)
            .map(|(id, _)| id.clone())
            .collect();

        if !stale_ids.is_empty() {
            let mut table = state.route_table.write().await;
            for stale_id in &stale_ids {
                workers.remove(stale_id);
                table.retain(|(_, _, wid)| wid != stale_id);
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
            table.push((
                route.method.clone(),
                route.path.clone(),
                worker_id.clone(),
            ));
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

// ---- Fallback: proxy handler -------------------------------------------------

async fn proxy_handler(
    State(state): State<Arc<HubState>>,
    request: Request,
) -> impl IntoResponse {
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
    let matched = route_table.iter().find(|(m, pattern, _)| {
        m == &method && match_route(pattern, &path).is_some()
    });

    let (route_pattern, worker_id) = match matched {
        Some((_, pattern, wid)) => (pattern.clone(), wid.clone()),
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
            return (
                StatusCode::BAD_GATEWAY,
                format!("SHM dispatch failed: {e}"),
            )
                .into_response();
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
