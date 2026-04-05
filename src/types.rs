//! Wire types for hub HTTP API.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Worker registration request (`POST /internal/register`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterRequest {
    /// Worker name (e.g., "tts", "api").
    pub name: String,
    /// Routes this worker handles.
    pub routes: Vec<RouteRegistration>,
}

/// A single route the worker can handle.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouteRegistration {
    /// HTTP method (GET, POST, PUT, DELETE, PATCH).
    pub method: String,
    /// Path pattern with `{param}` placeholders.
    pub path: String,
    /// If true, the hub manages the SSE connection on behalf of this worker.
    /// The worker receives `SSE_CONNECT`/`SSE_DISCONNECT` lifecycle events
    /// and pushes data via `POST /internal/sse-push`.
    #[serde(default)]
    pub sse: bool,
}

/// Registration response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterResponse {
    /// Assigned worker ID.
    pub worker_id: String,
    /// SHM control region name.
    pub shm_name: String,
}

/// Worker event pushed via `POST /internal/emit`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerEvent {
    /// Source worker name.
    pub source: String,
    /// Event type string.
    pub event_type: String,
    /// JSON-serialized event data.
    pub data: String,
}

/// Hub event broadcast via SSE.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HubEvent {
    /// Source worker name.
    pub source: String,
    /// Event type.
    pub event_type: String,
    /// JSON-serialized event data.
    pub data: String,
}

/// Response from `GET /health`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthResponse {
    pub ok: bool,
    pub workers: Vec<WorkerInfo>,
}

/// Summary of a connected worker.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerInfo {
    pub name: String,
    pub worker_id: String,
    pub route_count: usize,
    pub transport: String,
}

/// Request body for `POST /internal/sse-push`.
///
/// Push an event to a specific hub-managed SSE connection. Identify the
/// connection either by exact `path` or by `pattern` + `params`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SsePushRequest {
    /// Exact matched path (e.g. "/agent/events/abc-123").
    #[serde(default)]
    pub path: Option<String>,
    /// Route pattern (e.g. "/agent/events/{sessionId}").
    #[serde(default)]
    pub pattern: Option<String>,
    /// Path parameters used with `pattern` to resolve the connection.
    #[serde(default)]
    pub params: Option<HashMap<String, String>>,
    /// SSE event type (becomes the `event:` field).
    pub event_type: String,
    /// SSE event data (becomes the `data:` field).
    pub data: String,
}

/// SSE lifecycle notification body sent to workers via SHM.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SseLifecycle {
    /// "connect" or "disconnect".
    pub sse_lifecycle: String,
    /// Extracted path parameters from the SSE route.
    pub params: HashMap<String, String>,
}
