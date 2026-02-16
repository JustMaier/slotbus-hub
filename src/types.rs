//! Wire types for hub HTTP API.

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
