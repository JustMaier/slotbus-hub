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

/// The OS-level prefix for every hub SHM region, from `SLOTBUS_PREFIX`.
///
/// Region names are `{prefix}-{worker}` (slotbus `SlotBusConfig::region_name`),
/// so with a fixed prefix two hubs on one machine fight over `hub-tts` even when
/// their HTTP ports differ. The second worker to register displaces the first,
/// which reads as "worker went away" on a route that was working a moment ago.
/// That made it impossible to smoke-test a packaged build beside a running fleet
/// — ports were configurable, this was not.
///
/// Default `hub`, so nothing changes for an existing deployment, and a binary
/// built before this existed computes the same names as one built after.
///
/// **Both call sites must use this.** The hub creates the region and the worker
/// opens it by name; if they disagree the worker cannot find it, and the failure
/// is a silence rather than an error. Read once so a mid-run environment change
/// cannot make them diverge either.
pub fn shm_prefix() -> &'static str {
    static PREFIX: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    PREFIX.get_or_init(|| {
        std::env::var("SLOTBUS_PREFIX")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "hub".to_string())
    })
}
