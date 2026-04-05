//! Event endpoints: worker event ingestion, unified SSE stream, and SSE push.

use std::convert::Infallible;
use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::IntoResponse;
use axum::Json;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;

use crate::router::{self, HubState, SseEvent};
use slotbus_hub::types::{HubEvent, SsePushRequest, WorkerEvent};

/// `POST /internal/emit` — worker pushes an event for broadcast.
pub async fn emit_event(
    State(state): State<Arc<HubState>>,
    Json(worker_event): Json<WorkerEvent>,
) -> impl IntoResponse {
    let hub_event = HubEvent {
        source: worker_event.source,
        event_type: worker_event.event_type,
        data: worker_event.data,
    };

    let _ = state.event_tx.send(hub_event);
    Json(serde_json::json!({}))
}

/// `GET /events` — unified SSE stream of all hub events.
pub async fn unified_sse(
    State(state): State<Arc<HubState>>,
) -> Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>> {
    let rx = state.event_tx.subscribe();
    let stream = BroadcastStream::new(rx).filter_map(|result| match result {
        Ok(hub_event) => {
            let data = serde_json::to_string(&hub_event).unwrap_or_default();
            Some(Ok(Event::default().event(&hub_event.event_type).data(data)))
        }
        Err(_) => None,
    });

    Sse::new(stream).keep_alive(KeepAlive::default())
}

/// `GET /events/{channel}` — scoped SSE stream filtered to a specific channel.
///
/// Only forwards events where the JSON `data` field contains a `"channel"` key
/// matching the path parameter. This lets clients subscribe to events for a
/// specific channel ID without receiving all hub traffic.
pub async fn scoped_sse(
    State(state): State<Arc<HubState>>,
    axum::extract::Path(channel): axum::extract::Path<String>,
) -> Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>> {
    let rx = state.event_tx.subscribe();
    let stream = BroadcastStream::new(rx).filter_map(move |result| {
        match result {
            Ok(hub_event) => {
                // Check if the event's data JSON has a "channel" field matching our subscription
                if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&hub_event.data) {
                    if parsed.get("channel").and_then(|v| v.as_str()) == Some(channel.as_str()) {
                        let data = serde_json::to_string(&hub_event).unwrap_or_default();
                        return Some(Ok(Event::default().event(&hub_event.event_type).data(data)));
                    }
                }
                None
            }
            Err(_) => None,
        }
    });

    Sse::new(stream).keep_alive(KeepAlive::default())
}

/// `POST /internal/sse-push` — push an event to a hub-managed SSE connection.
///
/// Identify the target connection by exact `path` or by `pattern` + `params`.
pub async fn sse_push(
    State(state): State<Arc<HubState>>,
    Json(req): Json<SsePushRequest>,
) -> impl IntoResponse {
    let resolved = router::resolve_sse_path(
        req.path.as_deref(),
        req.pattern.as_deref(),
        req.params.as_ref(),
    );

    let path = match resolved {
        Some(p) => p,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": "Must provide either `path` or `pattern` + `params`"
                })),
            )
                .into_response();
        }
    };

    // First try with a read lock (common path: connection exists and is healthy)
    {
        let sse = state.sse_connections.read().await;
        let conn = match sse.get(&path) {
            Some(c) => c,
            None => {
                return (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({
                        "error": format!("No active SSE connection for path: {path}")
                    })),
                )
                    .into_response();
            }
        };

        match conn.sender.try_send(SseEvent {
            event_type: req.event_type.clone(),
            data: req.data.clone(),
        }) {
            Ok(_) => return Json(serde_json::json!({"ok": true})).into_response(),
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                tracing::warn!(path, "SSE channel full (256), dropping event for slow client");
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(serde_json::json!({
                        "error": "SSE client is too slow, event dropped"
                    })),
                )
                    .into_response();
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                // Fall through to remove the stale entry with a write lock
            }
        }
    }

    // Channel closed — remove stale entry and report gone
    {
        let mut sse = state.sse_connections.write().await;
        sse.remove(&path);
    }
    tracing::debug!(path, "removed stale SSE connection on push failure");

    (
        StatusCode::GONE,
        Json(serde_json::json!({
            "error": "SSE connection is closing"
        })),
    )
        .into_response()
}
