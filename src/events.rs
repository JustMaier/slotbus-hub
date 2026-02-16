//! Event endpoints: worker event ingestion and unified SSE stream.

use std::convert::Infallible;
use std::sync::Arc;

use axum::extract::State;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::IntoResponse;
use axum::Json;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;

use crate::router::HubState;
use crate::types::{HubEvent, WorkerEvent};

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
            Some(Ok(Event::default()
                .event(&hub_event.event_type)
                .data(data)))
        }
        Err(_) => None,
    });

    Sse::new(stream).keep_alive(KeepAlive::default())
}
