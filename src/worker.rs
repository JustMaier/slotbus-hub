//! Worker SDK for connecting to a slotbus-hub via shared memory.
//!
//! Handles registration, SHM setup, request dispatch, and event emission.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use slotbus::transport::{Request as ShmRequest, SlotWorker};
use slotbus::SlotBusConfig;

use crate::types::*;

// ---- Handler Response --------------------------------------------------------

/// Response returned by worker handler functions.
pub struct HandlerResponse {
    pub status: u16,
    pub body: Vec<u8>,
    pub content_type: String,
    pub headers: Vec<(String, String)>,
}

impl HandlerResponse {
    /// JSON response with given status code.
    pub fn json(status: u16, body: &impl serde::Serialize) -> Self {
        Self {
            status,
            body: serde_json::to_vec(body).unwrap_or_default(),
            content_type: "application/json".into(),
            headers: Vec::new(),
        }
    }

    /// 200 JSON response.
    pub fn ok_json(body: &impl serde::Serialize) -> Self {
        Self::json(200, body)
    }

    /// Error JSON response: `{"error": "msg"}`.
    pub fn error(status: u16, msg: &str) -> Self {
        Self::json(status, &serde_json::json!({ "error": msg }))
    }

    /// Raw bytes with given content-type.
    pub fn bytes(status: u16, body: Vec<u8>, content_type: &str) -> Self {
        Self {
            status,
            body,
            content_type: content_type.into(),
            headers: Vec::new(),
        }
    }

    /// Raw bytes with additional response headers.
    pub fn bytes_with_headers(
        status: u16,
        body: Vec<u8>,
        content_type: &str,
        headers: Vec<(String, String)>,
    ) -> Self {
        Self {
            status,
            body,
            content_type: content_type.into(),
            headers,
        }
    }

    /// Empty 200 OK.
    pub fn ok() -> Self {
        Self::json(200, &serde_json::json!({}))
    }

    /// Status code only (empty body).
    pub fn status(code: u16) -> Self {
        Self {
            status: code,
            body: Vec::new(),
            content_type: "application/json".into(),
            headers: Vec::new(),
        }
    }
}

// ---- Type-erased handler -----------------------------------------------------

type BoxHandler<S> = Box<
    dyn Fn(Arc<S>, HashMap<String, String>, Option<String>, Vec<u8>)
            -> Pin<Box<dyn Future<Output = HandlerResponse> + Send>>
        + Send
        + Sync,
>;

struct RouteEntry<S> {
    method: String,
    path: String,
    handler: BoxHandler<S>,
    sse: bool,
}

// ---- HubEmitter --------------------------------------------------------------

/// Clonable handle for pushing events from a worker to the hub.
#[derive(Clone)]
pub struct HubEmitter {
    hub_url: String,
    source: String,
    client: reqwest::Client,
}

impl HubEmitter {
    /// Push an event to the hub for broadcast on the unified SSE stream.
    pub async fn emit(&self, event_type: &str, data: &str) -> Result<(), String> {
        let event = WorkerEvent {
            source: self.source.clone(),
            event_type: event_type.to_string(),
            data: data.to_string(),
        };

        self.client
            .post(format!("{}/internal/emit", self.hub_url))
            .json(&event)
            .send()
            .await
            .map_err(|e| format!("Failed to emit event: {e}"))?;

        Ok(())
    }

    /// Push an event to a specific hub-managed SSE connection by exact path.
    pub async fn sse_push(&self, path: &str, event_type: &str, data: &str) -> Result<(), String> {
        let req = SsePushRequest {
            path: Some(path.to_string()),
            pattern: None,
            params: None,
            event_type: event_type.to_string(),
            data: data.to_string(),
        };

        let resp = self
            .client
            .post(format!("{}/internal/sse-push", self.hub_url))
            .json(&req)
            .send()
            .await
            .map_err(|e| format!("Failed to push SSE event: {e}"))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(format!("SSE push failed ({status}): {body}"));
        }

        Ok(())
    }

    /// Push an event to a hub-managed SSE connection by pattern + params.
    pub async fn sse_push_by_pattern(
        &self,
        pattern: &str,
        params: HashMap<String, String>,
        event_type: &str,
        data: &str,
    ) -> Result<(), String> {
        let req = SsePushRequest {
            path: None,
            pattern: Some(pattern.to_string()),
            params: Some(params),
            event_type: event_type.to_string(),
            data: data.to_string(),
        };

        let resp = self
            .client
            .post(format!("{}/internal/sse-push", self.hub_url))
            .json(&req)
            .send()
            .await
            .map_err(|e| format!("Failed to push SSE event: {e}"))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(format!("SSE push failed ({status}): {body}"));
        }

        Ok(())
    }

    /// Source name of this worker.
    pub fn source(&self) -> &str {
        &self.source
    }
}

// ---- HubWorker ---------------------------------------------------------------

/// Builder for a hub-connected worker.
pub struct HubWorker<S: Send + Sync + 'static> {
    hub_url: String,
    name: String,
    state: Arc<S>,
    routes: Vec<RouteEntry<S>>,
    client: reqwest::Client,
}

impl<S: Send + Sync + 'static> HubWorker<S> {
    /// Create a new worker that will connect to the given hub URL.
    pub fn new(hub_url: &str, name: &str, state: S) -> Self {
        Self {
            hub_url: hub_url.trim_end_matches('/').to_string(),
            name: name.to_string(),
            state: Arc::new(state),
            routes: Vec::new(),
            client: reqwest::Client::new(),
        }
    }

    /// Register a route handler.
    pub fn route<F, Fut>(mut self, method: &str, path: &str, handler: F) -> Self
    where
        F: Fn(Arc<S>, HashMap<String, String>, Option<String>, Vec<u8>) -> Fut
            + Send
            + Sync
            + 'static,
        Fut: Future<Output = HandlerResponse> + Send + 'static,
    {
        let boxed: BoxHandler<S> = Box::new(move |state, params, query, body| {
            Box::pin(handler(state, params, query, body))
        });
        self.routes.push(RouteEntry {
            method: method.to_string(),
            path: path.to_string(),
            handler: boxed,
            sse: false,
        });
        self
    }

    /// Register an SSE route. The hub manages the SSE connection and sends
    /// lifecycle events (`SSE_CONNECT`/`SSE_DISCONNECT`) as POST requests to
    /// this handler. The handler receives a JSON body with `sse_lifecycle`
    /// ("connect"/"disconnect") and `params` fields.
    pub fn sse_route<F, Fut>(mut self, path: &str, handler: F) -> Self
    where
        F: Fn(Arc<S>, HashMap<String, String>, Option<String>, Vec<u8>) -> Fut
            + Send
            + Sync
            + 'static,
        Fut: Future<Output = HandlerResponse> + Send + 'static,
    {
        let boxed: BoxHandler<S> = Box::new(move |state, params, query, body| {
            Box::pin(handler(state, params, query, body))
        });
        // SSE routes register as GET (the client-facing method) but the hub
        // sends lifecycle events as POST via SHM. The worker matches on
        // method=POST + route_pattern.
        self.routes.push(RouteEntry {
            method: "GET".to_string(),
            path: path.to_string(),
            handler: boxed,
            sse: true,
        });
        self
    }

    /// Get a clonable emitter handle for pushing events to the hub.
    pub fn emitter(&self) -> HubEmitter {
        HubEmitter {
            hub_url: self.hub_url.clone(),
            source: self.name.clone(),
            client: self.client.clone(),
        }
    }

    /// Get a clone of the shared state.
    pub fn state(&self) -> Arc<S> {
        self.state.clone()
    }

    /// Register with the hub, open the SHM region, and dispatch incoming
    /// requests to handlers. Blocks forever (reconnects on SHM loss).
    pub async fn run(self) -> Result<(), String> {
        // ── Register ─────────────────────────────────────────────────────
        let route_regs: Vec<RouteRegistration> = self
            .routes
            .iter()
            .map(|r| RouteRegistration {
                method: r.method.clone(),
                path: r.path.clone(),
                sse: r.sse,
            })
            .collect();

        let reg_req = RegisterRequest {
            name: self.name.clone(),
            routes: route_regs,
        };

        let resp = self
            .client
            .post(format!("{}/internal/register", self.hub_url))
            .json(&reg_req)
            .send()
            .await
            .map_err(|e| format!("Failed to register with hub: {e}"))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(format!("Hub registration failed ({status}): {body}"));
        }

        let reg_resp: RegisterResponse = resp
            .json()
            .await
            .map_err(|e| format!("Invalid registration response: {e}"))?;

        eprintln!(
            "[{}] Registered with hub (worker_id={}, shm={}, {} routes)",
            self.name.to_uppercase(),
            reg_resp.worker_id,
            reg_resp.shm_name,
            self.routes.len()
        );

        // ── Open SHM region + events ─────────────────────────────────────
        let worker_config = SlotBusConfig::builder()
            .name(&self.name)
            .prefix("hub")
            .build();

        let transport = Arc::new(
            SlotWorker::open(worker_config)
                .map_err(|e| format!("Failed to open SHM: {e}"))?,
        );

        // ── Shared state for concurrent request handling ─────────────────
        let routes: Arc<Vec<RouteEntry<S>>> = Arc::new(self.routes);
        let state = self.state;
        let name = self.name.clone();

        let rt_handle = tokio::runtime::Handle::current();

        // ── SHM receive loop (blocking thread) ───────────────────────────
        let transport_clone = Arc::clone(&transport);
        let handle = transport_clone.start_receive_loop(move |worker, slot_index, request| {
            let routes = Arc::clone(&routes);
            let state = Arc::clone(&state);
            let worker_name = name.clone();
            let worker = Arc::clone(&worker);

            rt_handle.spawn(async move {
                let response = dispatch_request(&worker_name, &routes, state, request).await;

                if let Err(e) = worker.send_response(
                    slot_index,
                    response.status,
                    response.body,
                    &response.content_type,
                    response.headers,
                ) {
                    eprintln!(
                        "[{}] Failed to write response to slot {slot_index}: {e}",
                        worker_name.to_uppercase()
                    );
                }
            });
        });

        handle.join().map_err(|_| "SHM receive thread panicked".to_string())
    }
}

/// Dispatch a single request to the matching handler.
async fn dispatch_request<S: Send + Sync + 'static>(
    _worker_name: &str,
    routes: &[RouteEntry<S>],
    state: Arc<S>,
    request: ShmRequest,
) -> HandlerResponse {
    let handler = routes
        .iter()
        .find(|r| r.method == request.method && r.path == request.route_pattern);

    if let Some(route) = handler {
        (route.handler)(state, request.path_params, request.query, request.body).await
    } else {
        HandlerResponse::error(
            404,
            &format!(
                "No handler for {} {}",
                request.method, request.route_pattern
            ),
        )
    }
}
