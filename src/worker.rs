//! Worker SDK for connecting to a slotbus-hub via shared memory.
//!
//! Handles registration, SHM setup, request dispatch, and event emission.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use slotbus::transport::{Request as ShmRequest, SlotWorker};
use slotbus::SlotBusConfig;

use crate::types::*;

/// Reserved route the SDK answers automatically so the hub's liveness reaper
/// can prove the SHM round-trip is alive. Kept in sync with the hub's
/// `router::WORKER_PING_ROUTE`. Implementers never see or handle this.
pub const WORKER_PING_ROUTE: &str = "/__ping__";

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
    dyn Fn(
            Arc<S>,
            HashMap<String, String>,
            Option<String>,
            Vec<u8>,
        ) -> Pin<Box<dyn Future<Output = HandlerResponse> + Send>>
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

    /// Register with the hub, open the SHM region, dispatch incoming
    /// requests, and **auto-reconnect if the hub restarts.**
    ///
    /// The worker runs a health-check poll in parallel with the SHM receive
    /// loop. If the hub becomes unreachable or no longer lists us as a
    /// registered worker (strong signal the hub process was replaced), we
    /// stop the current receive loop, drop the stale SHM handle, back off
    /// briefly, and re-run the full register → open → dispatch cycle. The
    /// route table and shared state are preserved across reconnects; only
    /// the SHM region and the hub-assigned `worker_id` change.
    ///
    /// This returns `Ok(())` only if the outer caller explicitly stops the
    /// tokio runtime. Under normal operation it loops forever.
    pub async fn run(self) -> Result<(), String> {
        let Self {
            hub_url,
            name,
            state,
            routes,
            client,
        } = self;
        let routes: Arc<Vec<RouteEntry<S>>> = Arc::new(routes);

        // Exponential backoff capped at 30s. The exponent is clamped at 5
        // (so 1 → 2 → 4 → 8 → 16 → 30 → 30 …) to avoid absurdly long waits
        // while still giving a struggling hub room to recover.
        const MAX_BACKOFF_SECS: u64 = 30;
        let mut attempt: u32 = 0;

        loop {
            match run_once(&hub_url, &name, &client, &routes, &state).await {
                Ok(()) => {
                    // run_once only returns Ok on clean shutdown, which
                    // currently isn't wired up. Propagate anyway so callers
                    // that add shutdown support later see the clean exit.
                    return Ok(());
                }
                Err(e) => {
                    let delay_secs = (1u64 << attempt.min(5)).min(MAX_BACKOFF_SECS);
                    eprintln!(
                        "[{}] Hub connection lost: {e}. Reconnecting in {delay_secs}s...",
                        name.to_uppercase()
                    );
                    tokio::time::sleep(std::time::Duration::from_secs(delay_secs)).await;
                    attempt = attempt.saturating_add(1);
                }
            }
        }
    }
}

/// Poll interval for the hub health check watchdog.
const HUB_HEALTH_POLL: std::time::Duration = std::time::Duration::from_secs(5);

/// One full lifecycle: register with the hub, open the SHM region, run the
/// receive loop, and block on a health-check watchdog. Returns `Err` with
/// a human-readable reason when the hub goes away so the outer `run()` can
/// back off and retry.
async fn run_once<S: Send + Sync + 'static>(
    hub_url: &str,
    name: &str,
    client: &reqwest::Client,
    routes: &Arc<Vec<RouteEntry<S>>>,
    state: &Arc<S>,
) -> Result<(), String> {
    // ── Register ─────────────────────────────────────────────────────────
    let mut route_regs: Vec<RouteRegistration> = routes
        .iter()
        .map(|r| RouteRegistration {
            method: r.method.clone(),
            path: r.path.clone(),
            sse: r.sse,
        })
        .collect();

    // Advertise the reserved liveness route so the hub can route `/__ping__`
    // (the reaper dispatches it over SHM; it's also reachable via HTTP proxy).
    // The response is short-circuited in `dispatch_request`, so there's no
    // implementer-provided handler for it.
    route_regs.push(RouteRegistration {
        method: "GET".to_string(),
        path: WORKER_PING_ROUTE.to_string(),
        sse: false,
    });

    let reg_req = RegisterRequest {
        name: name.to_string(),
        routes: route_regs,
    };

    let resp = client
        .post(format!("{hub_url}/internal/register"))
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

    let worker_id = reg_resp.worker_id.clone();
    eprintln!(
        "[{}] Registered with hub (worker_id={}, shm={}, {} routes)",
        name.to_uppercase(),
        worker_id,
        reg_resp.shm_name,
        routes.len()
    );

    // ── Open SHM region ──────────────────────────────────────────────────
    let worker_config = SlotBusConfig::builder().name(name).prefix("hub").build();

    let transport =
        Arc::new(SlotWorker::open(worker_config).map_err(|e| format!("Failed to open SHM: {e}"))?);

    // ── SHM receive loop (blocking thread) ───────────────────────────────
    let rt_handle = tokio::runtime::Handle::current();
    let transport_for_loop = Arc::clone(&transport);
    let routes_for_loop = Arc::clone(routes);
    let state_for_loop = Arc::clone(state);
    let name_for_loop = name.to_string();

    // Track in-flight response tasks. Each request is handled in a detached
    // tokio task that writes its response into the SHM region. On reconnect
    // the region is torn down/replaced; if a response task is still running it
    // would write into freed shared memory → access violation in
    // `write_response`. We count outstanding tasks and drain them before
    // dropping the region (see cleanup below).
    let in_flight = Arc::new(AtomicUsize::new(0));
    let in_flight_loop = Arc::clone(&in_flight);

    let receive_join = transport_for_loop.start_receive_loop(move |worker, slot_index, request| {
        let routes = Arc::clone(&routes_for_loop);
        let state = Arc::clone(&state_for_loop);
        let worker_name = name_for_loop.clone();
        let worker = Arc::clone(&worker);
        let in_flight = Arc::clone(&in_flight_loop);

        in_flight.fetch_add(1, Ordering::SeqCst);
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
            in_flight.fetch_sub(1, Ordering::SeqCst);
        });
    });

    // ── Health watchdog ──────────────────────────────────────────────────
    //
    // Poll /health and check that our worker_id is still in the list. If
    // the hub process restarts, it loses all worker records and our id
    // disappears — that's the signal to drop this session and reconnect.
    // A transient network error also triggers reconnect: it's cheap to
    // spuriously re-register, and the hub's register handler tolerates
    // replay of an existing worker by replacing the entry.
    let health_url = format!("{hub_url}/health");
    let reason = loop {
        tokio::time::sleep(HUB_HEALTH_POLL).await;
        match check_worker_registered(client, &health_url, &worker_id).await {
            Ok(true) => continue,
            Ok(false) => {
                break format!("worker_id {worker_id} missing from /health (hub restarted?)")
            }
            Err(e) => break format!("health check error: {e}"),
        }
    };

    // ── Clean up and signal caller to reconnect ──────────────────────────
    transport.stop();
    // Joining the receive thread is best-effort: it should exit promptly
    // once `stop()` wakes the req_event, but we don't want to block forever
    // if something is wedged.
    let _ = receive_join.join();

    // Drain in-flight response tasks BEFORE returning — the caller will
    // reconnect and drop/replace this SHM region, and any task still inside
    // `send_response`/`write_response` would write into freed shared memory
    // (access violation). The receive loop is already stopped, so no new
    // tasks spawn; wait (bounded) for the outstanding ones to finish.
    let drain_start = Instant::now();
    while in_flight.load(Ordering::SeqCst) > 0 {
        if drain_start.elapsed() > Duration::from_secs(5) {
            eprintln!(
                "[{}] WARN: {} response task(s) still in-flight after 5s drain timeout; proceeding to reconnect",
                name.to_uppercase(),
                in_flight.load(Ordering::SeqCst)
            );
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    Err(reason)
}

/// Hit the hub's `/health` endpoint and check whether the given worker_id
/// is still in the active workers list.
///
/// Returns:
/// - `Ok(true)`  — hub reachable and we're still registered
/// - `Ok(false)` — hub reachable but our worker_id is gone (hub restarted
///   or something else kicked us out)
/// - `Err(_)`    — hub unreachable or response is unparseable
async fn check_worker_registered(
    client: &reqwest::Client,
    health_url: &str,
    worker_id: &str,
) -> Result<bool, String> {
    let resp = client
        .get(health_url)
        .send()
        .await
        .map_err(|e| format!("GET {health_url}: {e}"))?;
    if !resp.status().is_success() {
        return Err(format!("health returned status {}", resp.status()));
    }
    let body: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| format!("invalid health JSON: {e}"))?;
    let Some(workers) = body.get("workers").and_then(|w| w.as_array()) else {
        return Err("health response missing 'workers' array".to_string());
    };
    Ok(workers
        .iter()
        .any(|w| w.get("worker_id").and_then(|id| id.as_str()) == Some(worker_id)))
}

/// Dispatch a single request to the matching handler.
async fn dispatch_request<S: Send + Sync + 'static>(
    _worker_name: &str,
    routes: &[RouteEntry<S>],
    state: Arc<S>,
    request: ShmRequest,
) -> HandlerResponse {
    // Reserved liveness probe — answered by the SDK, never dispatched to an
    // implementer handler. Keeps `/health` honest via the hub's reaper.
    if request.method == "GET" && request.route_pattern == WORKER_PING_ROUTE {
        return HandlerResponse::ok();
    }

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
