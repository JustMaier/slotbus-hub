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

/// Reserved route every worker answers via the SDK. The liveness reaper
/// round-trips this over each worker's SHM to prove the path is actually
/// alive — not merely registered. Kept in sync with the SDK's
/// `worker::WORKER_PING_ROUTE`.
const WORKER_PING_ROUTE: &str = "/__ping__";

/// How often the liveness reaper probes every registered worker.
const LIVENESS_REAP_INTERVAL: Duration = Duration::from_secs(10);

/// Per-probe SHM round-trip timeout. A live worker answers `/__ping__`
/// immediately (no handler work), so this is generous.
const LIVENESS_PING_TIMEOUT: Duration = Duration::from_secs(2);

/// Consecutive missed probes before a worker is evicted. At a 10s interval
/// that's ~30s of silence — long enough to ride out a momentarily saturated
/// worker, short enough to self-heal after a sleep/wake without a restart.
const LIVENESS_MAX_MISSES: u32 = 3;

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
    /// Which worker owns this stream, by **name** rather than by instance id.
    ///
    /// The name is the stable identity: a worker restarts, is evicted, sleeps
    /// and re-registers under a fresh `worker_id` every time, but it is the
    /// same `agent` throughout, and the route table is rebuilt under the name.
    /// Keying a connection to one instance meant every path that lost the
    /// instance lost the connection with it — see [`register_worker`]'s replay
    /// and [`dispatch_sse_lifecycle`], both of which now resolve through here.
    pub worker_name: String,
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
    /// Fires once when this worker leaves the hub, so requests already parked
    /// on its SHM stop waiting.
    ///
    /// Dropping the record is not enough to wake them. A request clones the
    /// `Arc<SlotBus>` before dispatching and holds it until it returns, and
    /// the bus owns the oneshot sender the request waits on — so the waiter
    /// keeps alive the very channel whose closure would have released it.
    /// Left alone it waits the full request timeout for a process that is
    /// already gone.
    gone: Arc<tokio::sync::Notify>,
    /// Proxied requests this hub is waiting on, for explaining slot exhaustion.
    inflight: Arc<InFlight>,
}

/// How often one worker's exhaustion snapshot may be logged. A storm produced
/// 37 failures in one minute; one snapshot per window says the same thing.
const EXHAUSTION_REPORT_INTERVAL: Duration = Duration::from_secs(5);

/// Proxied requests in flight to one worker, keyed by route pattern.
///
/// The slot table alone says only *that* a worker's slots are claimed, never
/// by what. On 2026-10-02 the agent worker ran out of 64 slots every ~75s for
/// days, and the hub logged none of it — the failures reached agents as tool
/// errors and nowhere else. The cause (one slow route holding a lock that
/// every other route queued behind) was only found by sampling stacks. This
/// table is what makes the next one a log line instead.
///
/// It tracks the hub's side: an entry leaves when the hub stops waiting. A
/// request the hub timed out on keeps its slot until the worker answers, so
/// a timeout is logged separately as the slot outliving its caller.
#[derive(Default)]
struct InFlight {
    next_id: std::sync::atomic::AtomicU64,
    requests: std::sync::Mutex<HashMap<u64, (std::time::Instant, String)>>,
    last_report: std::sync::Mutex<Option<std::time::Instant>>,
    suppressed: std::sync::atomic::AtomicU64,
}

/// Removes its entry from [`InFlight`] when the proxied request ends, on
/// every path out of the handler.
struct InFlightGuard {
    table: Arc<InFlight>,
    id: u64,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.table
            .requests
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.id);
    }
}

impl InFlight {
    fn track(self: &Arc<Self>, label: String) -> InFlightGuard {
        let id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.requests
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(id, (std::time::Instant::now(), label));
        InFlightGuard {
            table: Arc::clone(self),
            id,
        }
    }

    /// `(route, count, oldest age in ms)`, busiest route first.
    fn by_route(&self) -> Vec<(String, usize, u128)> {
        let mut grouped: HashMap<String, (usize, u128)> = HashMap::new();
        for (started, label) in self
            .requests
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
        {
            let entry = grouped.entry(label.clone()).or_default();
            entry.0 += 1;
            entry.1 = entry.1.max(started.elapsed().as_millis());
        }
        let mut out: Vec<_> = grouped.into_iter().map(|(r, (n, age))| (r, n, age)).collect();
        out.sort_by(|a, b| b.1.cmp(&a.1).then(b.2.cmp(&a.2)));
        out
    }

    /// One line naming what holds the slots, at most once per
    /// [`EXHAUSTION_REPORT_INTERVAL`]; failures in between are counted and
    /// reported with the next line rather than dropped.
    fn report_dispatch_failure(&self, worker: &str, route: &str, error: &str) {
        let now = std::time::Instant::now();
        {
            let mut last = self.last_report.lock().unwrap_or_else(|e| e.into_inner());
            if last.is_some_and(|t| now.duration_since(t) < EXHAUSTION_REPORT_INTERVAL) {
                self.suppressed
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                return;
            }
            *last = Some(now);
        }
        let suppressed = self
            .suppressed
            .swap(0, std::sync::atomic::Ordering::Relaxed);
        let held = self
            .by_route()
            .iter()
            .take(8)
            .map(|(r, n, age)| format!("{n}x {r} (oldest {age}ms)"))
            .collect::<Vec<_>>()
            .join("; ");
        tracing::warn!(
            worker,
            route,
            error,
            suppressed_since_last = suppressed,
            held_by = %held,
            "SHM dispatch failed"
        );
    }
}

/// Internal route table entry.
struct RouteEntry {
    method: String,
    pattern: String,
    worker_id: String,
    /// The owning worker's name, carried alongside the instance id so an SSE
    /// connection can be filed under the identity that survives a restart.
    worker_name: String,
    sse: bool,
}

// ---- Build router ------------------------------------------------------------

pub fn build_router(config: HubConfig, auth: crate::auth::AuthState) -> Router {
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

    // Kick off the worker liveness reaper. Probes each worker's SHM round-trip
    // and evicts ones that go silent (e.g. after the host sleeps), so `/health`
    // reflects real reachability and the SDK watchdog can re-register.
    spawn_liveness_reaper(Arc::clone(&state));

    Router::new()
        .route("/internal/register", post(register_worker))
        .route("/internal/emit", post(events::emit_event))
        .route("/internal/sse-push", post(events::sse_push))
        .route("/internal/slots", get(slot_diagnostics))
        .route("/events", get(events::unified_sse))
        .route("/events/{channel}", get(events::scoped_sse))
        .route("/health", get(health))
        .fallback(proxy_handler)
        .with_state(state)
        // Whole-router layer: wraps every route above *and* the fallback proxy,
        // which is what carries all worker-registered paths (including SSE).
        // Sits inside CORS so preflight is answered before auth and so 401s
        // still carry CORS headers.
        .layer(axum::middleware::from_fn_with_state(
            auth,
            crate::auth::require_secret,
        ))
        .layer(CorsLayer::permissive())
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

// ---- Worker liveness reaper --------------------------------------------------

/// Spawn a background task that probes every registered worker's SHM
/// round-trip on a fixed interval and evicts ones that stop responding.
///
/// Registration alone is not liveness: a worker process can stay listed in
/// `/health` while its SHM signaling is dead (notably after the host sleeps
/// and wakes — kernel handles survive but the round-trip can silently break).
/// The hub otherwise only removes a worker when a same-named worker
/// re-registers, so a zombie would linger forever and the SDK watchdog —
/// which keys off `worker_id` presence in `/health` — would never reconnect.
///
/// The reaper closes that gap: it dispatches a reserved `GET /__ping__` over
/// each worker's bus. After [`LIVENESS_MAX_MISSES`] consecutive failures the
/// worker is evicted (same cleanup as the stale-worker path), its id drops
/// from `/health`, and the SDK re-registers on its next poll.
fn spawn_liveness_reaper(state: Arc<HubState>) {
    tokio::spawn(async move {
        let mut misses: HashMap<String, u32> = HashMap::new();
        let mut ticker = tokio::time::interval(LIVENESS_REAP_INTERVAL);
        // The first tick fires immediately; skip it so freshly-registered
        // workers get a full interval before their first probe.
        ticker.tick().await;

        loop {
            ticker.tick().await;

            // Snapshot (worker_id, name, bus) so we don't hold the lock across
            // the awaited probes.
            let snapshot: Vec<(String, String, Arc<SlotBus>)> = {
                let workers = state.workers.read().await;
                workers
                    .iter()
                    .map(|(id, r)| (id.clone(), r.name.clone(), Arc::clone(&r.bus)))
                    .collect()
            };

            // Forget miss counters for workers that are already gone.
            misses.retain(|id, _| snapshot.iter().any(|(wid, _, _)| wid == id));

            for (worker_id, name, bus) in snapshot {
                if ping_worker(&bus).await {
                    misses.remove(&worker_id);
                    continue;
                }

                let count = misses.entry(worker_id.clone()).or_insert(0);
                *count += 1;
                if *count >= LIVENESS_MAX_MISSES {
                    tracing::warn!(
                        name,
                        worker_id,
                        misses = *count,
                        "evicting unresponsive worker (SHM liveness probe failed)"
                    );
                    evict_worker(&state, &worker_id).await;
                    misses.remove(&worker_id);
                } else {
                    tracing::debug!(
                        name,
                        worker_id,
                        misses = *count,
                        "worker liveness probe miss"
                    );
                }
            }
        }
    });
}

/// Round-trip the reserved ping route over a worker's SHM bus. Returns `true`
/// only if the worker answered within [`LIVENESS_PING_TIMEOUT`].
async fn ping_worker(bus: &Arc<SlotBus>) -> bool {
    let meta = RequestMeta {
        path: WORKER_PING_ROUTE.to_string(),
        route_pattern: WORKER_PING_ROUTE.to_string(),
        path_params: Vec::new(),
        query: None,
        headers: Vec::new(),
    };
    let req_id = uuid::Uuid::new_v4().to_string();
    let rx = match bus.dispatch(&req_id, "GET", &meta, &[]) {
        Ok(rx) => rx,
        // No free slot / write failure also counts as a missed probe.
        Err(_) => return false,
    };
    matches!(
        tokio::time::timeout(LIVENESS_PING_TIMEOUT, rx).await,
        Ok(Ok(_))
    )
}

/// Remove a worker and all of its routes. Mirrors the stale-worker cleanup in
/// [`register_worker`], but keyed by exact `worker_id` so it can't race-delete
/// a replacement that re-registered under the same name.
async fn evict_worker(state: &Arc<HubState>, worker_id: &str) {
    {
        let mut workers = state.workers.write().await;
        if let Some(record) = workers.remove(worker_id) {
            // Same reasoning as the stale-worker path in `register_worker`:
            // stop the watcher before a replacement can share its region.
            record.bus.stop();
            record.gone.notify_waiters();
        }
    }
    {
        let mut table = state.route_table.write().await;
        table.retain(|entry| entry.worker_id != worker_id);
    }
}

// ---- POST /internal/register -------------------------------------------------

/// Characters a worker name may never contain.
///
/// The name becomes part of a shared-memory region name, and on Windows that
/// reaches the filesystem as `%TEMP%\shared_memory-rs\<region>`. slotbus refuses
/// to act on a name that is not a single path component, but the hub should not
/// be handing it such names in the first place — this is the outer half of a
/// defence in depth. Loopback registration is auth-exempt, so without this a
/// LAN peer holding `X-Hub-Secret` could steer paths on the hub machine.
fn invalid_worker_name(name: &str) -> Option<&'static str> {
    if name.is_empty() {
        return Some("must not be empty");
    }
    if name.contains('/') || name.contains('\\') {
        return Some("must not contain path separators");
    }
    if name.split(['/', '\\']).any(|part| part == "..") || name.contains("..") {
        return Some("must not contain '..'");
    }
    if name.contains(':') {
        return Some("must not contain ':'");
    }
    if name.contains('\0') {
        return Some("must not contain NUL");
    }
    None
}

async fn register_worker(
    State(state): State<Arc<HubState>>,
    Json(req): Json<RegisterRequest>,
) -> impl IntoResponse {
    if let Some(reason) = invalid_worker_name(&req.name) {
        tracing::warn!(
            name = req.name,
            reason,
            "rejected worker registration: unsafe name"
        );
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": format!("worker name {reason}"),
            })),
        )
            .into_response();
    }

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
                if let Some(record) = workers.remove(stale_id) {
                    // A worker re-registering under the same name means the
                    // old process is gone. Release anything still waiting on
                    // it rather than letting it sit out the request timeout —
                    // after a rebuild that is the first call the UI makes.
                    //
                    // Stopping the old bus matters just as much, and is not
                    // about tidiness: the replacement re-creates a region of
                    // the SAME name, so until the old response watcher exits,
                    // two watcher threads scan one region. The first to win
                    // the Done→Free CAS consumes the response — and the stale
                    // one has no pending entry for that request id, so it
                    // drops the payload on the floor. The new worker looks
                    // like it answered nothing and the caller waits out the
                    // full timeout. Dropping the record cannot do this: the
                    // in-flight requests hold `Arc<SlotBus>` clones and keep
                    // the watcher alive.
                    record.bus.stop();
                    record.gone.notify_waiters();
                }
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
        .prefix(shm_prefix())
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
                worker_name: req.name.clone(),
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
                gone: Arc::new(tokio::sync::Notify::new()),
                inflight: Arc::new(InFlight::default()),
            },
        );
    }

    // ── SSE replay: hand this worker every connection that belongs to it ──
    //
    // Unconditional, and matched by NAME. It used to run only when a
    // same-named worker was still registered (`stale_ids`), which quietly made
    // recovery depend on how long the worker had been away: the liveness
    // reaper evicts after ~30s (`LIVENESS_REAP_INTERVAL` × `LIVENESS_MAX_MISSES`)
    // and `evict_worker` does not touch `sse_connections`, so past that point
    // `stale_ids` was empty, this block never ran, and every live client sat in
    // the map bound to an instance id that no longer existed — permanently,
    // because nothing else ever rewrites that field.
    //
    // Measured 2026-08-19: an agent-server rebuild takes longer than 30s to
    // come back, so the repair path was skipped on precisely the restarts that
    // needed it. 17 connections replayed on a fast restart; 1 on the one after
    // an eviction, the other 16 orphaned with no error anywhere. Clients cannot
    // detect this themselves — the hub's own `sse_ping` sweeper keeps their
    // heartbeat satisfied, so nothing reconnects — leaving a 60s client-side
    // registration watchdog as the only repair.
    //
    // Matching on the name removes the dependency rather than lengthening it:
    // no eviction reason, downtime, sleep or crash can lose a connection now,
    // because there is no per-instance state left for the loss to hang on.
    // A clean long absence and a crash no longer have to be told apart.
    {
        let replay = {
            let sse = state.sse_connections.read().await;
            connections_owned_by(&sse, &req.name)
        };

        if !replay.is_empty() {
            let replay_count = replay.len();
            let replay_state = Arc::clone(&state);
            let replay_worker_id = worker_id.clone();
            let replay_name = req.name.clone();
            let replay_name_for_dispatch = req.name.clone();

            tracing::info!(
                name = req.name,
                worker_id,
                count = replay_count,
                "replaying SSE connections to new worker (background)"
            );

            // Replay concurrently, not one after another.
            //
            // Sequentially, each `connect` waits up to 5s for the worker to
            // acknowledge, against a 30s budget for the whole batch — so a
            // worker with more than a handful of connections could never finish,
            // and "skipping remaining" silently abandoned the rest. The hub had
            // already re-pointed those connections at the new worker above, so
            // the worker was never told about connections the hub believed it
            // owned, and every abandoned client stayed invisible until it
            // reconnected on its own.
            //
            // Measured 2026-08-12: restarting agent-server took 12 of 24 live
            // agents out of the roster. They were running the whole time; the
            // app simply could not route to them, and a chat message to one came
            // back as "not running". The replay is the repair path, so it has to
            // cover every connection, not the first few.
            //
            // Concurrent dispatch makes the batch cost one slow worker rather
            // than the sum of them. The per-connection 5s ack wait stays: it is
            // what makes a wedged worker's own connection fail rather than
            // everyone else's.
            tokio::spawn(async move {
                let mut tasks = tokio::task::JoinSet::new();
                for (path, params, pattern) in replay {
                    let state = Arc::clone(&replay_state);
                    let worker_name = replay_name_for_dispatch.clone();
                    tasks.spawn(async move {
                        dispatch_sse_lifecycle(
                            &state, &worker_name, "connect", &path, &pattern, &params,
                        )
                        .await;
                    });
                }

                // Generous overall bound: with the dispatches running in
                // parallel this should finish in about one ack, so reaching it
                // means something is genuinely wrong and worth saying loudly.
                let mut done = 0usize;
                let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
                loop {
                    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                    match tokio::time::timeout(remaining, tasks.join_next()).await {
                        Ok(Some(_)) => done += 1,
                        Ok(None) => break,
                        Err(_) => break,
                    }
                }

                // Never let an incomplete replay pass as a completed one — an
                // agent dropped here looks offline forever with nothing failing.
                if done < replay_count {
                    tracing::error!(
                        name = replay_name,
                        worker_id = replay_worker_id,
                        replayed = done,
                        total = replay_count,
                        missing = replay_count - done,
                        "SSE replay INCOMPLETE — those clients are invisible to the worker \
                         until they reconnect"
                    );
                } else {
                    tracing::info!(
                        name = replay_name,
                        worker_id = replay_worker_id,
                        replayed = done,
                        "SSE replay complete"
                    );
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

        // What the hub is waiting on, by route — the answer to "claimed by
        // what?" that the slot states alone cannot give.
        let in_flight: Vec<_> = record
            .inflight
            .by_route()
            .into_iter()
            .map(|(route, count, oldest_ms)| {
                serde_json::json!({ "route": route, "count": count, "oldest_ms": oldest_ms })
            })
            .collect();

        entries.push(serde_json::json!({
            "name": record.name,
            "total_slots": total,
            "free": free,
            "in_use": in_use,
            "in_flight": in_flight,
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

    let (route_pattern, worker_id, worker_name, is_sse) = match matched {
        Some(entry) => (
            entry.pattern.clone(),
            entry.worker_id.clone(),
            entry.worker_name.clone(),
            entry.sse,
        ),
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
        return handle_sse_delegation(state, worker_name, path, route_pattern, path_params)
            .await
            .into_response();
    }

    // ── Normal SHM proxy ─────────────────────────────────────────────────

    // Look up worker
    let workers = state.workers.read().await;
    let (bus, gone, inflight) = match workers.get(&worker_id) {
        Some(record) => (
            Arc::clone(&record.bus),
            Arc::clone(&record.gone),
            Arc::clone(&record.inflight),
        ),
        None => {
            return (
                StatusCode::BAD_GATEWAY,
                format!("Worker {worker_id} not found"),
            )
                .into_response();
        }
    };
    // Subscribe while the read guard is still held. `notify_waiters` only
    // wakes waiters that already exist, so a subscription taken after the
    // lookup can miss an eviction that lands in between — and that waiter
    // then sleeps through its own worker's departure for the full timeout.
    // Eviction needs the write lock, so holding the read lock across the
    // subscribe makes the gap unrepresentable rather than merely small.
    let gone_signal = gone.notified();
    tokio::pin!(gone_signal);
    gone_signal.as_mut().enable();
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
    let route_label = format!("{method} {}", meta.route_pattern);
    let req_id = uuid::Uuid::new_v4().to_string();
    let rx = match bus.dispatch(&req_id, &method, &meta, &body_bytes) {
        Ok(rx) => rx,
        Err(e) => {
            inflight.report_dispatch_failure(&worker_name, &route_label, &e.to_string());
            return (StatusCode::BAD_GATEWAY, format!("SHM dispatch failed: {e}")).into_response();
        }
    };
    let _tracked = inflight.track(route_label.clone());

    // Wait for the response, the worker's departure, or the timeout —
    // whichever comes first.
    let timeout = Duration::from_secs(state.config.timeout_secs);
    let settled = async {
        tokio::select! {
            // Biased: if the response and the eviction land together, the
            // response is real and should win.
            biased;
            r = rx => Some(r),
            _ = gone_signal => None,
        }
    };
    match tokio::time::timeout(timeout, settled).await {
        Ok(None) => (
            StatusCode::BAD_GATEWAY,
            format!("Worker serving {path} went away"),
        )
            .into_response(),
        Ok(Some(Ok(resp))) => {
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
        Ok(Some(Err(_))) => (
            StatusCode::BAD_GATEWAY,
            "Worker dropped the request".to_string(),
        )
            .into_response(),
        Err(_) => {
            // The hub stops waiting here; the worker does not. Its slot stays
            // claimed until the handler returns, so a run of these is a worker
            // leaking slots in slow motion.
            tracing::warn!(
                worker = %worker_name,
                route = %route_label,
                timeout_secs = state.config.timeout_secs,
                "request timed out; its slot stays claimed until the worker answers"
            );
            (
                StatusCode::GATEWAY_TIMEOUT,
                format!(
                    "Request to {path} timed out after {}s",
                    state.config.timeout_secs
                ),
            )
                .into_response()
        }
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
    worker_name: String,
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
                worker_name: worker_name.clone(),
                path_pattern: route_pattern.clone(),
                params: path_params.clone(),
                sender: tx,
                connected_at,
            },
        );
    }

    tracing::info!(
        path,
        worker_name,
        connection_id,
        "SSE client connected (delegated)"
    );

    // Notify worker of the connection via SHM
    dispatch_sse_lifecycle(
        &state,
        &worker_name,
        "connect",
        &path,
        &route_pattern,
        &path_params,
    )
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
    let (worker_name, path_pattern, params, connected_at) = {
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
            info.worker_name.clone(),
            info.path_pattern.clone(),
            info.params.clone(),
            info.connected_at.clone(),
        );
        sse.remove(path);
        snapshot
    };

    tracing::info!(
        path,
        worker_name,
        reason,
        connected_at,
        "SSE client disconnected"
    );

    // By name, so the disconnect reaches whichever instance is serving that
    // worker now. Keyed by instance id this was the second half of the same
    // defect: a connection that outlived its worker carried a dead id, so the
    // eventual disconnect was dispatched into nothing and the live worker never
    // heard it — leaving a channel marked connected with no client behind it.
    dispatch_sse_lifecycle(
        state,
        &worker_name,
        "disconnect",
        path,
        &path_pattern,
        &params,
    )
    .await;
}

/// Every SSE connection belonging to `worker_name`, as `(path, params, pattern)`.
///
/// Deliberately a pure function of the connection map alone. It consults no
/// worker record, so the set it returns cannot shrink because a worker was
/// evicted, slept, crashed, or stayed away longer than some timeout — which is
/// exactly how the replay used to lose connections.
fn connections_owned_by(
    sse: &HashMap<String, SseConnectionInfo>,
    worker_name: &str,
) -> Vec<(String, HashMap<String, String>, String)> {
    sse.iter()
        .filter(|(_, info)| info.worker_name == worker_name)
        .map(|(path, info)| (path.clone(), info.params.clone(), info.path_pattern.clone()))
        .collect()
}

/// Dispatch an SSE lifecycle notification (connect/disconnect) to a worker via SHM.
///
/// Resolved by worker **name**: whichever instance currently answers for that
/// name is the one that needs to hear about the stream. Looking it up by
/// instance id meant a notification for a connection that had outlived one
/// worker process could never be delivered to its replacement.
async fn dispatch_sse_lifecycle(
    state: &HubState,
    worker_name: &str,
    lifecycle: &str,
    path: &str,
    route_pattern: &str,
    params: &HashMap<String, String>,
) {
    let workers = state.workers.read().await;
    let bus = match workers
        .values()
        .find(|record| record.name == worker_name)
        .map(|record| Arc::clone(&record.bus))
    {
        Some(bus) => bus,
        None => {
            // Not an error: the worker is down right now. The connection stays
            // in the map, and `register_worker` replays it by name as soon as
            // a process answers for that name again.
            tracing::warn!(
                worker_name,
                lifecycle,
                path,
                "cannot send SSE lifecycle: no live worker under that name"
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

    /// The table must empty itself on every way out of the proxy, or the
    /// exhaustion log blames routes that finished long ago.
    #[test]
    fn inflight_entries_leave_when_the_guard_drops() {
        let table = Arc::new(InFlight::default());
        let a = table.track("GET /agent/online".into());
        let b = table.track("GET /agent/online".into());
        let c = table.track("POST /agent/mail/send".into());

        let held = table.by_route();
        assert_eq!(held[0].0, "GET /agent/online");
        assert_eq!(held[0].1, 2);
        assert_eq!(held[1], ("POST /agent/mail/send".into(), 1, held[1].2));

        drop(a);
        drop(c);
        assert_eq!(table.by_route().len(), 1);
        assert_eq!(table.by_route()[0].1, 1);
        drop(b);
        assert!(table.by_route().is_empty());
    }

    /// A storm logs one snapshot per window and COUNTS the rest — a dropped
    /// failure would understate the storm, which is how this went unseen.
    #[test]
    fn dispatch_failures_inside_the_window_are_counted_not_lost() {
        let table = Arc::new(InFlight::default());
        table.report_dispatch_failure("agent", "GET /x", "no free slots");
        for _ in 0..4 {
            table.report_dispatch_failure("agent", "GET /x", "no free slots");
        }
        assert_eq!(
            table.suppressed.load(std::sync::atomic::Ordering::Relaxed),
            4
        );
        // Once the window has passed, the next report carries the count and resets it.
        *table.last_report.lock().unwrap() =
            Some(std::time::Instant::now() - EXHAUSTION_REPORT_INTERVAL);
        table.report_dispatch_failure("agent", "GET /x", "no free slots");
        assert_eq!(
            table.suppressed.load(std::sync::atomic::Ordering::Relaxed),
            0
        );
    }

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
        insert_conn_for(state, path, connection_id, "test-worker").await
    }

    /// Same, but under a named owner, for the replay-selection tests.
    async fn insert_conn_for(
        state: &HubState,
        path: &str,
        connection_id: &str,
        worker_name: &str,
    ) -> mpsc::Receiver<SseEvent> {
        let (tx, rx) = mpsc::channel::<SseEvent>(16);
        let mut sse = state.sse_connections.write().await;
        sse.insert(
            path.to_string(),
            SseConnectionInfo {
                connection_id: connection_id.to_string(),
                worker_name: worker_name.to_string(),
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

    /// A worker name becomes part of a shared-memory region name, which on
    /// Windows reaches the filesystem. Anything that could steer a path must be
    /// refused at the door.
    #[test]
    fn unsafe_worker_names_are_rejected() {
        for bad in [
            "",
            "../../../etc",
            "..",
            "a/b",
            r"a\b",
            "C:/windows/system32",
            "agent\0null",
            "hub-../../../victim",
        ] {
            assert!(
                invalid_worker_name(bad).is_some(),
                "should have rejected {bad:?}"
            );
        }
    }

    /// Guards against over-blocking: the names actually in use must survive.
    /// Without this the test above would pass if the check rejected everything.
    #[test]
    fn ordinary_worker_names_are_accepted() {
        for good in [
            "agent",
            "tts",
            "stt",
            "ui",
            "discord",
            "project-store",
            "node-api",
            "sse-test",
            "agent_2",
            "Agent.v2",
        ] {
            assert_eq!(
                invalid_worker_name(good),
                None,
                "should have accepted {good:?}"
            );
        }
    }

    // ---- SSE replay selection ------------------------------------------------
    //
    // The regression these cover: replay used to be scoped to workers still
    // present in `state.workers` under the same name. `evict_worker` removes
    // that record after ~30s of silence without touching `sse_connections`, so
    // any restart slower than the reaper — i.e. every rebuild — found nothing to
    // replay and stranded every live client on a dead worker id, permanently and
    // silently.

    #[tokio::test]
    async fn replay_selection_survives_eviction_of_the_owning_worker() {
        let state = make_state();
        let _rx_a = insert_conn_for(&state, "/agent/events/a", "conn-a", "agent").await;
        let _rx_b = insert_conn_for(&state, "/agent/events/b", "conn-b", "agent").await;

        // The worker is gone entirely — no record, nothing to be "stale"
        // against. This is the state a rebuild leaves behind.
        evict_worker(&state, "some-worker-id-that-was-never-registered").await;

        let sse = state.sse_connections.read().await;
        let replay = connections_owned_by(&sse, "agent");
        assert_eq!(
            replay.len(),
            2,
            "both connections must be replayed to the returning worker, \
             regardless of how long it was away"
        );
    }

    #[tokio::test]
    async fn replay_selection_does_not_adopt_another_workers_connections() {
        let state = make_state();
        let _rx_agent = insert_conn_for(&state, "/agent/events/a", "conn-a", "agent").await;
        let _rx_term = insert_conn_for(&state, "/term/events/t", "conn-t", "term").await;

        let sse = state.sse_connections.read().await;

        // A `term` registration must not inherit an orphaned `agent` stream and
        // point it at a worker with no such route — the hazard in matching
        // negatively ("belongs to no live worker") instead of by name.
        let term = connections_owned_by(&sse, "term");
        assert_eq!(term.len(), 1);
        assert_eq!(term[0].0, "/term/events/t");

        let agent = connections_owned_by(&sse, "agent");
        assert_eq!(agent.len(), 1);
        assert_eq!(agent[0].0, "/agent/events/a");
    }

    #[tokio::test]
    async fn replay_selection_carries_path_params_and_pattern() {
        let state = make_state();
        let (tx, _rx) = mpsc::channel::<SseEvent>(16);
        {
            let mut sse = state.sse_connections.write().await;
            sse.insert(
                "/agent/events/abc-123".to_string(),
                SseConnectionInfo {
                    connection_id: "conn-1".to_string(),
                    worker_name: "agent".to_string(),
                    path_pattern: "/agent/events/{sessionId}".to_string(),
                    params: HashMap::from([("sessionId".to_string(), "abc-123".to_string())]),
                    sender: tx,
                    connected_at: "0Z".to_string(),
                },
            );
        }

        let sse = state.sse_connections.read().await;
        let replay = connections_owned_by(&sse, "agent");
        assert_eq!(replay.len(), 1);
        // The worker rebuilds the channel from these; a replay that dropped the
        // params would reconnect the stream and still lose the session.
        assert_eq!(replay[0].1.get("sessionId").map(String::as_str), Some("abc-123"));
        assert_eq!(replay[0].2, "/agent/events/{sessionId}");
    }
}
