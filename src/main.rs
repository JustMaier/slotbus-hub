//! slotbus-hub — standalone HTTP-to-SHM router.
//!
//! Workers register routes via `POST /internal/register`. Clients send normal
//! HTTP requests. The hub dispatches them to the right worker via shared memory
//! and returns the response.

mod auth;
mod events;
mod router;

use std::net::{IpAddr, SocketAddr};

use clap::Parser;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "slotbus-hub", about = "HTTP-to-SHM router for slotbus workers")]
struct Cli {
    /// HTTP listen port.
    #[arg(long, default_value = "3200")]
    port: u16,

    /// IP address to bind. Defaults to loopback; use `0.0.0.0` (or a specific
    /// LAN address) to accept remote clients. Any non-loopback bind requires
    /// `HUB_SECRET` to be set — see the auth module.
    #[arg(long, env = "HUB_BIND", default_value = "127.0.0.1")]
    bind: IpAddr,

    /// Request timeout in seconds.
    #[arg(long, default_value = "30")]
    timeout: u64,

    /// Number of SHM slots per worker.
    #[arg(long, default_value = "32")]
    slots: usize,

    /// SHM region size per worker in bytes.
    #[arg(long, default_value = "1048576")]
    region_size: usize,

    /// Enable latency instrumentation logging.
    #[arg(long)]
    instrumentation: bool,

    /// SSE liveness sweep interval in milliseconds.
    ///
    /// The hub probes every hub-managed SSE connection on this interval and
    /// notifies the registered worker via `sse_lifecycle: "disconnect"` when
    /// it detects a dead client. Set to `0` to disable and fall back to
    /// axum's built-in keepalive only.
    #[arg(long, default_value_t = router::DEFAULT_SSE_SWEEP_INTERVAL_MS)]
    sse_sweep_interval_ms: u64,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .init();

    let hub_config = router::HubConfig {
        timeout_secs: cli.timeout,
        num_slots: cli.slots,
        region_size: cli.region_size,
        instrumentation: cli.instrumentation,
        sse_sweep_interval_ms: cli.sse_sweep_interval_ms,
    };

    let addr = SocketAddr::new(cli.bind, cli.port);
    let loopback_only = auth::is_loopback_peer(cli.bind);

    // A secret is required whenever the hub is reachable from off-box. Refusing
    // to start is deliberate: silently serving an unauthenticated API on a LAN
    // address is the failure mode worth being loud about.
    let secret = std::env::var("HUB_SECRET").ok().filter(|s| !s.is_empty());
    if !loopback_only && secret.is_none() {
        tracing::error!(
            "Refusing to start: bind address {addr} is not loopback and HUB_SECRET is unset or empty. \
             Set HUB_SECRET to a shared secret, or bind to 127.0.0.1."
        );
        std::process::exit(1);
    }

    let auth_state = auth::AuthState::new(secret);
    let app = router::build_router(hub_config, auth_state.clone());

    tracing::info!(
        "Starting slotbus-hub on http://{addr} (auth: {})",
        if auth_state.is_enforcing() {
            "X-Hub-Secret required for non-loopback peers"
        } else {
            "disabled, loopback-only bind"
        }
    );

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .unwrap_or_else(|e| {
            tracing::error!("Failed to bind {addr}: {e}");
            std::process::exit(1);
        });

    // `into_make_service_with_connect_info` is what makes the real peer address
    // available to the auth middleware.
    let app = app.into_make_service_with_connect_info::<SocketAddr>();

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .unwrap_or_else(|e| {
            tracing::error!("Server error: {e}");
        });

    tracing::info!("Shut down");
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm = signal(SignalKind::terminate()).expect("SIGTERM handler");
        let mut sigint = signal(SignalKind::interrupt()).expect("SIGINT handler");
        tokio::select! {
            _ = sigterm.recv() => tracing::info!("Received SIGTERM"),
            _ = sigint.recv() => tracing::info!("Received SIGINT"),
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await.expect("ctrl-c handler");
        tracing::info!("Received Ctrl+C");
    }
}
