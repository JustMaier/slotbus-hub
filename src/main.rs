//! slotbus-hub — standalone HTTP-to-SHM router.
//!
//! Workers register routes via `POST /internal/register`. Clients send normal
//! HTTP requests. The hub dispatches them to the right worker via shared memory
//! and returns the response.

mod events;
mod router;
mod types;

use clap::Parser;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "slotbus-hub", about = "HTTP-to-SHM router for slotbus workers")]
struct Cli {
    /// HTTP listen port.
    #[arg(long, default_value = "3200")]
    port: u16,

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
    };

    let app = router::build_router(hub_config);

    let addr = format!("127.0.0.1:{}", cli.port);
    tracing::info!("Starting slotbus-hub on http://{addr}");

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .unwrap_or_else(|e| {
            tracing::error!("Failed to bind {addr}: {e}");
            std::process::exit(1);
        });

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
        tokio::signal::ctrl_c()
            .await
            .expect("ctrl-c handler");
        tracing::info!("Received Ctrl+C");
    }
}
