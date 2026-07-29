//! Example slotbus worker using the HubWorker SDK.
//!
//! Usage:
//!   cargo run -p slotbus-example-worker
//!
//! Requires slotbus-hub running on localhost:3200.

use std::collections::HashMap;
use std::sync::Arc;

use slotbus_hub::{HandlerResponse, HubWorker};

struct AppState;

async fn status(
    _state: Arc<AppState>,
    _params: HashMap<String, String>,
    _query: Option<String>,
    _body: Vec<u8>,
) -> HandlerResponse {
    HandlerResponse::ok_json(&serde_json::json!({ "ok": true, "worker": "rust" }))
}

async fn echo(
    _state: Arc<AppState>,
    _params: HashMap<String, String>,
    _query: Option<String>,
    body: Vec<u8>,
) -> HandlerResponse {
    HandlerResponse::bytes(200, body, "application/json")
}

async fn get_item(
    _state: Arc<AppState>,
    params: HashMap<String, String>,
    _query: Option<String>,
    _body: Vec<u8>,
) -> HandlerResponse {
    let id = params.get("id").map(|s| s.as_str()).unwrap_or("?");
    HandlerResponse::ok_json(&serde_json::json!({ "id": id, "name": format!("Item {id}") }))
}

#[tokio::main]
async fn main() {
    let hub_url = std::env::var("HUB_URL").unwrap_or_else(|_| "http://localhost:3200".into());

    let worker = HubWorker::new(&hub_url, "rust-api", AppState)
        .route("GET", "/status", status)
        .route("POST", "/echo", echo)
        .route("GET", "/items/{id}", get_item);

    eprintln!("Starting worker, connecting to hub at {hub_url}...");

    if let Err(e) = worker.run().await {
        eprintln!("Worker error: {e}");
        std::process::exit(1);
    }
}
