//! Example slotbus worker in Rust.
//!
//! Registers routes with the hub, opens the SHM region, and handles requests.
//!
//! Usage:
//!   cargo run -p slotbus-example-worker
//!
//! Requires slotbus-hub running on localhost:3200.

use std::io::Read;
use std::sync::Arc;

use slotbus::{SlotBusConfig, SlotWorker};

fn main() {
    let hub_url = std::env::var("HUB_URL").unwrap_or_else(|_| "http://localhost:3200".into());
    let worker_name = "rust-api";

    // 1. Register routes with the hub
    let routes = serde_json::json!({
        "name": worker_name,
        "routes": [
            { "method": "GET",  "path": "/status" },
            { "method": "POST", "path": "/echo" },
            { "method": "GET",  "path": "/items/{id}" },
        ]
    });

    let body_str = routes.to_string();
    let mut resp_reader = ureq::post(&format!("{hub_url}/internal/register"))
        .set("Content-Type", "application/json")
        .send_bytes(body_str.as_bytes())
        .expect("failed to register with hub")
        .into_reader();
    let mut resp_body = String::new();
    resp_reader
        .read_to_string(&mut resp_body)
        .expect("failed to read response");
    let resp: serde_json::Value =
        serde_json::from_str(&resp_body).expect("invalid JSON");

    let shm_name = resp["shm_name"].as_str().expect("no shm_name in response");
    println!("Registered as '{worker_name}', SHM region: {shm_name}");

    // 2. Derive prefix from shm_name (e.g. "slotbus-rust-api" → prefix "slotbus")
    let prefix = shm_name
        .strip_suffix(&format!("-{worker_name}"))
        .unwrap_or("slotbus");

    let config = SlotBusConfig::builder()
        .name(worker_name)
        .prefix(prefix)
        .build();

    let worker = Arc::new(SlotWorker::open(config).expect("failed to open SHM"));
    println!("SHM opened, waiting for requests...");

    // 3. Handle requests
    let handle = worker.clone().start_receive_loop(|w, slot, req| {
        let (status, body, ct) = match (req.method.as_str(), req.route_pattern.as_str()) {
            ("GET", "/status") => {
                let json = serde_json::json!({ "ok": true, "worker": "rust" });
                (200, json.to_string().into_bytes(), "application/json")
            }
            ("POST", "/echo") => (200, req.body.clone(), "application/json"),
            ("GET", "/items/{id}") => {
                let id = req.path_params.get("id").map(|s| s.as_str()).unwrap_or("?");
                let json = serde_json::json!({ "id": id, "name": format!("Item {id}") });
                (200, json.to_string().into_bytes(), "application/json")
            }
            _ => (404, b"Not Found".to_vec(), "text/plain"),
        };

        if let Err(e) = w.send_response(slot, status, body, ct, vec![]) {
            eprintln!("send_response error: {e}");
        }
    });

    // 4. Block until Ctrl+C
    println!("Press Ctrl+C to stop.");
    handle.join().expect("receive loop panicked");
}
