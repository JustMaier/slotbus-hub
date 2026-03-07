//! slotbus-hub — HTTP-to-SHM router with worker SDK.
//!
//! This crate is both the hub binary and a library for building workers.
//!
//! # Worker usage
//!
//! ```rust,ignore
//! use slotbus_hub::{HubWorker, HandlerResponse};
//!
//! let worker = HubWorker::new("http://localhost:3200", "my-worker", app_state)
//!     .route("GET",  "/status",       handlers::status)
//!     .route("POST", "/items/{id}",   handlers::update_item);
//!
//! let emitter = worker.emitter();
//! tokio::spawn(bridge_events(emitter, event_rx));
//!
//! worker.run().await?;
//! ```

pub mod types;
pub mod worker;

pub use types::*;
pub use worker::{HandlerResponse, HubEmitter, HubWorker};
