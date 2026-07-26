//! Shared-secret authentication for non-loopback callers.
//!
//! The hub was built for loopback-only use: every local client (the Tauri app,
//! the TTS/STT workers, the agent server, MCP scripts) connects over
//! `127.0.0.1` with no credentials. Exposing the hub on a LAN address must not
//! break any of that, so the policy is:
//!
//! * Peers on `127.0.0.0/8` or `::1` are exempt — no header, no change.
//! * Everyone else must present `X-Hub-Secret` matching `HUB_SECRET`.
//!
//! The peer address always comes from the accepted TCP connection
//! (`ConnectInfo<SocketAddr>`), never from a request header. `X-Forwarded-For`
//! and friends are attacker-controlled and are deliberately ignored.

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use axum::{
    extract::{ConnectInfo, Request, State},
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
};
use subtle::ConstantTimeEq;

/// Header carrying the shared secret.
pub const SECRET_HEADER: &str = "x-hub-secret";

/// Auth policy handed to the middleware.
///
/// `None` means no secret is configured, which is only reachable when the hub
/// is bound to a loopback address (startup refuses otherwise). In that mode the
/// middleware is a pass-through, preserving the pre-auth behaviour byte for
/// byte.
#[derive(Clone)]
pub struct AuthState {
    secret: Option<Arc<Vec<u8>>>,
}

impl AuthState {
    pub fn new(secret: Option<String>) -> Self {
        Self {
            secret: secret.map(|s| Arc::new(s.into_bytes())),
        }
    }

    pub fn is_enforcing(&self) -> bool {
        self.secret.is_some()
    }
}

/// True for `127.0.0.0/8`, `::1`, and IPv4-mapped loopback (`::ffff:127.0.0.1`).
///
/// The mapped case matters on dual-stack listeners: a v4 client arriving on a
/// v6 socket shows up as `::ffff:127.0.0.1`, whose `is_loopback()` is `false`.
pub fn is_loopback_peer(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => v4.is_loopback(),
        IpAddr::V6(v6) => {
            v6.is_loopback() || v6.to_ipv4_mapped().is_some_and(|m| m.is_loopback())
        }
    }
}

/// Constant-time secret comparison. `subtle` short-circuits on length mismatch
/// only; equal-length candidates are compared without data-dependent branching.
fn secret_matches(expected: &[u8], provided: &[u8]) -> bool {
    expected.ct_eq(provided).into()
}

/// Auth middleware. Applied as a whole-router layer so it wraps every route
/// *and* the fallback proxy handler.
pub async fn require_secret(
    State(auth): State<AuthState>,
    req: Request,
    next: Next,
) -> Response {
    let Some(expected) = auth.secret.clone() else {
        // Loopback-only deployment: nothing to enforce.
        return next.run(req).await;
    };

    // Peer address comes from the TCP connection, not from any header.
    let peer = req
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ConnectInfo(addr)| *addr);

    match peer {
        Some(addr) if is_loopback_peer(addr.ip()) => return next.run(req).await,
        Some(_) => {}
        None => {
            // Serving without connect-info would make the loopback exemption
            // undecidable. Fail closed rather than guess.
            tracing::error!("auth: no ConnectInfo on request; denying");
            return unauthorized();
        }
    }

    let ok = {
        let provided = req
            .headers()
            .get(SECRET_HEADER)
            .map(|v| v.as_bytes())
            .unwrap_or(&[]);
        secret_matches(&expected, provided)
    };

    if ok {
        next.run(req).await
    } else {
        let peer = peer.map(|a| a.to_string()).unwrap_or_default();
        tracing::warn!(
            "auth: rejected {} {} from {peer} (bad or missing {SECRET_HEADER})",
            req.method(),
            req.uri().path()
        );
        unauthorized()
    }
}

fn unauthorized() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        [("www-authenticate", "X-Hub-Secret")],
        "unauthorized: missing or invalid X-Hub-Secret\n",
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loopback_detection() {
        assert!(is_loopback_peer("127.0.0.1".parse().unwrap()));
        assert!(is_loopback_peer("127.5.6.7".parse().unwrap()));
        assert!(is_loopback_peer("::1".parse().unwrap()));
        assert!(is_loopback_peer("::ffff:127.0.0.1".parse().unwrap()));
        assert!(!is_loopback_peer("192.168.0.9".parse().unwrap()));
        assert!(!is_loopback_peer("0.0.0.0".parse().unwrap()));
        assert!(!is_loopback_peer("::ffff:192.168.0.9".parse().unwrap()));
    }

    #[test]
    fn secret_compare() {
        assert!(secret_matches(b"abc", b"abc"));
        assert!(!secret_matches(b"abc", b"abd"));
        assert!(!secret_matches(b"abc", b""));
        assert!(!secret_matches(b"abc", b"abcd"));
    }
}
