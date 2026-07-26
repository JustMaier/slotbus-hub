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

/// Shortest accepted secret, in bytes, after trimming.
///
/// 16 characters of any reasonable alphabet is far past brute-forcing over a
/// network, and the floor exists to catch config accidents — a truncated
/// substitution, a placeholder left in place — not to rate a passphrase.
pub const MIN_SECRET_LEN: usize = 16;

/// Why a configured `HUB_SECRET` was rejected.
#[derive(Debug, PartialEq, Eq)]
pub enum SecretError {
    /// Present but shorter than [`MIN_SECRET_LEN`] after trimming.
    TooShort { len: usize },
}

impl std::fmt::Display for SecretError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SecretError::TooShort { len } => write!(
                f,
                "HUB_SECRET is {len} characters after trimming; minimum is {MIN_SECRET_LEN}"
            ),
        }
    }
}

/// Normalise a raw `HUB_SECRET` value into the secret the hub will enforce.
///
/// Surrounding whitespace is stripped from the *stored* value, not merely
/// ignored for validation. That is deliberate: HTTP strips optional whitespace
/// around a header field-value in transit (RFC 9110 §5.5), so a secret with
/// meaningful leading or trailing spaces could never be transmitted intact and
/// would reject every client forever. Trimming makes the stored secret equal to
/// what any client can actually put on the wire.
///
/// Returns `Ok(None)` when the value is absent or blank — the caller decides
/// whether that is fatal, since it only is for a non-loopback bind.
pub fn resolve_secret(raw: Option<String>) -> Result<Option<String>, SecretError> {
    let Some(raw) = raw else {
        return Ok(None);
    };

    let trimmed = raw.trim();
    if trimmed.is_empty() {
        // Covers both "" and whitespace-only, which must be treated the same:
        // a blank-but-not-empty value is exactly what a deploy script emits
        // when the substitution it meant to make silently produced nothing.
        return Ok(None);
    }
    if trimmed.len() < MIN_SECRET_LEN {
        return Err(SecretError::TooShort {
            len: trimmed.len(),
        });
    }
    if trimmed.len() != raw.len() {
        tracing::warn!(
            "HUB_SECRET had surrounding whitespace; using the trimmed value. \
             Clients must send the trimmed secret — HTTP strips whitespace around header values."
        );
    }
    Ok(Some(trimmed.to_string()))
}

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

    #[test]
    fn resolve_secret_rules() {
        // Absent and blank are the same thing: no secret configured.
        assert_eq!(resolve_secret(None), Ok(None));
        assert_eq!(resolve_secret(Some(String::new())), Ok(None));
        assert_eq!(resolve_secret(Some("   ".into())), Ok(None));
        assert_eq!(resolve_secret(Some("\t\n ".into())), Ok(None));

        // Short-but-present is a hard error, not a silent acceptance.
        assert_eq!(
            resolve_secret(Some("short".into())),
            Err(SecretError::TooShort { len: 5 })
        );
        assert_eq!(
            resolve_secret(Some(format!("  {}  ", "a".repeat(15)))),
            Err(SecretError::TooShort { len: 15 })
        );

        // The stored value is the trimmed one, matching what HTTP can carry.
        let ok = "a".repeat(MIN_SECRET_LEN);
        assert_eq!(resolve_secret(Some(ok.clone())), Ok(Some(ok.clone())));
        assert_eq!(
            resolve_secret(Some(format!("  {ok}\n"))),
            Ok(Some(ok.clone()))
        );
    }

    // ---- Middleware behaviour ------------------------------------------------
    //
    // These drive the real router through `tower::ServiceExt::oneshot`, so they
    // exercise `require_secret` as an installed layer rather than its helpers.
    // `GET /health` is the probe: it is a hub-native handler that answers 200
    // with no workers registered, so a 200 proves the request reached a handler
    // and a 401 proves the layer stopped it. `/no/such/route` falls through to
    // `proxy_handler`, whose 502 ("no worker registered") is the marker that the
    // fallback — where every worker-registered path lives — is inside the layer.

    use axum::body::Body;
    use axum::http::{Request as HttpRequest, StatusCode};
    use tower::ServiceExt;

    const TEST_SECRET: &str = "test-secret-value-0123456789";

    fn test_router() -> axum::Router {
        crate::router::build_router(
            crate::router::HubConfig {
                timeout_secs: 1,
                num_slots: 4,
                region_size: 4096,
                instrumentation: false,
                // Keep the sweeper out of the test process.
                sse_sweep_interval_ms: 0,
            },
            AuthState::new(Some(TEST_SECRET.to_string())),
        )
    }

    /// Build a request, optionally attaching a peer address the way
    /// `into_make_service_with_connect_info` does at runtime.
    fn request(path: &str, peer: Option<&str>, secret: Option<&str>) -> HttpRequest<Body> {
        let mut req = HttpRequest::builder().uri(path);
        if let Some(s) = secret {
            req = req.header(SECRET_HEADER, s);
        }
        let mut req = req.body(Body::empty()).unwrap();
        if let Some(peer) = peer {
            let addr: SocketAddr = peer.parse().unwrap();
            req.extensions_mut().insert(ConnectInfo(addr));
        }
        req
    }

    async fn status_of(req: HttpRequest<Body>) -> StatusCode {
        test_router().oneshot(req).await.unwrap().status()
    }

    #[tokio::test]
    async fn loopback_peer_passes_without_header() {
        // The exemption every local client depends on. This one cannot fail if
        // the layer is deleted — it pins the opposite direction, catching a
        // change that starts rejecting loopback traffic.
        assert_eq!(
            status_of(request("/health", Some("127.0.0.1:5555"), None)).await,
            StatusCode::OK
        );
    }

    #[tokio::test]
    async fn missing_connect_info_is_denied() {
        // Without a peer address the loopback exemption is undecidable, so the
        // only safe answer is no. Fails if the layer is removed.
        assert_eq!(
            status_of(request("/health", None, Some(TEST_SECRET))).await,
            StatusCode::UNAUTHORIZED
        );
    }

    #[tokio::test]
    async fn remote_peer_with_wrong_secret_is_denied() {
        // Fails if the layer is removed.
        for bad in [None, Some(""), Some("wrong"), Some("test-secret-value-012345678")] {
            assert_eq!(
                status_of(request("/health", Some("192.168.0.9:5555"), bad)).await,
                StatusCode::UNAUTHORIZED,
                "expected 401 for secret {bad:?}"
            );
        }
    }

    #[tokio::test]
    async fn remote_peer_with_correct_secret_passes() {
        // Guards against over-blocking; like the loopback case it cannot fail
        // if the layer is deleted.
        assert_eq!(
            status_of(request("/health", Some("192.168.0.9:5555"), Some(TEST_SECRET))).await,
            StatusCode::OK
        );
    }

    #[tokio::test]
    async fn fallback_proxy_route_is_behind_the_layer() {
        // The fallback carries every worker-registered path, including SSE, so
        // this is the coverage that matters most. Unauthenticated must stop at
        // the layer (401) rather than reach the proxy (502). Fails if the layer
        // is removed — the 401 becomes a 502.
        assert_eq!(
            status_of(request("/agent/speak", Some("192.168.0.9:5555"), None)).await,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            status_of(request(
                "/agent/speak",
                Some("192.168.0.9:5555"),
                Some(TEST_SECRET)
            ))
            .await,
            StatusCode::BAD_GATEWAY
        );
    }
}
