//! Browser-origin guard: a loopback hub must not be drivable by a web page.
//!
//! The secret layer in `auth.rs` exempts every loopback peer, because the local
//! clients (the Tauri app, the workers, the MCP scripts) connect over
//! `127.0.0.1` with no credential. But a *browser* open on the same machine is
//! also a loopback peer. With no further check, any web page — a malicious
//! localhost dev server, a `file://` page, or a public site that a browser
//! lets reach `127.0.0.1` — can fire cross-origin requests at the hub:
//!
//!   * a `no-cors` / form `POST` is fire-and-forget. The page cannot read the
//!     reply, but the hub still dispatches it. For a real route that is an
//!     inject, a spawn, a kill, an answer.
//!   * `CorsLayer::permissive()` echoed `access-control-allow-origin: *`, so a
//!     `GET` response body (e.g. the roster) was readable cross-origin too.
//!
//! This module closes all of that with three cheap checks that run on every
//! request, in front of the secret layer, and an explicit CORS allow-list.
//! The distinguishing fact a browser cannot hide is that it *always* attaches
//! `Origin` on a cross-origin request (including no-cors and form posts) — so
//! the write path (every state-changing POST) is fully covered. Modern
//! browsers also attach `Sec-Fetch-*` on every fetch, but that is NOT universal
//! (Safari < 16.4 and pre-2023 engines omit it), so check C is a best-effort
//! net for origin-less GETs and is backed by the standing rule that no proxied
//! GET route may mutate state. A non-browser client (curl, node/undici, the
//! worker SDK) sends neither header, so the guard is inert for them.
//!
//! The three checks:
//!   A. **Host / DNS-rebinding.** For a loopback peer, the `Host` header must
//!      name a trusted loopback vhost (`localhost`, `127.x`, `[::1]`,
//!      `tauri.localhost`). This blocks a rebinding page (`attacker.com` →
//!      `127.0.0.1`) whose request is *same-origin* and so carries no `Origin`.
//!   B. **Origin allow-list.** A present `Origin` must be on the allow-list or
//!      equal the request's own authority (a page the hub serves itself).
//!   C. **Origin-less cross-site fetch.** A cross/same-site fetch with no
//!      allowed `Origin` is refused unless it is a media subresource
//!      (`<img>`/`<audio>`/`<video>` — the desktop loads persona avatars that
//!      way, with no `Origin` and `Sec-Fetch-Site: cross-site`).

use std::net::{IpAddr, SocketAddr};
use std::sync::OnceLock;

use axum::{
    extract::{ConnectInfo, Request},
    http::{header, HeaderValue, Method, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use tower_http::cors::{Any, AllowOrigin, CorsLayer};

use crate::auth::is_loopback_peer;

/// Origins the hub's own trusted browser surfaces load from.
///
/// Keep this list short and literal. Anything dynamic (a dev server on a
/// random port, a mobile `mserve` proxy) goes through `HUB_EXTRA_ORIGINS`
/// rather than a wildcard, because a wildcard over `localhost:*` would re-admit
/// exactly the attacker this guard exists to stop — a page on any localhost
/// port.
const BUILTIN_ALLOWED_ORIGINS: &[&str] = &[
    // Tauri v2 webview. Windows defaults to `http://tauri.localhost`
    // (`useHttpsScheme` is unset in the app); macOS/Linux use `tauri://`.
    // Both schemes are listed so flipping `useHttpsScheme` later cannot lock
    // the app out of its own hub.
    //
    // NOTE (unverified on macOS): WebKit may send `Origin: null` for a fetch
    // from a `tauri://` page to an external `http://` origin. If it does, the
    // Mac app would be refused here and needs a different signal. The primary
    // (Windows) sends `http://tauri.localhost`, a normal origin, so this is a
    // Mac-only risk to spot-check on hardware before relying on the app there.
    "http://tauri.localhost",
    "https://tauri.localhost",
    "tauri://localhost",
    // The phone, reached through nginx at this name (nginx also adds the
    // shared secret, and arrives as a non-loopback peer).
    "https://hub.justinmaier.com",
];

/// Dev-only origins, trusted **only** when `HUB_ALLOW_DEV_ORIGINS` is set.
///
/// The Vite dev server binds a fixed port (1420); the desktop Svelte fetches
/// the hub at a hard-coded `http://localhost:3200`, so in DEV_URL/HMR mode its
/// page origin is 1420, and worktree views proxy through 1420 and share it.
/// But 1420 is a *fixed localhost port* — trusting it unconditionally would let
/// any local process that binds 1420 drive the hub, which is exactly the
/// "malicious localhost dev server" this guard exists to stop. So it is gated:
/// the dev-server that launches the hub in HMR mode sets the flag; a plain
/// (non-dev) hub does not trust 1420 at all.
const DEV_ALLOWED_ORIGINS: &[&str] = &["http://localhost:1420", "http://127.0.0.1:1420"];

/// Whether `HUB_ALLOW_DEV_ORIGINS` enables [`DEV_ALLOWED_ORIGINS`]. Read once.
///
/// Enabled only by an explicit affirmative (`1`/`true`/`yes`/`on`,
/// case-insensitive). An allow-listing flag in security code must not be
/// enabled by `HUB_ALLOW_DEV_ORIGINS=off` or `=no`, which a "truthy = non-empty"
/// test would wrongly accept.
fn dev_origins_enabled() -> bool {
    static FLAG: OnceLock<bool> = OnceLock::new();
    *FLAG.get_or_init(|| {
        std::env::var("HUB_ALLOW_DEV_ORIGINS")
            .ok()
            .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
            .unwrap_or(false)
    })
}

/// Extra origins from `HUB_EXTRA_ORIGINS` (comma-separated), read once.
///
/// Empty by default. This is how mobile `mserve` testing admits its
/// `http://127.0.0.1:<port>` page without a wildcard: the operator names the
/// exact origin for the life of that test hub.
fn extra_origins() -> &'static [String] {
    static EXTRA: OnceLock<Vec<String>> = OnceLock::new();
    EXTRA.get_or_init(|| {
        std::env::var("HUB_EXTRA_ORIGINS")
            .ok()
            .map(|s| {
                s.split(',')
                    .map(|x| x.trim().to_string())
                    .filter(|x| !x.is_empty())
                    .collect()
            })
            .unwrap_or_default()
    })
}

/// Is `origin` a trusted browser origin? A literal `null` (file://, sandboxed
/// iframe, `data:` URL) is never trusted.
pub fn is_allowed_origin(origin: &str) -> bool {
    is_allowed_origin_inner(origin, dev_origins_enabled())
}

/// Allow-list decision with the dev-origin gate passed explicitly, so a test
/// can exercise both states without the process-global `OnceLock`.
fn is_allowed_origin_inner(origin: &str, dev_enabled: bool) -> bool {
    let origin = origin.trim();
    if origin.is_empty() || origin.eq_ignore_ascii_case("null") {
        return false;
    }
    BUILTIN_ALLOWED_ORIGINS
        .iter()
        .any(|o| o.eq_ignore_ascii_case(origin))
        || (dev_enabled
            && DEV_ALLOWED_ORIGINS
                .iter()
                .any(|o| o.eq_ignore_ascii_case(origin)))
        || extra_origins().iter().any(|o| o.eq_ignore_ascii_case(origin))
}

/// The hostname part of a `Host` header, minus any port and IPv6 brackets.
fn host_name(host_header: &str) -> &str {
    let host = host_header.trim();
    if let Some(rest) = host.strip_prefix('[') {
        // IPv6 literal: `[addr]:port` or `[addr]`.
        return rest.split_once(']').map(|(a, _)| a).unwrap_or(rest);
    }
    // A bare (unbracketed) IPv6 literal has multiple colons and no port; take
    // it whole rather than mis-splitting `::1` into `::`. Spec requires IPv6
    // Hosts to be bracketed, so this is belt-and-suspenders for a lenient
    // client, and still fail-closed (an unparseable result is simply untrusted).
    if host.matches(':').count() > 1 && host.parse::<std::net::Ipv6Addr>().is_ok() {
        return host;
    }
    // `name:port` / `ipv4:port` / bare — only strip a trailing all-digit port.
    match host.rsplit_once(':') {
        Some((name, port)) if !port.is_empty() && port.bytes().all(|b| b.is_ascii_digit()) => name,
        _ => host,
    }
}

/// A loopback request may only address a trusted loopback vhost. Blocks DNS
/// rebinding: `attacker.com` resolving to `127.0.0.1` arrives with
/// `Host: attacker.com`, which is neither a loopback literal nor `localhost`.
fn is_trusted_loopback_host(host_header: &str) -> bool {
    let name = host_name(host_header);
    if name.eq_ignore_ascii_case("localhost") || name.eq_ignore_ascii_case("tauri.localhost") {
        return true;
    }
    name.parse::<IpAddr>().map(is_loopback_peer).unwrap_or(false)
}

/// Does `origin` (scheme://authority) have the same authority as the request's
/// own `Host`? Lets a page the hub serves itself (the `/m` bundle on
/// `localhost:3200`) POST to its own origin. Safe because the browser sets the
/// `Host` to the connection target (a page cannot forge it to match a
/// cross-origin), and the two backstops are already in place: for a loopback
/// peer, check A has proven `Host` is a trusted loopback name; for a
/// non-loopback peer (check A is skipped), the secret layer still applies —
/// a non-loopback bind refuses to start without `HUB_SECRET` (main.rs).
fn origin_matches_host(origin: &str, host: &str) -> bool {
    origin
        .split_once("://")
        .map(|(_, authority)| authority.eq_ignore_ascii_case(host.trim()))
        .unwrap_or(false)
}

fn forbidden(reason: &str, method: &Method, path: &str) -> Response {
    tracing::warn!("origin-guard: refused {method} {path} ({reason})");
    (
        StatusCode::FORBIDDEN,
        "forbidden: request origin not allowed\n",
    )
        .into_response()
}

/// The guard middleware. Installed in front of the secret layer so cross-origin
/// browser traffic is refused before it reaches either auth or dispatch.
pub async fn require_trusted_origin(req: Request, next: Next) -> Response {
    let peer_loopback = req
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ConnectInfo(a)| is_loopback_peer(a.ip()))
        .unwrap_or(false);

    let headers = req.headers();
    let origin = headers
        .get(header::ORIGIN)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let host = headers
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let sec_fetch_site = headers
        .get("sec-fetch-site")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_ascii_lowercase());
    let sec_fetch_dest = headers
        .get("sec-fetch-dest")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_ascii_lowercase());

    let method = req.method().clone();
    let path = req.uri().path().to_owned();

    // ── A. Host / DNS-rebinding — loopback peers only ────────────────────────
    // A non-loopback peer (the nginx bridge) is authenticated by the secret
    // layer and legitimately carries its own `Host` (hub.justinmaier.com), so
    // this check does not apply to it.
    // Reject only a Host that is *present and untrusted* — that is the
    // rebinding signature (`Host: attacker.com`). A browser fetch to the hub
    // always carries a Host, so an absent Host cannot be a rebinding page; it
    // is a non-browser or malformed client, left for auth/routing to handle
    // (and it preserves the pre-guard behaviour of the no-Host local clients).
    if peer_loopback {
        if let Some(h) = host.as_deref() {
            if !is_trusted_loopback_host(h) {
                return forbidden("untrusted host", &method, &path);
            }
        }
    }

    // ── B. Origin allow-list ─────────────────────────────────────────────────
    if let Some(o) = origin.as_deref() {
        if is_allowed_origin(o) {
            return next.run(req).await;
        }
        // A page the hub serves itself POSTs with Origin == its own authority.
        if host.as_deref().is_some_and(|h| origin_matches_host(o, h)) {
            return next.run(req).await;
        }
        return forbidden("untrusted origin", &method, &path);
    }

    // ── C. Origin-less cross-site fetch ──────────────────────────────────────
    // No Origin means a non-browser client (curl/node/MCP/worker SDK), a
    // same-origin top-level navigation, OR a cross-site GET with a side effect.
    // Gate the last one on Sec-Fetch-Site, allowing media subresources through
    // because the desktop loads avatars as <img> (and audio) with no Origin and
    // Sec-Fetch-Site: cross-site.
    //
    // The media exemption is restricted to SAFE methods (GET/HEAD): a real
    // media subresource is always a GET, and a POST/PUT/DELETE can never
    // legitimately be dest=image, so this denies a fire-and-forget mutation
    // dressed up with a media dest. It stays a fire-and-forget READ channel —
    // the page cannot read the reply (opaque, no ACAO) — which is harmless only
    // while no proxied GET route mutates state; that is a standing rule for the
    // hub, not something this layer can enforce alone (every proxied GET route
    // was a read when this shipped).
    if let Some(site) = sec_fetch_site.as_deref() {
        if site == "cross-site" || site == "same-site" {
            let dest = sec_fetch_dest.as_deref().unwrap_or("");
            let safe_method = method == Method::GET || method == Method::HEAD;
            if safe_method && matches!(dest, "image" | "audio" | "video") {
                return next.run(req).await;
            }
            // A top-level navigation (tapping a hub link in Discord, mail or a
            // push) is cross-site with no Origin and dest=document. Refusing it
            // made the phone page unreachable from any link. It is the same
            // fire-and-forget GET as a media load — the linking page never sees
            // the response — so it is admitted on the same terms: safe methods
            // only, under the standing no-mutating-GET rule.
            if safe_method && dest == "document" {
                return next.run(req).await;
            }
            return forbidden("origin-less cross-site fetch", &method, &path);
        }
    }

    next.run(req).await
}

/// Explicit CORS layer. Replaces `CorsLayer::permissive()`: it echoes
/// `access-control-allow-origin` only for an allow-listed origin, and sets no
/// `allow-credentials`, so a disallowed origin can neither write (its request
/// is refused by the guard) nor read (no ACAO, so the browser blocks the body).
/// Same-origin requests need no CORS and are unaffected. Methods and headers
/// stay broad — the restriction that matters is on the origin.
pub fn cors_layer() -> CorsLayer {
    CorsLayer::new()
        .allow_origin(AllowOrigin::predicate(
            |origin: &HeaderValue, _parts: &_| origin.to_str().map(is_allowed_origin).unwrap_or(false),
        ))
        .allow_methods(Any)
        .allow_headers(Any)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_name_parsing() {
        assert_eq!(host_name("localhost:3200"), "localhost");
        assert_eq!(host_name("localhost"), "localhost");
        assert_eq!(host_name("127.0.0.1:3200"), "127.0.0.1");
        assert_eq!(host_name("[::1]:3200"), "::1");
        assert_eq!(host_name("[::1]"), "::1");
        // Bare (unbracketed) IPv6 is taken whole, not split at the first colon.
        assert_eq!(host_name("::1"), "::1");
        assert_eq!(host_name("hub.justinmaier.com"), "hub.justinmaier.com");
        // A name that ends in a non-numeric ":suffix" is not a port.
        assert_eq!(host_name("tauri.localhost"), "tauri.localhost");
    }

    #[test]
    fn trusted_loopback_host() {
        assert!(is_trusted_loopback_host("localhost:3200"));
        assert!(is_trusted_loopback_host("127.0.0.1:3200"));
        assert!(is_trusted_loopback_host("127.5.6.7"));
        assert!(is_trusted_loopback_host("[::1]:3200"));
        assert!(is_trusted_loopback_host("tauri.localhost"));
        // DNS rebinding and unexpected vhosts.
        assert!(!is_trusted_loopback_host("attacker.com"));
        assert!(!is_trusted_loopback_host("evil.test:3200"));
        assert!(!is_trusted_loopback_host("192.168.0.9:3200"));
    }

    #[test]
    fn origin_allow_list() {
        // `_inner` lets us pin the dev gate; `dev=false` is the default/prod.
        let allow = |o| is_allowed_origin_inner(o, false);
        assert!(allow("http://tauri.localhost"));
        assert!(allow("https://tauri.localhost"));
        assert!(allow("tauri://localhost"));
        assert!(allow("https://hub.justinmaier.com"));
        // The probe, and everything else, is not.
        assert!(!allow("http://localhost:5215"));
        assert!(!allow("https://evil.example"));
        assert!(!allow("null"));
        assert!(!allow("NULL"));
        assert!(!allow(""));
    }

    #[test]
    fn dev_origins_are_gated() {
        // 1420 is a fixed localhost port: trusted only when the dev flag is on,
        // so a production hub does not trust whatever binds 1420.
        assert!(!is_allowed_origin_inner("http://localhost:1420", false));
        assert!(!is_allowed_origin_inner("http://127.0.0.1:1420", false));
        assert!(is_allowed_origin_inner("http://localhost:1420", true));
        assert!(is_allowed_origin_inner("http://127.0.0.1:1420", true));
        // The dev flag never launders a `null` or an arbitrary origin.
        assert!(!is_allowed_origin_inner("null", true));
        assert!(!is_allowed_origin_inner("http://localhost:5215", true));
    }

    #[test]
    fn same_origin_match() {
        assert!(origin_matches_host("http://localhost:3200", "localhost:3200"));
        assert!(origin_matches_host("http://127.0.0.1:3200", "127.0.0.1:3200"));
        assert!(!origin_matches_host("http://localhost:5215", "localhost:3200"));
    }

    // ---- Middleware behaviour, through the real router -----------------------
    //
    // Mirrors the auth tests: GET /health is a hub-native 200, so a 200 means
    // the request reached a handler and a 403 means the guard stopped it. The
    // test router's AuthState exempts loopback peers, so on a loopback peer the
    // ONLY thing that can produce a 403 here is this guard — which is the
    // positive control. Delete the guard layer and every `_is_refused` test
    // below flips to 200.

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
                sse_sweep_interval_ms: 0,
            },
            crate::auth::AuthState::new(Some(TEST_SECRET.to_string())),
        )
    }

    struct Req {
        method: Method,
        path: &'static str,
        peer: Option<&'static str>,
        host: Option<&'static str>,
        origin: Option<&'static str>,
        sec_fetch_site: Option<&'static str>,
        sec_fetch_dest: Option<&'static str>,
        secret: Option<&'static str>,
    }

    impl Req {
        fn get(peer: &'static str) -> Self {
            Req {
                method: Method::GET,
                path: "/health",
                peer: Some(peer),
                host: Some("localhost:3200"),
                origin: None,
                sec_fetch_site: None,
                sec_fetch_dest: None,
                secret: None,
            }
        }
        fn path(mut self, p: &'static str) -> Self {
            self.path = p;
            self
        }
        fn host(mut self, h: Option<&'static str>) -> Self {
            self.host = h;
            self
        }
        fn origin(mut self, o: &'static str) -> Self {
            self.origin = Some(o);
            self
        }
        fn fetch(mut self, site: &'static str, dest: &'static str) -> Self {
            self.sec_fetch_site = Some(site);
            self.sec_fetch_dest = Some(dest);
            self
        }
        fn secret(mut self, s: &'static str) -> Self {
            self.secret = Some(s);
            self
        }
        fn peer(mut self, p: Option<&'static str>) -> Self {
            self.peer = p;
            self
        }
        async fn status(self) -> StatusCode {
            let mut b = HttpRequest::builder().method(self.method).uri(self.path);
            if let Some(h) = self.host {
                b = b.header(header::HOST, h);
            }
            if let Some(o) = self.origin {
                b = b.header(header::ORIGIN, o);
            }
            if let Some(s) = self.sec_fetch_site {
                b = b.header("sec-fetch-site", s);
            }
            if let Some(d) = self.sec_fetch_dest {
                b = b.header("sec-fetch-dest", d);
            }
            if let Some(s) = self.secret {
                b = b.header(crate::auth::SECRET_HEADER, s);
            }
            let mut req = b.body(Body::empty()).unwrap();
            if let Some(p) = self.peer {
                req.extensions_mut()
                    .insert(ConnectInfo(p.parse::<SocketAddr>().unwrap()));
            }
            test_router().oneshot(req).await.unwrap().status()
        }
    }

    // --- refusals (each flips to 200 if the guard layer is removed) -----------

    #[tokio::test]
    async fn cross_origin_post_is_refused() {
        // The exact Phase-1 attack: a page on localhost:5215 POSTing to the hub.
        let s = Req {
            method: Method::POST,
            ..Req::get("127.0.0.1:5555")
        }
        .origin("http://localhost:5215")
        .status()
        .await;
        assert_eq!(s, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn cross_origin_get_is_refused() {
        let s = Req::get("127.0.0.1:5555")
            .origin("http://localhost:5215")
            .status()
            .await;
        assert_eq!(s, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn null_origin_is_refused() {
        let s = Req::get("127.0.0.1:5555").origin("null").status().await;
        assert_eq!(s, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn rebinding_host_is_refused() {
        // attacker.com -> 127.0.0.1: same-origin, no Origin header, trusted
        // peer. Only the Host check catches it.
        let s = Req::get("127.0.0.1:5555")
            .host(Some("attacker.com"))
            .status()
            .await;
        assert_eq!(s, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn missing_host_on_loopback_passes() {
        // A browser fetch always carries a Host, so an absent Host is not a
        // rebinding vector; it is a non-browser/malformed client and must stay
        // allowed (local clients that omit Host relied on this pre-guard).
        let s = Req::get("127.0.0.1:5555").host(None).status().await;
        assert_eq!(s, StatusCode::OK);
    }

    #[tokio::test]
    async fn origin_less_cross_site_fetch_is_refused() {
        // A cross-site GET with no Origin and a non-media destination.
        let s = Req::get("127.0.0.1:5555")
            .fetch("cross-site", "empty")
            .status()
            .await;
        assert_eq!(s, StatusCode::FORBIDDEN);
    }

    // --- allowances (each pins the opposite: flips to 403 if over-blocked) ----

    #[tokio::test]
    async fn allowed_origin_passes() {
        let s = Req::get("127.0.0.1:5555")
            .origin("http://tauri.localhost")
            .status()
            .await;
        assert_eq!(s, StatusCode::OK);
    }

    #[tokio::test]
    async fn no_origin_curl_passes() {
        // curl / node / MCP: no Origin, no Sec-Fetch, trusted loopback Host.
        let s = Req::get("127.0.0.1:5555").status().await;
        assert_eq!(s, StatusCode::OK);
    }

    #[tokio::test]
    async fn hub_served_same_origin_post_passes() {
        // POST to a fallback (worker) path: 502 "no worker registered" proves
        // the request passed the guard AND auth and reached dispatch. A 403
        // would mean the guard blocked it. (/health is GET-only, so it cannot
        // serve as the POST probe.)
        let s = Req {
            method: Method::POST,
            ..Req::get("127.0.0.1:5555")
        }
        .path("/agent/speak")
        .origin("http://localhost:3200")
        .status()
        .await;
        assert_eq!(s, StatusCode::BAD_GATEWAY);
    }

    #[tokio::test]
    async fn cross_site_media_subresource_passes() {
        // A persona avatar <img>: cross-site, no Origin, dest=image.
        let s = Req::get("127.0.0.1:5555")
            .fetch("cross-site", "image")
            .status()
            .await;
        assert_eq!(s, StatusCode::OK);
    }

    #[tokio::test]
    async fn cross_site_media_dest_post_is_refused() {
        // The media exemption is GET/HEAD only: a POST dressed up with
        // dest=image must not slip a fire-and-forget mutation through.
        let s = Req {
            method: Method::POST,
            ..Req::get("127.0.0.1:5555")
        }
        .fetch("cross-site", "image")
        .status()
        .await;
        assert_eq!(s, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn cross_site_top_level_navigation_passes() {
        // Tapping a hub link from another site: no Origin, dest=document.
        let s = Req::get("127.0.0.1:5555")
            .fetch("cross-site", "document")
            .status()
            .await;
        assert_eq!(s, StatusCode::OK);
    }

    #[tokio::test]
    async fn cross_site_document_dest_post_is_refused() {
        // A form POST navigation is dest=document too, and carries an Origin
        // in every current browser; without one it must still be refused.
        let s = Req {
            method: Method::POST,
            ..Req::get("127.0.0.1:5555")
        }
        .fetch("cross-site", "document")
        .status()
        .await;
        assert_eq!(s, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn nginx_peer_with_secret_and_phone_origin_passes() {
        // Non-loopback peer (the bridge): Host check is skipped, Origin is
        // allow-listed, secret authenticates.
        let s = Req::get("192.168.0.9:5555")
            .peer(Some("192.168.0.9:5555"))
            .host(Some("hub.justinmaier.com"))
            .origin("https://hub.justinmaier.com")
            .secret(TEST_SECRET)
            .status()
            .await;
        assert_eq!(s, StatusCode::OK);
    }
}
