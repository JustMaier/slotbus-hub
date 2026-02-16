"""Flask-like worker API for slotbus shared-memory IPC."""

from __future__ import annotations

import ctypes
import json
import sys
import threading
import traceback
import urllib.error
import urllib.request
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from slotbus import _native as ffi


# ---------------------------------------------------------------------------
# Request
# ---------------------------------------------------------------------------

class Request:
    """Incoming HTTP request parsed from the SHM slot.

    Attributes:
        method:  HTTP method (GET, POST, etc.)
        path:    Request path (e.g. "/items/42")
        query:   Query string without leading '?', or None
        body:    Raw body bytes
        headers: Dict of header name -> value
        params:  Dict of path parameters (e.g. {"id": "42"})
    """

    __slots__ = ("method", "path", "query", "body", "headers", "params")

    def __init__(
        self,
        method: str,
        path: str,
        query: Optional[str],
        body: bytes,
        headers: Dict[str, str],
        params: Dict[str, str],
    ) -> None:
        self.method = method
        self.path = path
        self.query = query
        self.body = body
        self.headers = headers
        self.params = params

    def json(self) -> Any:
        """Parse body as JSON."""
        return json.loads(self.body)

    @property
    def text(self) -> str:
        """Decode body as UTF-8 text."""
        return self.body.decode("utf-8")

    @classmethod
    def _from_ffi(cls, req_ptr: ctypes.c_void_p) -> Request:
        """Build a Request from an FFI request pointer."""
        method_raw = ffi.slotbus_request_method(req_ptr)
        method = method_raw.decode("utf-8") if method_raw else "GET"

        path_raw = ffi.slotbus_request_path(req_ptr)
        path = path_raw.decode("utf-8") if path_raw else "/"

        query_raw = ffi.slotbus_request_query(req_ptr)
        query = query_raw.decode("utf-8") if query_raw else None

        # Body
        body_len = ctypes.c_size_t(0)
        body_ptr = ffi.slotbus_request_body(req_ptr, ctypes.byref(body_len))
        if body_ptr and body_len.value > 0:
            body = bytes(ctypes.cast(body_ptr, ctypes.POINTER(ctypes.c_uint8 * body_len.value)).contents)
        else:
            body = b""

        # Headers
        headers: Dict[str, str] = {}
        hcount = ffi.slotbus_request_header_count(req_ptr)
        for i in range(hcount):
            name_p = ctypes.c_char_p()
            value_p = ctypes.c_char_p()
            if ffi.slotbus_request_header_get(req_ptr, i, ctypes.byref(name_p), ctypes.byref(value_p)) == 0:
                n = name_p.value
                v = value_p.value
                if n is not None and v is not None:
                    headers[n.decode("utf-8")] = v.decode("utf-8")

        # Path params
        params: Dict[str, str] = {}
        pcount = ffi.slotbus_request_path_param_count(req_ptr)
        for i in range(pcount):
            name_p = ctypes.c_char_p()
            value_p = ctypes.c_char_p()
            if ffi.slotbus_request_path_param_get(req_ptr, i, ctypes.byref(name_p), ctypes.byref(value_p)) == 0:
                n = name_p.value
                v = value_p.value
                if n is not None and v is not None:
                    params[n.decode("utf-8")] = v.decode("utf-8")

        return cls(method, path, query, body, headers, params)


# ---------------------------------------------------------------------------
# Response
# ---------------------------------------------------------------------------

class Response:
    """Custom response with explicit status, body, and content type.

    If body is a dict or list, it is JSON-serialized automatically.
    If body is a str, it is UTF-8 encoded.
    If body is bytes, it is used as-is.
    """

    __slots__ = ("status", "body", "content_type")

    def __init__(
        self,
        body: Union[dict, list, str, bytes] = b"",
        status: int = 200,
        content_type: Optional[str] = None,
    ) -> None:
        self.status = status

        if isinstance(body, (dict, list)):
            self.body = json.dumps(body).encode("utf-8")
            self.content_type = content_type or "application/json"
        elif isinstance(body, str):
            self.body = body.encode("utf-8")
            self.content_type = content_type or "text/plain"
        elif isinstance(body, bytes):
            self.body = body
            self.content_type = content_type or "application/octet-stream"
        else:
            self.body = str(body).encode("utf-8")
            self.content_type = content_type or "text/plain"


# ---------------------------------------------------------------------------
# Route matching
# ---------------------------------------------------------------------------

def _match_route(
    pattern: str, path: str
) -> Optional[Dict[str, str]]:
    """Match a route pattern against a concrete path.

    Patterns use {name} for path parameters.
    Returns extracted params dict on match, or None on mismatch.
    """
    pat_parts = pattern.strip("/").split("/")
    path_parts = path.strip("/").split("/")

    if len(pat_parts) != len(path_parts):
        return None

    params: Dict[str, str] = {}
    for pp, rp in zip(pat_parts, path_parts):
        if pp.startswith("{") and pp.endswith("}"):
            params[pp[1:-1]] = rp
        elif pp != rp:
            return None

    return params


# ---------------------------------------------------------------------------
# SlotWorker
# ---------------------------------------------------------------------------

# Type alias for route handler
RouteHandler = Callable[[Request], Any]

# Route key: (METHOD, pattern)
RouteKey = Tuple[str, str]


class SlotWorker:
    """Flask-like worker for slotbus shared-memory IPC.

    Usage::

        from slotbus import SlotWorker

        app = SlotWorker("http://localhost:3200", name="my-api")

        @app.route("GET", "/status")
        def status(req):
            return {"ok": True}

        app.run()
    """

    def __init__(self, hub_url: str, name: str) -> None:
        self.hub_url = hub_url.rstrip("/")
        self.name = name
        self._routes: Dict[RouteKey, RouteHandler] = {}
        self._route_list: List[RouteKey] = []  # preserves insertion order
        self._worker_ptr: Optional[ctypes.c_void_p] = None
        self._callback_ref: Optional[ffi.SlotRequestCallback] = None
        self._stopped = threading.Event()

    def route(self, method: str, path: str) -> Callable[[RouteHandler], RouteHandler]:
        """Decorator to register a route handler.

        Args:
            method: HTTP method (GET, POST, PUT, DELETE, etc.)
            path:   Route pattern, e.g. "/items/{id}"
        """
        method = method.upper()

        def decorator(fn: RouteHandler) -> RouteHandler:
            key: RouteKey = (method, path)
            self._routes[key] = fn
            self._route_list.append(key)
            return fn

        return decorator

    def _resolve_route(
        self, method: str, path: str
    ) -> Optional[Tuple[RouteHandler, Dict[str, str]]]:
        """Find a matching route handler for the given method and path."""
        for route_method, route_pattern in self._route_list:
            if route_method != method:
                continue
            params = _match_route(route_pattern, path)
            if params is not None:
                return self._routes[(route_method, route_pattern)], params
        return None

    def _register_with_hub(self) -> str:
        """POST /internal/register to the hub. Returns the shm_name."""
        routes_payload = []
        for method, path in self._route_list:
            routes_payload.append({"method": method, "path": path})

        body = json.dumps({
            "name": self.name,
            "routes": routes_payload,
        }).encode("utf-8")

        req = urllib.request.Request(
            f"{self.hub_url}/internal/register",
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        try:
            with urllib.request.urlopen(req) as resp:
                data = json.loads(resp.read().decode("utf-8"))
        except urllib.error.URLError as e:
            raise ConnectionError(
                f"Failed to register with hub at {self.hub_url}: {e}"
            ) from e

        shm_name = data.get("shm_name")
        if not shm_name:
            raise RuntimeError(f"Hub response missing 'shm_name': {data}")
        return shm_name

    def _handle_request(
        self, worker_ptr: ctypes.c_void_p, req_ptr: ctypes.c_void_p
    ) -> None:
        """Called from the C callback thread for each incoming request."""
        try:
            request = Request._from_ffi(req_ptr)

            match = self._resolve_route(request.method, request.path)

            if match is None:
                status = 404
                body_bytes = b'{"error":"Not Found"}'
                ct = "application/json"
            else:
                handler, params = match
                # Merge FFI-provided params with our matched params
                # (FFI params come from the hub's route matching; ours are a fallback)
                request.params.update(params)
                result = handler(request)
                status, body_bytes, ct = _coerce_response(result)

        except Exception:
            traceback.print_exc()
            status = 500
            body_bytes = b'{"error":"Internal Server Error"}'
            ct = "application/json"

        # Send response via FFI
        body_array = (ctypes.c_uint8 * len(body_bytes))(*body_bytes) if body_bytes else None
        body_ptr = ctypes.cast(body_array, ctypes.POINTER(ctypes.c_uint8)) if body_array else None
        body_len = len(body_bytes)

        rc = ffi.slotbus_respond(
            worker_ptr,
            req_ptr,
            status,
            body_ptr,
            body_len,
            ct.encode("utf-8"),
        )
        if rc != 0:
            err = ffi.slotbus_last_error()
            err_msg = err.decode("utf-8") if err else "unknown error"
            print(f"[slotbus] respond error: {err_msg}", file=sys.stderr)

    def run(self) -> None:
        """Register with the hub, open SHM, start the receive loop, and block.

        Blocks until KeyboardInterrupt (Ctrl+C) or stop() is called.
        """
        # 1. Register routes with hub
        shm_name = self._register_with_hub()
        print(f"[slotbus] Registered '{self.name}', SHM: {shm_name}")

        # 2. Open the SHM worker
        self._worker_ptr = ffi.slotbus_worker_open(
            self.name.encode("utf-8"),
            shm_name.encode("utf-8"),
        )
        if not self._worker_ptr:
            err = ffi.slotbus_last_error()
            err_msg = err.decode("utf-8") if err else "unknown error"
            raise RuntimeError(f"Failed to open SHM worker: {err_msg}")

        print(f"[slotbus] SHM opened, listening for requests...")

        # 3. Create and pin the C callback (prevent GC)
        def _c_callback(
            w_ptr: ctypes.c_void_p,
            r_ptr: ctypes.c_void_p,
            _user_data: ctypes.c_void_p,
        ) -> None:
            self._handle_request(w_ptr, r_ptr)

        self._callback_ref = ffi.SlotRequestCallback(_c_callback)

        # 4. Start the receive loop (runs on a dedicated C thread)
        rc = ffi.slotbus_worker_start(self._worker_ptr, self._callback_ref, None)
        if rc != 0:
            err = ffi.slotbus_last_error()
            err_msg = err.decode("utf-8") if err else "unknown error"
            ffi.slotbus_worker_free(self._worker_ptr)
            self._worker_ptr = None
            raise RuntimeError(f"Failed to start worker: {err_msg}")

        # 5. Block until interrupted
        try:
            self._stopped.wait()
        except KeyboardInterrupt:
            print(f"\n[slotbus] Shutting down '{self.name}'...")
        finally:
            self._cleanup()

    def emit(self, event_type: str, data: Union[str, dict, list]) -> None:
        """Emit an event to the hub via HTTP POST.

        Args:
            event_type: Event type string
            data:       JSON-serializable event data (or a raw JSON string)
        """
        if isinstance(data, (dict, list)):
            data_str = json.dumps(data)
        else:
            data_str = str(data)

        rc = ffi.slotbus_emit_event(
            self.hub_url.encode("utf-8"),
            self.name.encode("utf-8"),
            event_type.encode("utf-8"),
            data_str.encode("utf-8"),
        )
        if rc != 0:
            err = ffi.slotbus_last_error()
            err_msg = err.decode("utf-8") if err else "unknown error"
            raise RuntimeError(f"Failed to emit event: {err_msg}")

    def stop(self) -> None:
        """Signal the worker to stop. Can be called from any thread."""
        self._stopped.set()

    def _cleanup(self) -> None:
        """Stop and free the native worker."""
        if self._worker_ptr:
            ffi.slotbus_worker_stop(self._worker_ptr)
            ffi.slotbus_worker_free(self._worker_ptr)
            self._worker_ptr = None
            self._callback_ref = None
            print(f"[slotbus] Worker '{self.name}' stopped.")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _coerce_response(result: Any) -> Tuple[int, bytes, str]:
    """Convert a handler return value to (status, body_bytes, content_type)."""
    if isinstance(result, Response):
        return result.status, result.body, result.content_type

    if isinstance(result, dict) or isinstance(result, list):
        return 200, json.dumps(result).encode("utf-8"), "application/json"

    if isinstance(result, str):
        return 200, result.encode("utf-8"), "text/plain"

    if isinstance(result, bytes):
        return 200, result, "application/octet-stream"

    if result is None:
        return 204, b"", "text/plain"

    # Fallback: coerce to string
    return 200, str(result).encode("utf-8"), "text/plain"
