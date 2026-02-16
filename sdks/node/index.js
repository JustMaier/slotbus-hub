'use strict';

const native = require('./lib/native');

// ---- Response class -----------------------------------------------------------

/**
 * Custom response object for controlling status, headers, and content type.
 *
 * Handlers can return a Response instance for full control, or return a plain
 * value (object/string/Buffer) for automatic serialization.
 */
class Response {
    /**
     * @param {string|Buffer} body
     * @param {number} [status=200]
     * @param {string} [contentType]
     * @param {Object<string,string>} [headers={}]
     */
    constructor(body, status = 200, contentType, headers = {}) {
        if (Buffer.isBuffer(body)) {
            this.body = body;
            this.contentType = contentType || 'application/octet-stream';
        } else if (typeof body === 'string') {
            this.body = Buffer.from(body, 'utf-8');
            this.contentType = contentType || 'text/plain';
        } else {
            this.body = Buffer.from(JSON.stringify(body), 'utf-8');
            this.contentType = contentType || 'application/json';
        }
        this.status = status;
        this.headers = headers;
    }

    /** Create a JSON response. */
    static json(data, status = 200) {
        return new Response(data, status, 'application/json');
    }

    /** Create a plain text response. */
    static text(str, status = 200) {
        return new Response(str, status, 'text/plain');
    }
}

// ---- Request class ------------------------------------------------------------

/**
 * Wraps a native SlotRequest pointer with a convenient JS interface.
 *
 * Properties are read lazily from the native request on first access.
 */
class Request {
    /** @param {*} reqPtr - Native SlotRequest pointer */
    constructor(reqPtr) {
        this._ptr = reqPtr;
        this._body = undefined;
        this._headers = undefined;
        this._params = undefined;
        this._query = undefined;
        this._materialized = false;
    }

    /**
     * Eagerly read all properties from native memory.
     * Must be called before the native request pointer is freed (e.g., for async handlers).
     * After this call, all getters return cached values and the native pointer is no longer needed.
     */
    _materialize() {
        if (this._materialized) return;
        // Access each getter to force lazy initialization
        void this.method;
        void this.path;
        void this.routePattern;
        void this.query;
        void this.body;
        void this.headers;
        void this.params;
        this._materialized = true;
        this._ptr = null; // Prevent accidental use-after-free
    }

    /** HTTP method (GET, POST, etc.) */
    get method() {
        if (!this._method) this._method = native.slotbus_request_method(this._ptr);
        return this._method;
    }

    /** Request path (e.g. /items/42) */
    get path() {
        if (!this._path) this._path = native.slotbus_request_path(this._ptr);
        return this._path;
    }

    /** Route pattern that matched (e.g. /items/{id}) */
    get routePattern() {
        if (!this._routePattern) {
            this._routePattern = native.slotbus_request_route_pattern(this._ptr);
        }
        return this._routePattern;
    }

    /** Query string (without leading ?) or null */
    get query() {
        if (this._query === undefined) {
            this._query = native.slotbus_request_query(this._ptr);
        }
        return this._query;
    }

    /** Request body as Buffer */
    get body() {
        if (this._body === undefined) {
            const lenOut = [0];
            const bodyPtr = native.slotbus_request_body(this._ptr, lenOut);
            const len = lenOut[0];
            if (!bodyPtr || len === 0) {
                this._body = Buffer.alloc(0);
            } else {
                // Create a zero-copy ArrayBuffer view into native memory,
                // then copy into a Node Buffer so it survives request free
                const ab = native.koffi.view(bodyPtr, len);
                this._body = Buffer.from(Buffer.from(ab));
            }
        }
        return this._body;
    }

    /** Request body as UTF-8 string */
    get text() {
        return this.body.toString('utf-8');
    }

    /** Parse request body as JSON */
    json() {
        return JSON.parse(this.text);
    }

    /** Request headers as a plain object */
    get headers() {
        if (!this._headers) {
            this._headers = {};
            const count = native.slotbus_request_header_count(this._ptr);
            const nameOut = [null];
            const valueOut = [null];
            for (let i = 0; i < count; i++) {
                if (native.slotbus_request_header_get(this._ptr, i, nameOut, valueOut) === 0) {
                    this._headers[nameOut[0]] = valueOut[0];
                }
            }
        }
        return this._headers;
    }

    /** Path parameters as a plain object (e.g. { id: '42' }) */
    get params() {
        if (!this._params) {
            this._params = {};
            const count = native.slotbus_request_path_param_count(this._ptr);
            const nameOut = [null];
            const valueOut = [null];
            for (let i = 0; i < count; i++) {
                if (native.slotbus_request_path_param_get(this._ptr, i, nameOut, valueOut) === 0) {
                    this._params[nameOut[0]] = valueOut[0];
                }
            }
        }
        return this._params;
    }

    /** Get a specific header by name (case-sensitive) */
    header(name) {
        return native.slotbus_request_header(this._ptr, name);
    }

    /** Get a specific path parameter by name */
    param(name) {
        return native.slotbus_request_path_param(this._ptr, name);
    }
}

// ---- Route matching -----------------------------------------------------------

/**
 * Convert Express-style `:param` path to hub `{param}` path.
 * e.g. '/items/:id/sub/:name' -> '/items/{id}/sub/{name}'
 */
function expressToHub(path) {
    return path.replace(/:([a-zA-Z_][a-zA-Z0-9_]*)/g, '{$1}');
}

/**
 * Match a hub-style route pattern against an actual path.
 * Returns extracted params object, or null if no match.
 */
function matchRoute(pattern, actualPath) {
    const patternParts = pattern.split('/');
    const pathParts = actualPath.split('/');
    if (patternParts.length !== pathParts.length) return null;
    const params = {};
    for (let i = 0; i < patternParts.length; i++) {
        const pat = patternParts[i];
        if (pat.startsWith('{') && pat.endsWith('}')) {
            params[pat.slice(1, -1)] = pathParts[i];
        } else if (pat !== pathParts[i]) {
            return null;
        }
    }
    return params;
}

// ---- SlotWorker class ---------------------------------------------------------

/**
 * Express-like worker that registers routes with a slotbus hub and handles
 * incoming requests via shared memory.
 *
 * Uses the **polling API** — koffi cannot safely marshal C callbacks from
 * foreign threads into Node's event loop, so we poll using setImmediate +
 * the native OS wait handle when available.
 *
 * @example
 * const { SlotWorker } = require('slotbus');
 *
 * const app = new SlotWorker('http://localhost:3200', 'my-api');
 *
 * app.get('/status', (req) => {
 *     return { ok: true };
 * });
 *
 * app.listen();
 */
class SlotWorker {
    /**
     * @param {string} hubUrl - Hub base URL (e.g. 'http://localhost:3200')
     * @param {string} name - Worker name
     */
    constructor(hubUrl, name) {
        this._hubUrl = hubUrl.replace(/\/+$/, '');
        this._name = name;
        this._routes = [];       // { method, hubPath, handler }
        this._workerPtr = null;
        this._running = false;
        this._pollTimer = null;
        this._workerId = null;
        this._shmName = null;
    }

    /** Register a GET route handler. */
    get(path, handler) { return this.route('GET', path, handler); }
    /** Register a POST route handler. */
    post(path, handler) { return this.route('POST', path, handler); }
    /** Register a PUT route handler. */
    put(path, handler) { return this.route('PUT', path, handler); }
    /** Register a DELETE route handler. */
    delete(path, handler) { return this.route('DELETE', path, handler); }
    /** Register a PATCH route handler. */
    patch(path, handler) { return this.route('PATCH', path, handler); }

    /**
     * Register a route handler.
     * @param {string} method - HTTP method
     * @param {string} path - Express-style path (e.g. '/items/:id')
     * @param {function} handler - Request handler: (req) => value | Promise<value>
     * @returns {SlotWorker} this (for chaining)
     */
    route(method, path, handler) {
        const hubPath = expressToHub(path);
        this._routes.push({ method: method.toUpperCase(), hubPath, handler });
        return this;
    }

    /**
     * Register routes with the hub, open the SHM region, and start polling.
     * @returns {Promise<void>}
     */
    async listen() {
        // 1. Register with the hub
        const regBody = JSON.stringify({
            name: this._name,
            routes: this._routes.map(r => ({ method: r.method, path: r.hubPath })),
        });

        const resp = await fetch(`${this._hubUrl}/internal/register`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: regBody,
        });

        if (!resp.ok) {
            const text = await resp.text();
            throw new Error(`Hub registration failed (${resp.status}): ${text}`);
        }

        const { worker_id, shm_name } = await resp.json();
        this._workerId = worker_id;
        this._shmName = shm_name;

        console.log(`[slotbus] Registered '${this._name}' (${worker_id}), SHM: ${shm_name}`);
        console.log(`[slotbus] Routes:`);
        for (const r of this._routes) {
            console.log(`  ${r.method.padEnd(7)} ${r.hubPath}`);
        }

        // 2. Open the SHM region via FFI
        this._workerPtr = native.slotbus_worker_open(this._name, shm_name);
        if (!this._workerPtr) {
            const err = native.slotbus_last_error();
            throw new Error(`Failed to open SHM region: ${err}`);
        }

        // 3. Start the poll loop
        this._running = true;
        this._startPollLoop();

        // 4. Graceful shutdown on SIGINT
        const shutdown = () => {
            if (this._running) {
                console.log('\n[slotbus] Shutting down...');
                this.close();
            }
        };
        process.on('SIGINT', shutdown);
        process.on('SIGTERM', shutdown);

        console.log(`[slotbus] Listening for requests via SHM...`);
    }

    /**
     * Emit an event to the hub.
     * @param {string} eventType - Event type string
     * @param {*} data - JSON-serializable event data
     */
    emit(eventType, data) {
        const dataStr = typeof data === 'string' ? data : JSON.stringify(data);
        const result = native.slotbus_emit_event(
            this._hubUrl, this._name, eventType, dataStr
        );
        if (result !== 0) {
            const err = native.slotbus_last_error();
            throw new Error(`Failed to emit event: ${err}`);
        }
    }

    /**
     * Stop the receive loop. The worker can still respond to deferred requests.
     */
    stop() {
        this._running = false;
        if (this._pollTimer) {
            clearTimeout(this._pollTimer);
            this._pollTimer = null;
        }
        if (this._workerPtr) {
            native.slotbus_worker_stop(this._workerPtr);
        }
    }

    /**
     * Stop and free the worker. After this call the instance is unusable.
     */
    close() {
        this.stop();
        if (this._workerPtr) {
            native.slotbus_worker_free(this._workerPtr);
            this._workerPtr = null;
        }
    }

    /** Library version string. */
    static version() {
        return native.slotbus_version();
    }

    // ---- Internal: poll loop --------------------------------------------------

    /** Start polling for incoming requests. */
    _startPollLoop() {
        const pollOnce = () => {
            if (!this._running) return;

            let processed = 0;
            // Drain all available requests in one tick
            // eslint-disable-next-line no-constant-condition
            while (true) {
                const reqPtr = native.slotbus_poll(this._workerPtr);
                if (!reqPtr) break;
                processed++;
                this._handleRequest(reqPtr);
            }

            // Schedule next poll:
            // - If we just processed work, check again immediately (more may have arrived)
            // - Otherwise, back off to 1ms to avoid busy-spinning
            if (this._running) {
                if (processed > 0) {
                    this._pollTimer = setImmediate(pollOnce);
                } else {
                    this._pollTimer = setTimeout(pollOnce, 1);
                }
            }
        };

        // Kick off the first poll
        setImmediate(pollOnce);
    }

    /**
     * Dispatch a native request to the matching route handler.
     * @param {*} reqPtr - Native SlotRequest pointer
     */
    _handleRequest(reqPtr) {
        const req = new Request(reqPtr);
        const method = req.method;
        const routePattern = req.routePattern;

        // Find matching handler by method + route pattern
        const route = this._routes.find(
            r => r.method === method && r.hubPath === routePattern
        );

        if (!route) {
            // No handler — send 404 and free the request
            const body = Buffer.from('Not Found');
            native.slotbus_poll_respond(
                this._workerPtr, reqPtr, 404, body, body.length, 'text/plain'
            );
            return;
        }

        let result;
        try {
            result = route.handler(req);
        } catch (err) {
            // Sync handler threw — send 500
            console.error(`[slotbus] Handler error for ${method} ${routePattern}:`, err);
            const body = Buffer.from(String(err.message || err));
            native.slotbus_poll_respond(
                this._workerPtr, reqPtr, 500, body, body.length, 'text/plain'
            );
            return;
        }

        // Check if the handler returned a Promise (async handler)
        if (result && typeof result.then === 'function') {
            // Defer the response — the slot stays open until we respond
            const handle = native.slotbus_defer(this._workerPtr, reqPtr);
            if (!handle) {
                // Defer failed — the request is already auto-500'd by the FFI
                console.error(`[slotbus] Failed to defer request for ${method} ${routePattern}`);
                native.slotbus_poll_drop(this._workerPtr, reqPtr);
                return;
            }

            // Eagerly read all request properties into JS before freeing native memory.
            // The async handler's Promise may still reference req.params, req.body, etc.
            req._materialize();

            // Free the native FfiRequest. Since we called slotbus_defer(), the request
            // is marked as deferred, so poll_drop won't send a 500 — it just frees memory.
            native.slotbus_poll_drop(this._workerPtr, reqPtr);

            result.then(
                (value) => {
                    try {
                        const { body, status, contentType } = serializeReturnValue(value);
                        native.slotbus_deferred_respond(
                            handle, status, body, body.length, contentType
                        );
                    } catch (err) {
                        console.error(`[slotbus] Deferred respond error:`, err);
                        native.slotbus_deferred_free(handle);
                    }
                },
                (err) => {
                    console.error(`[slotbus] Async handler error for ${method} ${routePattern}:`, err);
                    const body = Buffer.from(String(err.message || err));
                    native.slotbus_deferred_respond(
                        handle, 500, body, body.length, 'text/plain'
                    );
                }
            );
        } else {
            // Sync response — respond immediately via poll_respond (responds + frees)
            try {
                const { body, status, contentType } = serializeReturnValue(result);
                native.slotbus_poll_respond(
                    this._workerPtr, reqPtr, status, body, body.length, contentType
                );
            } catch (err) {
                console.error(`[slotbus] Serialize error:`, err);
                const errBody = Buffer.from(String(err.message || err));
                native.slotbus_poll_respond(
                    this._workerPtr, reqPtr, 500, errBody, errBody.length, 'text/plain'
                );
            }
        }
    }
}

// ---- Helpers ------------------------------------------------------------------

/**
 * Convert a handler's return value into { body: Buffer, status: number, contentType: string }.
 */
function serializeReturnValue(value) {
    if (value instanceof Response) {
        return {
            body: value.body,
            status: value.status,
            contentType: value.contentType,
        };
    }
    if (value === null || value === undefined) {
        return { body: Buffer.alloc(0), status: 204, contentType: 'text/plain' };
    }
    if (Buffer.isBuffer(value)) {
        return { body: value, status: 200, contentType: 'application/octet-stream' };
    }
    if (typeof value === 'string') {
        return { body: Buffer.from(value, 'utf-8'), status: 200, contentType: 'text/plain' };
    }
    // Object or Array — serialize as JSON
    const json = JSON.stringify(value);
    return { body: Buffer.from(json, 'utf-8'), status: 200, contentType: 'application/json' };
}

// ---- Exports ------------------------------------------------------------------

module.exports = { SlotWorker, Request, Response };
