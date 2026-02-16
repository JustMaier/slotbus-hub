'use strict';

const koffi = require('koffi');
const path = require('path');
const fs = require('fs');

// ---- Locate the shared library ------------------------------------------------

function findLibrary() {
    // 1. Explicit env var override
    const envPath = process.env.SLOTBUS_LIB;
    if (envPath) {
        if (!fs.existsSync(envPath)) {
            throw new Error(`SLOTBUS_LIB points to non-existent file: ${envPath}`);
        }
        return envPath;
    }

    // 2. Search relative to this file: ../../../target/release/
    const ext = process.platform === 'win32' ? '.dll'
              : process.platform === 'darwin' ? '.dylib'
              : '.so';
    const prefix = process.platform === 'win32' ? '' : 'lib';
    const libName = `${prefix}slotbus_ffi${ext}`;

    const candidates = [
        // Relative to sdks/node/lib/ -> target/release/
        path.join(__dirname, '..', '..', '..', 'target', 'release', libName),
        // Relative to sdks/node/lib/ -> target/debug/
        path.join(__dirname, '..', '..', '..', 'target', 'debug', libName),
        // Same directory as the script
        path.join(__dirname, libName),
        // Current working directory
        path.join(process.cwd(), libName),
    ];

    for (const candidate of candidates) {
        if (fs.existsSync(candidate)) {
            return candidate;
        }
    }

    throw new Error(
        `Could not find ${libName}. Set SLOTBUS_LIB env var or build with: ` +
        `cargo build --release -p slotbus-ffi\n` +
        `Searched:\n  ${candidates.join('\n  ')}`
    );
}

const libPath = findLibrary();
const lib = koffi.load(libPath);

// ---- Opaque pointer types -----------------------------------------------------

const SlotWorker = koffi.opaque('SlotWorker');
const SlotRequest = koffi.opaque('SlotRequest');
const SlotHandle = koffi.opaque('SlotHandle');

// Pointer types for convenience
const SlotWorkerPtr = koffi.pointer(SlotWorker);
const SlotRequestPtr = koffi.pointer(SlotRequest);
const SlotHandlePtr = koffi.pointer(SlotHandle);

// ---- Callback type (for the callback API — Python-style) ----------------------

const SlotRequestCallback = koffi.proto(
    'void SlotRequestCallback(SlotWorker *worker, SlotRequest *request, void *user_data)'
);

// ---- Function declarations ----------------------------------------------------

// Error handling
const slotbus_last_error = lib.func('const char *slotbus_last_error()');

// Version
const slotbus_version = lib.func('const char *slotbus_version()');

// Lifecycle
const slotbus_worker_open = lib.func(
    'SlotWorker *slotbus_worker_open(const char *worker_name, const char *shm_name)'
);
const slotbus_worker_stop = lib.func('void slotbus_worker_stop(SlotWorker *worker)');
const slotbus_worker_free = lib.func('void slotbus_worker_free(SlotWorker *worker)');

// Callback API
const slotbus_worker_start = lib.func(
    'int32_t slotbus_worker_start(SlotWorker *worker, SlotRequestCallback *cb, void *user_data)'
);

// Polling API
const slotbus_poll = lib.func('SlotRequest *slotbus_poll(SlotWorker *worker)');
const slotbus_poll_respond = lib.func(
    'int32_t slotbus_poll_respond(SlotWorker *worker, SlotRequest *request, ' +
    'uint16_t status, const uint8_t *body, size_t body_len, const char *content_type)'
);
const slotbus_poll_drop = lib.func(
    'void slotbus_poll_drop(SlotWorker *worker, SlotRequest *request)'
);
const slotbus_worker_wait_handle = lib.func(
    'intptr_t slotbus_worker_wait_handle(SlotWorker *worker)'
);

// Request accessors
const slotbus_request_id = lib.func('const char *slotbus_request_id(const SlotRequest *req)');
const slotbus_request_method = lib.func('const char *slotbus_request_method(const SlotRequest *req)');
const slotbus_request_path = lib.func('const char *slotbus_request_path(const SlotRequest *req)');
const slotbus_request_route_pattern = lib.func(
    'const char *slotbus_request_route_pattern(const SlotRequest *req)'
);
const slotbus_request_query = lib.func('const char *slotbus_request_query(const SlotRequest *req)');
const slotbus_request_body = lib.func(
    'const uint8_t *slotbus_request_body(const SlotRequest *req, _Out_ size_t *out_len)'
);
const slotbus_request_header_count = lib.func(
    'uint32_t slotbus_request_header_count(const SlotRequest *req)'
);
const slotbus_request_header_get = lib.func(
    'int32_t slotbus_request_header_get(const SlotRequest *req, uint32_t i, ' +
    '_Out_ const char **name, _Out_ const char **value)'
);
const slotbus_request_header = lib.func(
    'const char *slotbus_request_header(const SlotRequest *req, const char *name)'
);
const slotbus_request_path_param_count = lib.func(
    'uint32_t slotbus_request_path_param_count(const SlotRequest *req)'
);
const slotbus_request_path_param_get = lib.func(
    'int32_t slotbus_request_path_param_get(const SlotRequest *req, uint32_t i, ' +
    '_Out_ const char **name, _Out_ const char **value)'
);
const slotbus_request_path_param = lib.func(
    'const char *slotbus_request_path_param(const SlotRequest *req, const char *name)'
);

// Responding
const slotbus_respond = lib.func(
    'int32_t slotbus_respond(SlotWorker *worker, const SlotRequest *request, ' +
    'uint16_t status, const uint8_t *body, size_t body_len, const char *content_type)'
);
const slotbus_respond_headers = lib.func(
    'int32_t slotbus_respond_headers(SlotWorker *worker, const SlotRequest *request, ' +
    'uint16_t status, const uint8_t *body, size_t body_len, const char *content_type, ' +
    'const char * const *header_names, const char * const *header_values, uint32_t header_count)'
);

// Deferred response
const slotbus_defer = lib.func(
    'SlotHandle *slotbus_defer(SlotWorker *worker, const SlotRequest *request)'
);
const slotbus_deferred_respond = lib.func(
    'int32_t slotbus_deferred_respond(SlotHandle *handle, ' +
    'uint16_t status, const uint8_t *body, size_t body_len, const char *content_type)'
);
const slotbus_deferred_free = lib.func('void slotbus_deferred_free(SlotHandle *handle)');

// Event emission
const slotbus_emit_event = lib.func(
    'int32_t slotbus_emit_event(const char *hub_url, const char *source, ' +
    'const char *event_type, const char *data)'
);

// ---- Exports ------------------------------------------------------------------

module.exports = {
    lib,
    koffi,

    // Types
    SlotWorker,
    SlotRequest,
    SlotHandle,
    SlotWorkerPtr,
    SlotRequestPtr,
    SlotHandlePtr,
    SlotRequestCallback,

    // Functions
    slotbus_last_error,
    slotbus_version,
    slotbus_worker_open,
    slotbus_worker_stop,
    slotbus_worker_free,
    slotbus_worker_start,
    slotbus_poll,
    slotbus_poll_respond,
    slotbus_poll_drop,
    slotbus_worker_wait_handle,
    slotbus_request_id,
    slotbus_request_method,
    slotbus_request_path,
    slotbus_request_route_pattern,
    slotbus_request_query,
    slotbus_request_body,
    slotbus_request_header_count,
    slotbus_request_header_get,
    slotbus_request_header,
    slotbus_request_path_param_count,
    slotbus_request_path_param_get,
    slotbus_request_path_param,
    slotbus_respond,
    slotbus_respond_headers,
    slotbus_defer,
    slotbus_deferred_respond,
    slotbus_deferred_free,
    slotbus_emit_event,
};
