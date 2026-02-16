/**
 * slotbus.h — C API for slotbus shared-memory IPC workers.
 *
 * Two consumption modes:
 *   - Callback API (Python/ctypes): slotbus_worker_start() invokes a callback
 *     on a dedicated thread for each incoming request.
 *   - Polling API (Node.js/koffi): slotbus_poll() returns requests non-blocking;
 *     respond with slotbus_poll_respond().
 *
 * Both modes support deferred responses via slotbus_defer().
 */

#ifndef SLOTBUS_H
#define SLOTBUS_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ── Error handling ──────────────────────────────────── */

/** Last error message for the calling thread, or "". */
const char* slotbus_last_error(void);

/* ── Opaque handles ──────────────────────────────────── */

typedef struct FfiWorker   SlotWorker;
typedef struct FfiRequest  SlotRequest;
typedef struct FfiSlotHandle SlotHandle;

/* ── Lifecycle ───────────────────────────────────────── */

/**
 * Open a worker connection to an existing SHM region.
 *
 * @param worker_name  Worker name (e.g. "my-python-worker")
 * @param shm_name     SHM region name from hub registration (e.g. "slotbus-my-python-worker")
 * @return Worker handle, or NULL on failure (check slotbus_last_error())
 */
SlotWorker* slotbus_worker_open(
    const char* worker_name,
    const char* shm_name
);

/** Signal the worker to stop processing. Safe to call multiple times. */
void slotbus_worker_stop(SlotWorker* worker);

/** Free the worker handle. Stops and joins the receive thread first. */
void slotbus_worker_free(SlotWorker* worker);

/* ── Callback API (Python) ───────────────────────────── */

/**
 * Callback invoked for each incoming request.
 *
 * Must call slotbus_respond() or slotbus_defer() before returning.
 * If neither is called, a 500 response is sent automatically.
 */
typedef void (*SlotRequestCallback)(
    SlotWorker* worker,
    const SlotRequest* request,
    void* user_data
);

/**
 * Start the callback receive loop on a dedicated OS thread.
 *
 * Only one callback loop can be active per worker.
 * @return 0 on success, -1 on failure.
 */
int32_t slotbus_worker_start(
    SlotWorker* worker,
    SlotRequestCallback cb,
    void* user_data
);

/* ── Polling API (Node.js) ───────────────────────────── */

/**
 * Non-blocking poll for the next request.
 *
 * @return Request handle, or NULL if none available.
 *         Must be freed via slotbus_poll_respond() or slotbus_poll_drop().
 */
SlotRequest* slotbus_poll(SlotWorker* worker);

/**
 * Respond to a polled request and free it.
 * @return 0 on success, -1 on failure.
 */
int32_t slotbus_poll_respond(
    SlotWorker* worker,
    SlotRequest* request,
    uint16_t status,
    const uint8_t* body,
    size_t body_len,
    const char* content_type
);

/** Drop a polled request (sends 500) and free it. */
void slotbus_poll_drop(SlotWorker* worker, SlotRequest* request);

/**
 * Get the OS event handle for external event loop integration.
 *
 * Windows: returns a HANDLE (auto-reset event) suitable for
 * WaitForSingleObject or libuv's uv_poll_init.
 *
 * @return Handle value, or -1 on error / unsupported platform.
 */
intptr_t slotbus_worker_wait_handle(SlotWorker* worker);

/* ── Request accessors ───────────────────────────────── */
/* Pointers are valid until the request is freed.        */

const char*    slotbus_request_id(const SlotRequest* req);
const char*    slotbus_request_method(const SlotRequest* req);
const char*    slotbus_request_path(const SlotRequest* req);
const char*    slotbus_request_route_pattern(const SlotRequest* req);
const char*    slotbus_request_query(const SlotRequest* req);     /* NULL if none */

/**
 * Get the request body. Writes length to out_len.
 * @return Pointer to body bytes, or NULL if empty.
 */
const uint8_t* slotbus_request_body(const SlotRequest* req, size_t* out_len);

uint32_t slotbus_request_header_count(const SlotRequest* req);

/**
 * Get header by index. Writes name/value pointers.
 * @return 0 on success, -1 if index out of bounds.
 */
int32_t slotbus_request_header_get(
    const SlotRequest* req,
    uint32_t i,
    const char** name,
    const char** value
);

/** Look up header by name (case-sensitive). Returns NULL if not found. */
const char* slotbus_request_header(const SlotRequest* req, const char* name);

uint32_t slotbus_request_path_param_count(const SlotRequest* req);

/**
 * Get path parameter by index. Writes name/value pointers.
 * @return 0 on success, -1 if index out of bounds.
 */
int32_t slotbus_request_path_param_get(
    const SlotRequest* req,
    uint32_t i,
    const char** name,
    const char** value
);

/** Look up path parameter by name. Returns NULL if not found. */
const char* slotbus_request_path_param(const SlotRequest* req, const char* name);

/* ── Responding ──────────────────────────────────────── */

/**
 * Send a response for a request (does not free the request).
 * @return 0 on success, -1 on failure.
 */
int32_t slotbus_respond(
    SlotWorker* worker,
    const SlotRequest* request,
    uint16_t status,
    const uint8_t* body,
    size_t body_len,
    const char* content_type
);

/**
 * Send a response with custom headers.
 *
 * header_names and header_values are parallel arrays of header_count strings.
 * @return 0 on success, -1 on failure.
 */
int32_t slotbus_respond_headers(
    SlotWorker* worker,
    const SlotRequest* request,
    uint16_t status,
    const uint8_t* body,
    size_t body_len,
    const char* content_type,
    const char* const* header_names,
    const char* const* header_values,
    uint32_t header_count
);

/* ── Deferred response ───────────────────────────────── */

/**
 * Create a deferred response handle from a request.
 *
 * The request is marked as deferred (no auto-500). The caller must
 * eventually call slotbus_deferred_respond() or slotbus_deferred_free().
 *
 * @return Handle, or NULL on failure.
 */
SlotHandle* slotbus_defer(SlotWorker* worker, const SlotRequest* request);

/**
 * Send a response via deferred handle, then free the handle.
 * @return 0 on success, -1 on failure.
 */
int32_t slotbus_deferred_respond(
    SlotHandle* handle,
    uint16_t status,
    const uint8_t* body,
    size_t body_len,
    const char* content_type
);

/** Free a deferred handle, sending 500 if not yet responded. */
void slotbus_deferred_free(SlotHandle* handle);

/* ── Event emission ──────────────────────────────────── */

/**
 * Emit an event to the hub via blocking HTTP POST.
 *
 * @param hub_url     Hub base URL (e.g. "http://localhost:3200")
 * @param source      Worker name (event source)
 * @param event_type  Event type string
 * @param data        JSON event data
 * @return 0 on success, -1 on failure.
 */
int32_t slotbus_emit_event(
    const char* hub_url,
    const char* source,
    const char* event_type,
    const char* data
);

/* ── Misc ────────────────────────────────────────────── */

/** Return the library version string (e.g. "0.1.0"). */
const char* slotbus_version(void);

#ifdef __cplusplus
}
#endif

#endif /* SLOTBUS_H */
