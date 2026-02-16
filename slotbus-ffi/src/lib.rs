// FFI functions are inherently unsafe; safety contracts are documented in slotbus.h.
#![allow(clippy::missing_safety_doc, clippy::too_many_arguments)]

//! C FFI bindings for the slotbus shared-memory IPC library.
//!
//! Exposes `SlotWorker` functionality through opaque pointers and C-compatible types.
//! Two consumption modes:
//!
//! - **Callback API** (Python/ctypes): `slotbus_worker_start()` spawns a thread that
//!   invokes a C function pointer for each incoming request.
//! - **Polling API** (Node.js/koffi): `slotbus_poll()` returns requests non-blocking;
//!   caller responds with `slotbus_poll_respond()`.
//!
//! Both modes support deferred responses via `slotbus_defer()`.

use std::cell::RefCell;
use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use slotbus::config::SlotBusConfig;
use slotbus::events::NamedEvent;
use slotbus::region::{self, ShmRegion};
use slotbus::types::*;

// ---- Version ----------------------------------------------------------------

const VERSION: &str = concat!(env!("CARGO_PKG_VERSION"), "\0");

/// Return the library version string.
#[no_mangle]
pub extern "C" fn slotbus_version() -> *const c_char {
    VERSION.as_ptr() as *const c_char
}

// ---- Thread-local error reporting -------------------------------------------

thread_local! {
    static LAST_ERROR: RefCell<CString> = RefCell::new(CString::default());
}

fn set_last_error(msg: impl Into<String>) {
    let msg = msg.into();
    LAST_ERROR.with(|e| {
        *e.borrow_mut() = CString::new(msg).unwrap_or_default();
    });
}

/// Return the last error message for the calling thread, or an empty string.
#[no_mangle]
pub extern "C" fn slotbus_last_error() -> *const c_char {
    LAST_ERROR.with(|e| e.borrow().as_ptr())
}

// ---- Opaque types -----------------------------------------------------------

/// Worker handle wrapping the SHM region + events.
pub struct FfiWorker {
    config: SlotBusConfig,
    region: Arc<ShmRegion>,
    req_event: NamedEvent,
    rsp_event: Arc<NamedEvent>,
    overflow_regions: Arc<Mutex<HashMap<usize, ShmRegion>>>,
    running: Arc<AtomicBool>,
    recv_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

/// Pre-decoded request with CStrings for zero-copy C access.
pub struct FfiRequest {
    slot_index: usize,
    req_id: CString,
    method: CString,
    path: CString,
    route_pattern: CString,
    query: Option<CString>,
    body: Vec<u8>,
    headers: Vec<(CString, CString)>,
    path_params: Vec<(CString, CString)>,
    responded: AtomicBool,
    deferred: AtomicBool,
}

/// Deferred response handle — owns the slot until responded.
pub struct FfiSlotHandle {
    region: Arc<ShmRegion>,
    rsp_event: Arc<NamedEvent>,
    overflow_regions: Arc<Mutex<HashMap<usize, ShmRegion>>>,
    config: SlotBusConfig,
    slot_index: usize,
    responded: AtomicBool,
}

// ---- Lifecycle --------------------------------------------------------------

/// Open a worker connection to an existing SHM region.
///
/// `worker_name` and `shm_name` must be valid UTF-8 C strings.
/// `shm_name` is the name returned by hub registration (e.g. `"slotbus-my-worker"`).
/// The prefix is extracted from `shm_name` by stripping the `-{worker_name}` suffix.
///
/// Returns NULL on failure (check `slotbus_last_error()`).
#[no_mangle]
pub unsafe extern "C" fn slotbus_worker_open(
    worker_name: *const c_char,
    shm_name: *const c_char,
) -> *mut FfiWorker {
    let name = match ptr_to_str(worker_name) {
        Some(s) => s,
        None => {
            set_last_error("worker_name is null or invalid UTF-8");
            return std::ptr::null_mut();
        }
    };
    let shm = match ptr_to_str(shm_name) {
        Some(s) => s,
        None => {
            set_last_error("shm_name is null or invalid UTF-8");
            return std::ptr::null_mut();
        }
    };

    // Derive prefix: shm_name = "{prefix}-{name}", so prefix = shm_name[..shm_name.len()-name.len()-1]
    let prefix = if shm.ends_with(&format!("-{name}")) {
        &shm[..shm.len() - name.len() - 1]
    } else {
        "slotbus"
    };

    let config = SlotBusConfig::builder()
        .name(name)
        .prefix(prefix)
        .build();

    let mut region = match ShmRegion::open(&config.region_name()) {
        Ok(r) => r,
        Err(e) => {
            set_last_error(format!("failed to open SHM region: {e}"));
            return std::ptr::null_mut();
        }
    };

    if let Err(e) = region.validate_control() {
        set_last_error(format!("invalid control region: {e}"));
        return std::ptr::null_mut();
    }

    let req_event = match NamedEvent::open(&config.request_event_name()) {
        Ok(e) => e,
        Err(e) => {
            set_last_error(format!("failed to open request event: {e}"));
            return std::ptr::null_mut();
        }
    };

    let rsp_event = match NamedEvent::open(&config.response_event_name()) {
        Ok(e) => e,
        Err(e) => {
            set_last_error(format!("failed to open response event: {e}"));
            return std::ptr::null_mut();
        }
    };

    let worker = FfiWorker {
        config,
        region: Arc::new(region),
        req_event,
        rsp_event: Arc::new(rsp_event),
        overflow_regions: Arc::new(Mutex::new(HashMap::new())),
        running: Arc::new(AtomicBool::new(true)),
        recv_thread: Mutex::new(None),
    };

    Box::into_raw(Box::new(worker))
}

/// Signal the worker to stop processing requests.
///
/// After calling this, the callback thread (if started) will exit.
/// Safe to call multiple times.
#[no_mangle]
pub unsafe extern "C" fn slotbus_worker_stop(worker: *mut FfiWorker) {
    if worker.is_null() {
        return;
    }
    let w = &*worker;
    w.running.store(false, Ordering::Release);
    // Wake the event so the receive loop can exit
    w.req_event.signal();
}

/// Free the worker handle.
///
/// Stops the worker first if still running, then joins the receive thread.
/// After this call, the pointer is invalid.
#[no_mangle]
pub unsafe extern "C" fn slotbus_worker_free(worker: *mut FfiWorker) {
    if worker.is_null() {
        return;
    }
    let w = &*worker;
    w.running.store(false, Ordering::Release);
    w.req_event.signal();

    // Join the receive thread if running
    if let Ok(mut guard) = w.recv_thread.lock() {
        if let Some(handle) = guard.take() {
            let _ = handle.join();
        }
    }

    drop(Box::from_raw(worker));
}

// ---- Callback API -----------------------------------------------------------

/// C callback type for incoming requests.
///
/// Called on a dedicated OS thread. The callback must respond via
/// `slotbus_respond()` or defer via `slotbus_defer()`. If neither is called
/// before the callback returns, a 500 response is sent automatically.
pub type SlotRequestCallback = unsafe extern "C" fn(
    worker: *mut FfiWorker,
    request: *const FfiRequest,
    user_data: *mut std::ffi::c_void,
);

/// Start the callback receive loop on a dedicated OS thread.
///
/// Returns 0 on success, -1 on failure.
/// Only one callback loop can be active per worker.
#[no_mangle]
pub unsafe extern "C" fn slotbus_worker_start(
    worker: *mut FfiWorker,
    cb: SlotRequestCallback,
    user_data: *mut std::ffi::c_void,
) -> i32 {
    if worker.is_null() {
        set_last_error("worker is null");
        return -1;
    }
    let w = &*worker;

    // Check if already running
    {
        let guard = w.recv_thread.lock().unwrap();
        if guard.is_some() {
            set_last_error("receive loop already started");
            return -1;
        }
    }

    let region = Arc::clone(&w.region);
    let rsp_event = Arc::clone(&w.rsp_event);
    let overflow_regions = Arc::clone(&w.overflow_regions);
    let config = w.config.clone();
    let running = Arc::clone(&w.running);
    // Raw pointers are !Send. Wrap in usize for the thread boundary.
    // SAFETY: caller guarantees the worker outlives the receive thread,
    // and the callback is safe to invoke from any thread.
    let worker_usize = worker as usize;
    let user_data_usize = user_data as usize;

    let handle = std::thread::Builder::new()
        .name(format!("{}-ffi-recv", config.name))
        .spawn(move || {
            let worker_ptr = worker_usize as *mut FfiWorker;
            let user_data = user_data_usize as *mut std::ffi::c_void;

            // Open the request event once for this thread (separate handle from the worker's)
            #[cfg(windows)]
            let recv_event = NamedEvent::open(&config.request_event_name()).ok();

            while running.load(Ordering::Acquire) {
                // Wait for request signal (with timeout as safety fallback)
                #[cfg(windows)]
                {
                    if let Some(ref evt) = recv_event {
                        evt.wait_timeout(config.wait_timeout_ms);
                    } else {
                        std::thread::sleep(std::time::Duration::from_millis(100));
                    }
                }
                #[cfg(not(windows))]
                {
                    std::thread::sleep(std::time::Duration::from_millis(
                        config.wait_timeout_ms as u64,
                    ));
                }

                // Scan slots for Ready requests
                for i in 0..region.num_slots() {
                    if !running.load(Ordering::Relaxed) {
                        break;
                    }

                    let slot = region.slot(i);
                    if slot
                        .status
                        .compare_exchange(
                            SLOT_READY,
                            SLOT_CLAIMED,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_err()
                    {
                        continue;
                    }

                    // Read request from SHM
                    let ffi_req = match read_and_decode(&region, i, &config) {
                        Some(r) => r,
                        None => {
                            // Failed to decode — release slot
                            slot.status.store(SLOT_FREE, Ordering::Release);
                            continue;
                        }
                    };

                    let req_ptr = Box::into_raw(Box::new(ffi_req));

                    // Invoke callback
                    cb(worker_ptr, req_ptr, user_data);

                    // Check if callback responded or deferred
                    let req = &*req_ptr;
                    if !req.responded.load(Ordering::Acquire)
                        && !req.deferred.load(Ordering::Acquire)
                    {
                        // Auto-send 500
                        let body = b"Internal Server Error";
                        let _ = send_response_to_slot(
                            &region,
                            &rsp_event,
                            &overflow_regions,
                            &config,
                            i,
                            500,
                            body,
                            "text/plain",
                            &[],
                        );
                    }

                    // Drop the request (CStrings freed)
                    if !req.deferred.load(Ordering::Acquire) {
                        drop(Box::from_raw(req_ptr));
                    }
                }
            }
        })
        .ok();

    match handle {
        Some(h) => {
            *w.recv_thread.lock().unwrap() = Some(h);
            0
        }
        None => {
            set_last_error("failed to spawn receive thread");
            -1
        }
    }
}

// ---- Polling API ------------------------------------------------------------

/// Non-blocking poll for the next incoming request.
///
/// Returns NULL if no request is available. The returned pointer must be
/// freed by calling `slotbus_poll_respond()` or `slotbus_poll_drop()`.
#[no_mangle]
pub unsafe extern "C" fn slotbus_poll(worker: *mut FfiWorker) -> *mut FfiRequest {
    if worker.is_null() {
        return std::ptr::null_mut();
    }
    let w = &*worker;

    for i in 0..w.region.num_slots() {
        let slot = w.region.slot(i);
        if slot
            .status
            .compare_exchange(
                SLOT_READY,
                SLOT_CLAIMED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            match read_and_decode(&w.region, i, &w.config) {
                Some(req) => return Box::into_raw(Box::new(req)),
                None => {
                    slot.status.store(SLOT_FREE, Ordering::Release);
                }
            }
        }
    }

    std::ptr::null_mut()
}

/// Respond to a polled request and free it.
///
/// Returns 0 on success, -1 on failure.
#[no_mangle]
pub unsafe extern "C" fn slotbus_poll_respond(
    worker: *mut FfiWorker,
    request: *mut FfiRequest,
    status: u16,
    body: *const u8,
    body_len: usize,
    content_type: *const c_char,
) -> i32 {
    if worker.is_null() || request.is_null() {
        set_last_error("null pointer");
        return -1;
    }
    let w = &*worker;
    let req = &*request;

    if req.responded.load(Ordering::Acquire) {
        set_last_error("already responded");
        return -1;
    }

    let body_slice = if body.is_null() || body_len == 0 {
        &[]
    } else {
        std::slice::from_raw_parts(body, body_len)
    };
    let ct = ptr_to_str(content_type).unwrap_or("application/octet-stream");

    let result = send_response_to_slot(
        &w.region,
        &w.rsp_event,
        &w.overflow_regions,
        &w.config,
        req.slot_index,
        status,
        body_slice,
        ct,
        &[],
    );

    req.responded.store(true, Ordering::Release);
    drop(Box::from_raw(request));

    match result {
        Ok(()) => 0,
        Err(e) => {
            set_last_error(e);
            -1
        }
    }
}

/// Drop a polled request without responding (sends 500 automatically).
#[no_mangle]
pub unsafe extern "C" fn slotbus_poll_drop(
    worker: *mut FfiWorker,
    request: *mut FfiRequest,
) {
    if worker.is_null() || request.is_null() {
        return;
    }
    let w = &*worker;
    let req = &*request;

    if !req.responded.load(Ordering::Acquire) && !req.deferred.load(Ordering::Acquire) {
        let _ = send_response_to_slot(
            &w.region,
            &w.rsp_event,
            &w.overflow_regions,
            &w.config,
            req.slot_index,
            500,
            b"Internal Server Error",
            "text/plain",
            &[],
        );
    }

    drop(Box::from_raw(request));
}

/// Get the OS event handle for integrating with external event loops.
///
/// On Windows, returns a `HANDLE` (auto-reset event). The caller can pass this
/// to `WaitForSingleObject`, libuv's `uv_poll_init`, or similar.
/// Returns -1 on error or non-Windows platforms.
#[no_mangle]
pub unsafe extern "C" fn slotbus_worker_wait_handle(worker: *mut FfiWorker) -> isize {
    if worker.is_null() {
        return -1;
    }
    #[cfg(windows)]
    {
        let w = &*worker;
        w.req_event.raw_handle()
    }
    #[cfg(not(windows))]
    {
        -1
    }
}

// ---- Request accessors ------------------------------------------------------

#[no_mangle]
pub unsafe extern "C" fn slotbus_request_id(req: *const FfiRequest) -> *const c_char {
    if req.is_null() {
        return empty_cstr();
    }
    (*req).req_id.as_ptr()
}

#[no_mangle]
pub unsafe extern "C" fn slotbus_request_method(req: *const FfiRequest) -> *const c_char {
    if req.is_null() {
        return empty_cstr();
    }
    (*req).method.as_ptr()
}

#[no_mangle]
pub unsafe extern "C" fn slotbus_request_path(req: *const FfiRequest) -> *const c_char {
    if req.is_null() {
        return empty_cstr();
    }
    (*req).path.as_ptr()
}

#[no_mangle]
pub unsafe extern "C" fn slotbus_request_route_pattern(
    req: *const FfiRequest,
) -> *const c_char {
    if req.is_null() {
        return empty_cstr();
    }
    (*req).route_pattern.as_ptr()
}

/// Returns the query string, or NULL if none.
#[no_mangle]
pub unsafe extern "C" fn slotbus_request_query(req: *const FfiRequest) -> *const c_char {
    if req.is_null() {
        return std::ptr::null();
    }
    match &(*req).query {
        Some(q) => q.as_ptr(),
        None => std::ptr::null(),
    }
}

/// Returns a pointer to the request body and writes the length to `out_len`.
/// The pointer is valid until the request is freed.
#[no_mangle]
pub unsafe extern "C" fn slotbus_request_body(
    req: *const FfiRequest,
    out_len: *mut usize,
) -> *const u8 {
    if req.is_null() {
        if !out_len.is_null() {
            *out_len = 0;
        }
        return std::ptr::null();
    }
    let r = &*req;
    if !out_len.is_null() {
        *out_len = r.body.len();
    }
    if r.body.is_empty() {
        std::ptr::null()
    } else {
        r.body.as_ptr()
    }
}

#[no_mangle]
pub unsafe extern "C" fn slotbus_request_header_count(req: *const FfiRequest) -> u32 {
    if req.is_null() {
        return 0;
    }
    (*req).headers.len() as u32
}

/// Get a header by index. Writes name and value pointers.
/// Returns 0 on success, -1 if index is out of bounds.
#[no_mangle]
pub unsafe extern "C" fn slotbus_request_header_get(
    req: *const FfiRequest,
    i: u32,
    name: *mut *const c_char,
    value: *mut *const c_char,
) -> i32 {
    if req.is_null() {
        return -1;
    }
    let r = &*req;
    let idx = i as usize;
    if idx >= r.headers.len() {
        return -1;
    }
    if !name.is_null() {
        *name = r.headers[idx].0.as_ptr();
    }
    if !value.is_null() {
        *value = r.headers[idx].1.as_ptr();
    }
    0
}

/// Look up a header by name (case-sensitive). Returns NULL if not found.
#[no_mangle]
pub unsafe extern "C" fn slotbus_request_header(
    req: *const FfiRequest,
    name: *const c_char,
) -> *const c_char {
    if req.is_null() || name.is_null() {
        return std::ptr::null();
    }
    let needle = match CStr::from_ptr(name).to_str() {
        Ok(s) => s,
        Err(_) => return std::ptr::null(),
    };
    let r = &*req;
    for (k, v) in &r.headers {
        if k.to_str() == Ok(needle) {
            return v.as_ptr();
        }
    }
    std::ptr::null()
}

#[no_mangle]
pub unsafe extern "C" fn slotbus_request_path_param_count(req: *const FfiRequest) -> u32 {
    if req.is_null() {
        return 0;
    }
    (*req).path_params.len() as u32
}

/// Get a path parameter by index. Writes name and value pointers.
/// Returns 0 on success, -1 if index is out of bounds.
#[no_mangle]
pub unsafe extern "C" fn slotbus_request_path_param_get(
    req: *const FfiRequest,
    i: u32,
    name: *mut *const c_char,
    value: *mut *const c_char,
) -> i32 {
    if req.is_null() {
        return -1;
    }
    let r = &*req;
    let idx = i as usize;
    if idx >= r.path_params.len() {
        return -1;
    }
    if !name.is_null() {
        *name = r.path_params[idx].0.as_ptr();
    }
    if !value.is_null() {
        *value = r.path_params[idx].1.as_ptr();
    }
    0
}

/// Look up a path parameter by name. Returns NULL if not found.
#[no_mangle]
pub unsafe extern "C" fn slotbus_request_path_param(
    req: *const FfiRequest,
    name: *const c_char,
) -> *const c_char {
    if req.is_null() || name.is_null() {
        return std::ptr::null();
    }
    let needle = match CStr::from_ptr(name).to_str() {
        Ok(s) => s,
        Err(_) => return std::ptr::null(),
    };
    let r = &*req;
    for (k, v) in &r.path_params {
        if k.to_str() == Ok(needle) {
            return v.as_ptr();
        }
    }
    std::ptr::null()
}

// ---- Responding -------------------------------------------------------------

/// Send a response for a request.
///
/// Returns 0 on success, -1 on failure.
/// The request pointer remains valid after this call (it is not freed).
#[no_mangle]
pub unsafe extern "C" fn slotbus_respond(
    worker: *mut FfiWorker,
    request: *const FfiRequest,
    status: u16,
    body: *const u8,
    body_len: usize,
    content_type: *const c_char,
) -> i32 {
    if worker.is_null() || request.is_null() {
        set_last_error("null pointer");
        return -1;
    }
    let w = &*worker;
    let req = &*request;

    if req.responded.load(Ordering::Acquire) {
        set_last_error("already responded");
        return -1;
    }

    let body_slice = if body.is_null() || body_len == 0 {
        &[]
    } else {
        std::slice::from_raw_parts(body, body_len)
    };
    let ct = ptr_to_str(content_type).unwrap_or("application/octet-stream");

    match send_response_to_slot(
        &w.region,
        &w.rsp_event,
        &w.overflow_regions,
        &w.config,
        req.slot_index,
        status,
        body_slice,
        ct,
        &[],
    ) {
        Ok(()) => {
            req.responded.store(true, Ordering::Release);
            0
        }
        Err(e) => {
            set_last_error(e);
            -1
        }
    }
}

/// Send a response with custom headers.
///
/// `header_names` and `header_values` are parallel arrays of `header_count` C strings.
/// Returns 0 on success, -1 on failure.
#[no_mangle]
pub unsafe extern "C" fn slotbus_respond_headers(
    worker: *mut FfiWorker,
    request: *const FfiRequest,
    status: u16,
    body: *const u8,
    body_len: usize,
    content_type: *const c_char,
    header_names: *const *const c_char,
    header_values: *const *const c_char,
    header_count: u32,
) -> i32 {
    if worker.is_null() || request.is_null() {
        set_last_error("null pointer");
        return -1;
    }
    let w = &*worker;
    let req = &*request;

    if req.responded.load(Ordering::Acquire) {
        set_last_error("already responded");
        return -1;
    }

    let body_slice = if body.is_null() || body_len == 0 {
        &[]
    } else {
        std::slice::from_raw_parts(body, body_len)
    };
    let ct = ptr_to_str(content_type).unwrap_or("application/octet-stream");

    let mut headers = Vec::new();
    if !header_names.is_null() && !header_values.is_null() && header_count > 0 {
        let names = std::slice::from_raw_parts(header_names, header_count as usize);
        let values = std::slice::from_raw_parts(header_values, header_count as usize);
        for (n, v) in names.iter().zip(values.iter()) {
            if let (Some(name), Some(value)) = (ptr_to_str(*n), ptr_to_str(*v)) {
                headers.push((name.to_string(), value.to_string()));
            }
        }
    }

    match send_response_to_slot(
        &w.region,
        &w.rsp_event,
        &w.overflow_regions,
        &w.config,
        req.slot_index,
        status,
        body_slice,
        ct,
        &headers,
    ) {
        Ok(()) => {
            req.responded.store(true, Ordering::Release);
            0
        }
        Err(e) => {
            set_last_error(e);
            -1
        }
    }
}

// ---- Deferred response ------------------------------------------------------

/// Create a deferred response handle from a request.
///
/// The request is marked as deferred and will NOT receive an auto-500.
/// The caller must eventually call `slotbus_deferred_respond()` or
/// `slotbus_deferred_free()`.
///
/// Returns NULL on failure.
#[no_mangle]
pub unsafe extern "C" fn slotbus_defer(
    worker: *mut FfiWorker,
    request: *const FfiRequest,
) -> *mut FfiSlotHandle {
    if worker.is_null() || request.is_null() {
        set_last_error("null pointer");
        return std::ptr::null_mut();
    }
    let w = &*worker;
    let req = &*request;

    if req.responded.load(Ordering::Acquire) {
        set_last_error("already responded");
        return std::ptr::null_mut();
    }
    if req.deferred.load(Ordering::Acquire) {
        set_last_error("already deferred");
        return std::ptr::null_mut();
    }

    req.deferred.store(true, Ordering::Release);

    let handle = FfiSlotHandle {
        region: Arc::clone(&w.region),
        rsp_event: Arc::clone(&w.rsp_event),
        overflow_regions: Arc::clone(&w.overflow_regions),
        config: w.config.clone(),
        slot_index: req.slot_index,
        responded: AtomicBool::new(false),
    };

    Box::into_raw(Box::new(handle))
}

/// Send a response via a deferred handle, then free the handle.
///
/// Returns 0 on success, -1 on failure.
#[no_mangle]
pub unsafe extern "C" fn slotbus_deferred_respond(
    handle: *mut FfiSlotHandle,
    status: u16,
    body: *const u8,
    body_len: usize,
    content_type: *const c_char,
) -> i32 {
    if handle.is_null() {
        set_last_error("null handle");
        return -1;
    }
    let h = &*handle;

    if h.responded.load(Ordering::Acquire) {
        set_last_error("already responded");
        drop(Box::from_raw(handle));
        return -1;
    }

    let body_slice = if body.is_null() || body_len == 0 {
        &[]
    } else {
        std::slice::from_raw_parts(body, body_len)
    };
    let ct = ptr_to_str(content_type).unwrap_or("application/octet-stream");

    let result = send_response_to_slot(
        &h.region,
        &h.rsp_event,
        &h.overflow_regions,
        &h.config,
        h.slot_index,
        status,
        body_slice,
        ct,
        &[],
    );

    h.responded.store(true, Ordering::Release);
    drop(Box::from_raw(handle));

    match result {
        Ok(()) => 0,
        Err(e) => {
            set_last_error(e);
            -1
        }
    }
}

/// Free a deferred handle, sending a 500 response if not yet responded.
#[no_mangle]
pub unsafe extern "C" fn slotbus_deferred_free(handle: *mut FfiSlotHandle) {
    if handle.is_null() {
        return;
    }
    let h = &*handle;
    if !h.responded.load(Ordering::Acquire) {
        let _ = send_response_to_slot(
            &h.region,
            &h.rsp_event,
            &h.overflow_regions,
            &h.config,
            h.slot_index,
            500,
            b"Internal Server Error",
            "text/plain",
            &[],
        );
    }
    drop(Box::from_raw(handle));
}

// ---- Event emission ---------------------------------------------------------

/// Emit an event to the hub via HTTP POST.
///
/// This is a blocking call (uses ureq, no tokio). All parameters must be
/// valid UTF-8 C strings. Returns 0 on success, -1 on failure.
#[no_mangle]
pub unsafe extern "C" fn slotbus_emit_event(
    hub_url: *const c_char,
    source: *const c_char,
    event_type: *const c_char,
    data: *const c_char,
) -> i32 {
    let url = match ptr_to_str(hub_url) {
        Some(s) => s,
        None => {
            set_last_error("hub_url is null or invalid UTF-8");
            return -1;
        }
    };
    let src = match ptr_to_str(source) {
        Some(s) => s,
        None => {
            set_last_error("source is null or invalid UTF-8");
            return -1;
        }
    };
    let evt = match ptr_to_str(event_type) {
        Some(s) => s,
        None => {
            set_last_error("event_type is null or invalid UTF-8");
            return -1;
        }
    };
    let d = match ptr_to_str(data) {
        Some(s) => s,
        None => {
            set_last_error("data is null or invalid UTF-8");
            return -1;
        }
    };

    let body = serde_json::json!({
        "source": src,
        "event_type": evt,
        "data": d,
    });

    let endpoint = format!("{}/internal/emit", url.trim_end_matches('/'));

    match ureq::post(&endpoint)
        .set("Content-Type", "application/json")
        .send_bytes(body.to_string().as_bytes())
    {
        Ok(_) => 0,
        Err(e) => {
            set_last_error(format!("emit failed: {e}"));
            -1
        }
    }
}

// ---- Internal helpers -------------------------------------------------------

fn ptr_to_str<'a>(ptr: *const c_char) -> Option<&'a str> {
    if ptr.is_null() {
        return None;
    }
    unsafe { CStr::from_ptr(ptr).to_str().ok() }
}

fn empty_cstr() -> *const c_char {
    b"\0".as_ptr() as *const c_char
}

/// Read a request from a claimed slot and decode it into an FfiRequest.
unsafe fn read_and_decode(
    region: &ShmRegion,
    slot_index: usize,
    config: &SlotBusConfig,
) -> Option<FfiRequest> {
    let (req_id, method_u8, meta, body) =
        region::read_request(region, slot_index, config).ok()?;

    let method_str = u8_to_method(method_u8);

    let headers: Vec<(CString, CString)> = meta
        .headers
        .iter()
        .filter_map(|(k, v)| Some((CString::new(k.as_str()).ok()?, CString::new(v.as_str()).ok()?)))
        .collect();

    let path_params: Vec<(CString, CString)> = meta
        .path_params
        .iter()
        .filter_map(|(k, v)| Some((CString::new(k.as_str()).ok()?, CString::new(v.as_str()).ok()?)))
        .collect();

    Some(FfiRequest {
        slot_index,
        req_id: CString::new(req_id).ok()?,
        method: CString::new(method_str).ok()?,
        path: CString::new(meta.path).ok()?,
        route_pattern: CString::new(meta.route_pattern).ok()?,
        query: meta.query.and_then(|q| CString::new(q).ok()),
        body,
        headers,
        path_params,
        responded: AtomicBool::new(false),
        deferred: AtomicBool::new(false),
    })
}

/// Write a response to a slot and signal the hub.
fn send_response_to_slot(
    region: &ShmRegion,
    rsp_event: &NamedEvent,
    overflow_regions: &Mutex<HashMap<usize, ShmRegion>>,
    config: &SlotBusConfig,
    slot_index: usize,
    status: u16,
    body: &[u8],
    content_type: &str,
    headers: &[(String, String)],
) -> Result<(), String> {
    let resp_meta = ResponseMeta {
        content_type: content_type.to_string(),
        headers: headers.to_vec(),
    };
    let meta_bytes =
        postcard::to_allocvec(&resp_meta).map_err(|e| format!("serialize error: {e}"))?;

    let overflow = region::write_response(region, slot_index, status, &meta_bytes, body, config)
        .map_err(|e| format!("write_response error: {e}"))?;

    if let Some(ovf) = overflow {
        overflow_regions
            .lock()
            .unwrap()
            .insert(slot_index, ovf);
    }

    rsp_event.signal();
    Ok(())
}
