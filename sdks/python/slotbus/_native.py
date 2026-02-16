"""ctypes declarations for slotbus_ffi shared library."""

from __future__ import annotations

import ctypes
import os
import platform
import sys
from ctypes import (
    CFUNCTYPE,
    POINTER,
    c_char_p,
    c_int32,
    c_size_t,
    c_uint8,
    c_uint16,
    c_uint32,
    c_void_p,
)


def _find_library() -> str:
    """Locate the slotbus FFI shared library.

    Search order:
      1. SLOTBUS_LIB env var (explicit path)
      2. Relative to this file: ../../../target/release/ (dev layout)
      3. Common system paths
    """
    system = platform.system()
    if system == "Windows":
        lib_name = "slotbus_ffi.dll"
    elif system == "Darwin":
        lib_name = "libslotbus_ffi.dylib"
    else:
        lib_name = "libslotbus_ffi.so"

    # 1. Explicit env var
    env_path = os.environ.get("SLOTBUS_LIB")
    if env_path:
        if os.path.isfile(env_path):
            return env_path
        raise OSError(f"SLOTBUS_LIB points to non-existent file: {env_path}")

    # 2. Dev layout: sdks/python/slotbus/_native.py -> ../../.. -> target/release/
    this_dir = os.path.dirname(os.path.abspath(__file__))
    dev_path = os.path.join(this_dir, "..", "..", "..", "target", "release", lib_name)
    dev_path = os.path.normpath(dev_path)
    if os.path.isfile(dev_path):
        return dev_path

    # 3. Common system paths
    search_dirs = [
        os.getcwd(),
        os.path.dirname(sys.executable),
    ]

    if system == "Windows":
        search_dirs.append(os.path.join(os.environ.get("LOCALAPPDATA", ""), "slotbus"))
    else:
        search_dirs.extend([
            "/usr/local/lib",
            "/usr/lib",
            os.path.expanduser("~/.local/lib"),
        ])

    for d in search_dirs:
        candidate = os.path.join(d, lib_name)
        if os.path.isfile(candidate):
            return candidate

    raise OSError(
        f"Cannot find {lib_name}. Set SLOTBUS_LIB env var to the full path, "
        f"or build slotbus-ffi in release mode (cargo build -p slotbus-ffi --release)."
    )


# ---------------------------------------------------------------------------
# Load library
# ---------------------------------------------------------------------------

_lib_path = _find_library()
_lib = ctypes.cdll.LoadLibrary(_lib_path)

# ---------------------------------------------------------------------------
# Callback type
# ---------------------------------------------------------------------------

# void (*SlotRequestCallback)(SlotWorker*, const SlotRequest*, void* user_data)
SlotRequestCallback = CFUNCTYPE(None, c_void_p, c_void_p, c_void_p)

# ---------------------------------------------------------------------------
# Function declarations
# ---------------------------------------------------------------------------

# -- Error handling --

slotbus_last_error = _lib.slotbus_last_error
slotbus_last_error.argtypes = []
slotbus_last_error.restype = c_char_p

# -- Lifecycle --

slotbus_worker_open = _lib.slotbus_worker_open
slotbus_worker_open.argtypes = [c_char_p, c_char_p]
slotbus_worker_open.restype = c_void_p

slotbus_worker_start = _lib.slotbus_worker_start
slotbus_worker_start.argtypes = [c_void_p, SlotRequestCallback, c_void_p]
slotbus_worker_start.restype = c_int32

slotbus_worker_stop = _lib.slotbus_worker_stop
slotbus_worker_stop.argtypes = [c_void_p]
slotbus_worker_stop.restype = None

slotbus_worker_free = _lib.slotbus_worker_free
slotbus_worker_free.argtypes = [c_void_p]
slotbus_worker_free.restype = None

# -- Request accessors --

slotbus_request_id = _lib.slotbus_request_id
slotbus_request_id.argtypes = [c_void_p]
slotbus_request_id.restype = c_char_p

slotbus_request_method = _lib.slotbus_request_method
slotbus_request_method.argtypes = [c_void_p]
slotbus_request_method.restype = c_char_p

slotbus_request_path = _lib.slotbus_request_path
slotbus_request_path.argtypes = [c_void_p]
slotbus_request_path.restype = c_char_p

slotbus_request_route_pattern = _lib.slotbus_request_route_pattern
slotbus_request_route_pattern.argtypes = [c_void_p]
slotbus_request_route_pattern.restype = c_char_p

slotbus_request_query = _lib.slotbus_request_query
slotbus_request_query.argtypes = [c_void_p]
slotbus_request_query.restype = c_char_p  # NULL if none

slotbus_request_body = _lib.slotbus_request_body
slotbus_request_body.argtypes = [c_void_p, POINTER(c_size_t)]
slotbus_request_body.restype = POINTER(c_uint8)

slotbus_request_header_count = _lib.slotbus_request_header_count
slotbus_request_header_count.argtypes = [c_void_p]
slotbus_request_header_count.restype = c_uint32

slotbus_request_header_get = _lib.slotbus_request_header_get
slotbus_request_header_get.argtypes = [c_void_p, c_uint32, POINTER(c_char_p), POINTER(c_char_p)]
slotbus_request_header_get.restype = c_int32

slotbus_request_header = _lib.slotbus_request_header
slotbus_request_header.argtypes = [c_void_p, c_char_p]
slotbus_request_header.restype = c_char_p

slotbus_request_path_param_count = _lib.slotbus_request_path_param_count
slotbus_request_path_param_count.argtypes = [c_void_p]
slotbus_request_path_param_count.restype = c_uint32

slotbus_request_path_param_get = _lib.slotbus_request_path_param_get
slotbus_request_path_param_get.argtypes = [c_void_p, c_uint32, POINTER(c_char_p), POINTER(c_char_p)]
slotbus_request_path_param_get.restype = c_int32

slotbus_request_path_param = _lib.slotbus_request_path_param
slotbus_request_path_param.argtypes = [c_void_p, c_char_p]
slotbus_request_path_param.restype = c_char_p

# -- Responding --

slotbus_respond = _lib.slotbus_respond
slotbus_respond.argtypes = [c_void_p, c_void_p, c_uint16, POINTER(c_uint8), c_size_t, c_char_p]
slotbus_respond.restype = c_int32

# -- Deferred response --

slotbus_defer = _lib.slotbus_defer
slotbus_defer.argtypes = [c_void_p, c_void_p]
slotbus_defer.restype = c_void_p

slotbus_deferred_respond = _lib.slotbus_deferred_respond
slotbus_deferred_respond.argtypes = [c_void_p, c_uint16, POINTER(c_uint8), c_size_t, c_char_p]
slotbus_deferred_respond.restype = c_int32

slotbus_deferred_free = _lib.slotbus_deferred_free
slotbus_deferred_free.argtypes = [c_void_p]
slotbus_deferred_free.restype = None

# -- Event emission --

slotbus_emit_event = _lib.slotbus_emit_event
slotbus_emit_event.argtypes = [c_char_p, c_char_p, c_char_p, c_char_p]
slotbus_emit_event.restype = c_int32

# -- Misc --

slotbus_version = _lib.slotbus_version
slotbus_version.argtypes = []
slotbus_version.restype = c_char_p
