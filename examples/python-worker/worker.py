#!/usr/bin/env python3
"""Example slotbus worker in Python.

Registers routes with the hub, opens the SHM region, and handles requests
using the Flask-like slotbus SDK.

Usage:
    python worker.py

Requires slotbus-hub running on localhost:3200.
"""

import os
import sys
import time

# Allow running from the examples directory without installing the package
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "sdks", "python"))

from slotbus import SlotWorker, Response

hub_url = os.environ.get("HUB_URL", "http://localhost:3200")
app = SlotWorker(hub_url, name="python-api")


@app.route("GET", "/status")
def status(req):
    """Health check endpoint."""
    return {"ok": True, "worker": "python", "time": time.time()}


@app.route("POST", "/echo")
def echo(req):
    """Echo back the request body as JSON."""
    return req.json()


@app.route("GET", "/items/{id}")
def get_item(req):
    """Retrieve an item by ID from path parameters."""
    item_id = req.params["id"]
    return {"id": item_id, "name": f"Item {item_id}"}


@app.route("POST", "/greet")
def greet(req):
    """Custom response with explicit status code."""
    body = req.json()
    name = body.get("name", "World")
    return Response(
        body=f"Hello, {name}!",
        status=200,
        content_type="text/plain",
    )


if __name__ == "__main__":
    print(f"Starting Python worker (hub: {hub_url})...")
    app.run()
