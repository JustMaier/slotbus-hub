'use strict';

const { SlotWorker, Response } = require('slotbus');

const hubUrl = process.env.HUB_URL || 'http://localhost:3200';
const app = new SlotWorker(hubUrl, 'node-api');

// ---- Routes -------------------------------------------------------------------

// Simple status check
app.get('/status', () => {
    return { ok: true, worker: 'node', pid: process.pid };
});

// Echo back the request body
app.post('/echo', (req) => {
    return req.json();
});

// Path parameter extraction
app.get('/items/:id', (req) => {
    return { id: req.params.id, name: `Item ${req.params.id}` };
});

// Async handler (simulated DB lookup)
app.get('/users/:id', async (req) => {
    // Simulate async work
    await new Promise(resolve => setTimeout(resolve, 10));
    return {
        id: req.params.id,
        name: `User ${req.params.id}`,
        fetched_at: new Date().toISOString(),
    };
});

// Custom Response object with status code
app.post('/validate', (req) => {
    const body = req.json();
    if (!body.name) {
        return new Response(
            JSON.stringify({ error: 'name is required' }),
            400,
            'application/json'
        );
    }
    return { valid: true, name: body.name };
});

// Return raw text
app.get('/health', () => {
    return Response.text('OK');
});

// ---- Start --------------------------------------------------------------------

app.listen().then(() => {
    console.log(`Worker PID: ${process.pid}`);
    console.log(`Library version: ${SlotWorker.version()}`);
}).catch((err) => {
    console.error('Failed to start worker:', err);
    process.exit(1);
});
