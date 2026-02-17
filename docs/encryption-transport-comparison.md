# SlotBus Encryption Model vs. Network-Based Transports

**A Technical Comparison for Multi-Process Architectures**

---

## Executive Summary

When multiple services process the same encrypted data on a single machine, traditional network-based transports (HTTP, gRPC, WebSockets) require a full encrypt/decrypt cycle at every service boundary. SlotBus's shared memory architecture eliminates this overhead entirely — data is decrypted once at ingress and encrypted once at egress, regardless of how many workers process it.

For a pipeline of N services, this reduces cryptographic operations from 2N to 2, removes serialization overhead between services, and confines key material to a single process.

---

## The Problem

Modern architectures often split work across multiple services on the same machine — validation, business logic, analytics, logging. When data is encrypted (as required by HIPAA, PCI-DSS, SOC 2, etc.), every boundary crossing between services incurs:

1. **TLS decryption** at the receiving service
2. **Deserialization** from wire format into application memory
3. **Processing**
4. **Re-serialization** into wire format
5. **TLS encryption** before sending to the next service

Each cycle adds latency, CPU cost, and — critically — expands the number of processes that must hold encryption keys.

---

## Transport Comparison

### Per-Request Cost for N Workers Processing the Same Data

| Metric | HTTP/REST | gRPC | WebSocket | SlotBus |
|---|---|---|---|---|
| Encrypt/decrypt operations | 2N | 2N | 2N | 2 |
| Serialization passes | 2N (JSON) | 2N (Protobuf) | 2N | 0 |
| Kernel boundary crossings | 2N | 2N | 2N | 0 |
| Processes holding keys | N + 1 | N + 1 | N + 1 | 1 |
| Localhost round-trip latency | 5–20 ms | 1–5 ms | 1–3 ms | 0.1–0.8 ms |

*With 5 workers, HTTP performs 10 encrypt/decrypt operations per request. SlotBus performs 2.*

### What Each Transport Does Well

**gRPC** offers HTTP/2 multiplexing, efficient Protobuf serialization, and a mature ecosystem with codegen for 10+ languages. It is the standard for inter-service RPC and supports streaming natively.

**HTTP/REST** is universally understood, tooling-rich, and works with every language and framework. It remains the default for external APIs and human-debuggable interfaces.

**WebSockets** provide low-overhead bidirectional streaming over persistent connections. They excel at real-time communication patterns (notifications, live updates).

**SlotBus** eliminates the network stack entirely for same-machine communication. Data is read and written directly in shared memory with lock-free atomic coordination. No serialization, no socket buffers, no kernel transit.

### Where Each Falls Short

**gRPC, HTTP, WebSocket** — all three share the same fundamental constraint: data must leave process memory, enter the kernel network stack, and be received into another process's memory. Even on localhost, this path means:
- Data is copied into socket send/receive buffers
- TLS (if enabled) adds a full encrypt/decrypt cycle per hop
- Serialization/deserialization occurs at every boundary

**SlotBus** is currently limited to same-machine communication. It requires FFI bindings for non-Rust languages (SDKs exist for Python and Node.js) and is opaque to standard network observability tools (Wireshark, OpenTelemetry trace propagation).

---

## Encryption Architecture Implications

### Key Management

| | Network Transports | SlotBus |
|---|---|---|
| Which processes need keys? | Every service | Hub only |
| mTLS certificates required? | Yes (per service) | No |
| Key rotation scope | All services simultaneously | Single process |
| Blast radius of key compromise | Compromised service exposes its keys | Workers have no keys to steal |

In a network-based architecture, each service must authenticate to its peers and decrypt incoming traffic. This means distributing keys or certificates to every service, managing rotation across all of them, and accepting that any compromised service leaks its key material.

With SlotBus, workers receive plaintext via shared memory. They never possess encryption keys. A compromised worker can read data in its assigned memory slots, but it cannot decrypt traffic from other sources or impersonate the hub.

### Trust Boundaries

Network transports rely on TLS to establish trust between services. On localhost, this means:
- Plaintext exists in kernel socket buffers (readable with sufficient privileges)
- Each service must validate certificates or tokens
- The attack surface includes the TLS implementation in every service

SlotBus replaces network-layer trust with OS-level process isolation:
- Shared memory regions are protected by file-system permissions
- Only processes with the correct SHM handle (provided during registration) can access a region
- No plaintext in kernel socket buffers — data stays in userspace shared memory

### Cryptographic Surface Area

Every encrypt/decrypt operation is a potential side-channel vector (timing attacks, cache-line attacks). Reducing the number of operations reduces exposure:

- **5 services over gRPC with mTLS**: 10 TLS operations per request, 5 processes running TLS libraries
- **5 workers via SlotBus**: 2 crypto operations per request, 1 process running a TLS library

---

## Recommended Architecture

The strongest architecture uses SlotBus for intra-machine fan-out and network transports for inter-machine communication:

```
Client ──TLS──▶ Hub (decrypt once)
                  ├── Worker A ──── shared memory (plaintext)
                  ├── Worker B ──── shared memory (plaintext)
                  └── Worker C ──── shared memory (plaintext)
                         │
                    Hub encrypts final result once
                         │
                         ▼
                      Response ──TLS──▶ Client
```

For distributed deployments spanning multiple machines:

```
Machine 1                              Machine 2
─────────                              ─────────
Hub ──── Workers (SHM)          Hub ──── Workers (SHM)
  │                               ▲
  └──── gRPC/mTLS ────────────────┘
```

Each machine handles encryption at its own hub. Workers on each machine read plaintext from shared memory. Inter-machine traffic uses standard encrypted transports.

This hybrid approach:
- Minimizes crypto operations on each machine (2 per machine, not 2N)
- Maintains standard encryption for data in transit between machines
- Keeps key material confined to hub processes
- Preserves compatibility with existing infrastructure (load balancers, service meshes, observability)

---

## Compliance Considerations

| Requirement | Network Transport Approach | SlotBus Approach |
|---|---|---|
| Data encrypted in transit | TLS between every service | TLS at external boundary; SHM internally (no transit) |
| Key access minimization | Difficult — all services need keys | Natural — only hub holds keys |
| Audit trail for key access | Must instrument every service | Single process to audit |
| Data at rest protection | N/A (in-memory) | `mlock()` prevents swap; slot zeroing on free |
| Core dump risk | Standard risk per service | Disable core dumps or encrypt swap |

Shared memory communication between processes on the same machine is not "data in transit" under most compliance frameworks — it never enters a network interface. This can simplify compliance posture significantly.

---

## Risks and Mitigations

**Plaintext in shared memory**: An attacker with access to process memory (Spectre-class vulnerabilities, debug access) could read plaintext from slots.
*Mitigation*: `mlock()` to prevent paging to disk, zero slot memory on free, restrict ptrace/debug access.

**SHM permission misconfiguration**: Overly permissive shared memory regions could allow unauthorized processes to read data.
*Mitigation*: Hub creates SHM regions with owner-only permissions. Worker identity is validated during registration.

**Single point of failure**: The hub becomes the sole encryption/decryption point.
*Mitigation*: Standard high-availability patterns (process supervision, health checks, restart policies).

**Observability gap**: Shared memory traffic is invisible to network monitoring tools.
*Mitigation*: Application-level logging and metrics within the hub. SlotBus exposes slot utilization and latency metrics.

---

## Summary

For same-machine multi-process architectures handling encrypted data, SlotBus provides a measurable reduction in cryptographic overhead, a simpler key management model, and a smaller attack surface compared to network-based transports. It is not a replacement for gRPC or HTTP — it operates at a different layer — but it eliminates the most expensive redundancy in on-host service communication: re-encrypting data that never needed to leave memory in the first place.
