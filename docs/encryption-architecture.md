# SlotBus + Encryption: Decrypt Once, Process Many

## The Problem with Traditional Architectures

In conventional multi-service architectures, encryption creates a costly tax on every network hop:

```
Encrypted Data → Service A (decrypt, process, re-encrypt, send over network)
             → Service B (decrypt again, process, re-encrypt, send over network)
             → Service C (decrypt again, process, re-encrypt, send)
```

Every boundary crossing requires a full decrypt-process-encrypt cycle. AES-GCM and ChaCha20 aren't free, and RSA/ECDH key exchanges are expensive. For N services, you pay 2N cryptographic operations.

## The SlotBus Model

SlotBus's shared memory architecture eliminates this overhead entirely:

```
Encrypted Data → Hub (decrypt once into shared memory slot)
                  ├── Worker A reads plaintext directly from slot
                  ├── Worker B reads plaintext directly from slot
                  └── Worker C reads plaintext directly from slot
                → Hub encrypts final result once on the way out
```

**One decrypt. One encrypt. Multiple processors.** The plaintext never leaves the shared memory region, never crosses a network boundary, never gets serialized into a socket buffer.

## Why This Matters

### 1. Reduced Cryptographic Surface Area

Every encrypt/decrypt cycle is a potential side-channel attack vector (timing attacks, cache attacks). Fewer cycles means fewer opportunities for leakage. With SlotBus, crypto operations happen exactly twice — in at the boundary, out at the boundary — instead of 2N times for N services.

### 2. Key Distribution Simplifies Dramatically

In a traditional microservice mesh with encryption, every service needs access to keys — either shared symmetric keys or its own key pair plus a PKI for certificate exchange.

With SlotBus, only the **hub** needs the keys. Workers never need decryption keys because they receive plaintext via shared memory:

- Smaller blast radius if a worker is compromised (no keys to steal)
- No key rotation coordination across services
- No mTLS between services
- No secrets management for worker processes

### 3. The Shared Memory Boundary IS the Trust Boundary

Shared memory between processes on the same machine is protected by OS-level access controls (file permissions on the SHM region). The security model becomes:

- **External boundary**: Encryption (TLS, application-layer crypto)
- **Internal boundary**: OS process isolation + SHM permissions

This is arguably *stronger* than mTLS between localhost services. mTLS on localhost still puts plaintext into kernel socket buffers, whereas SHM with proper permissions keeps data in a controlled memory region.

### 4. Symmetric Encryption for Slot Transport

For **multi-machine** scenarios where SlotBus extends across hosts:

- Hub-to-hub communication can use a symmetric session key
- The slot format is already a fixed, well-defined binary layout
- Encrypt the entire slot content with AES-GCM as a single block
- Derive the IV/nonce from `(worker_id, slot_index, sequence_number)` — data already available in the protocol
- Zero additional metadata overhead

### 5. Asymmetric Encryption for Worker Registration

The registration flow (`POST /internal/register`) is a natural handshake point:

- Worker generates a key pair at startup
- Sends public key during registration
- Hub encrypts the SHM name / bootstrap data with worker's public key
- Only that specific worker can access its assigned memory region
- Prevents rogue processes from connecting to SHM regions they shouldn't access

## Use Cases

### Healthcare / HIPAA

Patient data arrives encrypted, gets decrypted once at the hub. Multiple workers (billing, clinical, analytics) all process the same plaintext record without any of them needing access to encryption keys or storing encrypted data in transit buffers.

### Financial Processing

Encrypted transaction data decrypts at ingress. Multiple workers handle validation, fraud detection, routing, and ledger updates simultaneously from the same slot — no re-encryption between stages.

### Multi-Tenant SaaS

Each tenant's data encrypted with tenant-specific keys. The hub manages key lookup and decryption. Workers are tenant-agnostic — they just see plaintext in their slots. Tenant isolation comes from slot assignment, not from workers having per-tenant keys.

## Risks and Mitigations

### Plaintext in Memory

The shared memory region contains plaintext. If an attacker can read arbitrary process memory (Spectre-class attacks, `/proc/pid/mem`), it's exposed.

**Mitigations:**
- `mlock()` to prevent swapping plaintext to disk
- Zero slot memory on free (`SLOT_FREE` transition)
- Restrict `/proc/pid/mem` access via kernel hardening

### SHM Permissions

Default SHM permissions may be too open on some platforms.

**Mitigations:**
- Hub creates SHM regions with owner-only read/write permissions
- Validate worker identity during registration before granting SHM access

### Core Dumps

A crash could dump plaintext slots to disk.

**Mitigations:**
- Disable core dumps for hub/worker processes (`RLIMIT_CORE = 0`)
- Use encrypted swap if core dumps are needed for debugging
