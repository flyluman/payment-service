# Infrastructure Systems Analysis

## Overview

The payment service is built on 15+ infrastructure systems that work together like organs in a body. This document explains each system, when it fires, what it does, and how they relate.

---

## The 15 Systems

### 🧠 Central Nervous System — Outbox Events

**What:** Event bus that propagates state changes outward.

**When it fires:** Every state transition writes an event to `outbox_events` in the same DB transaction. A relay goroutine polls and routes them.

**Consumers:** SNS (external), callback dispatcher (merchant), gateway initiator (calls gateway API).

**Why it matters:** Decouples "what happened" from "what to do about it." The API returns fast; side effects happen asynchronously.

**Event types and emission points:**

| Event | Trigger | Emitted From |
|-------|---------|-------------|
| `TRANSACTION_CREATED` | New payment initiated | `app/payment/service.go` |
| `GATEWAY_INITIATE` | Payment sent to gateway | `app/payment/service.go` |
| `TRANSACTION_CAPTURED` | Gateway returns captured | `app/payment/gateway.go`, `app/webhook/service.go` |
| `TRANSACTION_FAILED` | Gateway returns failed | `app/payment/gateway.go`, `app/webhook/service.go` |
| `TRANSACTION_CANCELLED` | Gateway returns cancelled | `app/payment/gateway.go`, `app/webhook/service.go` |
| `TRANSACTION_CALLBACK` | Callback URL configured | `app/payment/gateway.go`, `app/webhook/service.go` |
| `REFUND_INITIATED` | Refund requested | `app/refund/service.go` |
| `REFUND_SUCCEEDED` | Refund completed | `app/refund/process.go` |
| `REFUND_FAILED` | Refund failed | `app/refund/process.go` |
| `DISPUTE_CREATED` | New dispute filed | `app/dispute/service.go` |
| `DISPUTE_UPDATED` | Dispute resolved | `app/dispute/service.go` |

---

### 🔊 Sensory Nerves — Middleware Chain

**What:** Every HTTP request passes through 7 layers before hitting a handler.

**Chain order (outermost → innermost):**

| Layer | File | Purpose |
|-------|------|---------|
| RequestID | `middleware/middleware.go:48` | Accepts `X-Request-ID` header or generates UUID |
| TraceID | `middleware/trace.go:10` | Parses W3C `traceparent` header or generates UUID |
| RequestLog | `middleware/requestlog.go:28` | Logs method, path, status, duration; sanitizes tokens |
| Recover | `middleware/middleware.go:60` | Catches panics, returns 500 |
| Auth | `middleware/auth.go:67` | SHA-256 token lookup, tenant/user resolution. Exempt: `/health`, `/webhooks/*`, `/pay/*` |
| RateLimit | `middleware/ratelimit.go:29` | Token bucket (Valkey + local fallback). Returns 429 with `Retry-After` |
| ResponseCache | `middleware/idempotency.go:24` | HTTP-level idempotency for POST/PUT/PATCH/DELETE via `Idempotency-Key` header |

**Why it matters:** Security, observability, and resilience are applied uniformly. No handler can accidentally skip auth or rate limiting.

---

### 💪 Muscles — Gateway Initiator

**What:** The bridge between outbox events and actual gateway API calls.

**Flow:**
```
GATEWAY_INITIATE event
  → relay picks it up
    → initiator.Handler
      → ProcessGatewayInitiate
        → gateway adapter (Stripe/Razorpay/FIB)
          → FinalizeGatewayResponse
```

**Why it matters:** The API never blocks on slow gateway HTTP calls. The outbox queues the request; the relay processes it when resources are available.

---

### 🫀 Heartbeat — Circuit Breaker

**What:** Protects the system from cascading gateway failures.

**States:** CLOSED → OPEN → HALF_OPEN (Valkey-backed Lua scripts).

**File:** `internal/adapters/valkey/circuit_breaker.go`

**Trigger:** After every gateway call, `RecordSuccess`/`RecordFailure` is called.

**Behavior:**
- When failures exceed threshold → OPEN (gateway skipped)
- Cooldown: `60s × 2^(failures-1)`, capped at 240s
- HALF_OPEN: one probe request allowed
- Probe success → CLOSED; probe failure → OPEN

**Why it matters:** If Stripe is down, the system doesn't keep hammering it. It routes to alternative gateways or fails fast.

---

### 🩸 Blood Pressure — Rate Limiter

**What:** Token bucket algorithm at three levels: user, tenant, IP.

**File:** `internal/adapters/valkey/rate_limiter.go`

**Storage:** Valkey (distributed), with in-process fallback when Valkey is unreachable.

**Trigger:** Every request through middleware.

**Lua script:** Atomic read-refill-check across all three buckets.

**Fallback:** When Valkey is down (3+ consecutive failures), switches to local in-memory buckets with higher capacity.

**Why it matters:** Prevents any single user/tenant from starving resources. Graceful degradation when Valkey is down.

---

### 🔒 Immune Memory — Idempotency

**What:** Two layers prevent duplicate operations.

| Layer | Where | How |
|-------|-------|-----|
| HTTP Response Cache | Middleware | `Idempotency-Key` header → caches 2xx responses, replays on re-POST |
| App-level Guard | payment/refund/cancel services | Atomic `INSERT ... ON CONFLICT` → reserve → execute → complete |

**App-level verdicts:**
- `Created` — operation executed successfully
- `Replayed` — same key, same payload, status COMPLETED → returns cached response
- `KeyReused` — same key, **different payload** → safety rejection
- `InProgress` — key exists but operation hasn't completed yet

**Why it matters:** Network retries, user double-clicks, and relay re-delivery can't create duplicate charges.

---

### 🦴 Skeleton — Processing Lease

**What:** Prevents concurrent gateway calls on the same transaction.

**File:** `internal/adapters/postgres/lease.go`

**How:** `INSERT INTO processing_lease` with TTL when entering PROCESSING state. `ON CONFLICT` returns cached response.

**Reaper** (`internal/jobs/lease_expiry/reaper.go`):
- Runs every 60s
- Finds expired leases
- Queries gateway for actual status (read-only `CheckStatus`)
- Transitions transaction accordingly
- **Never re-attempts a payment** — only queries current state

**Why it matters:** Two relays can't both try to capture the same payment. The reaper is the safety net for crashes.

---

### 🧬 DNA — Envelope Encryption

**What:** Protects gateway credentials at rest.

**File:** `internal/adapters/encryption/envelope.go`

**Hierarchy:**
```
Master Key (AES-256, from ENCRYPTION_KEY env)
  → encrypts DEK (Data Encryption Key, random 32 bytes)
    → DEK encrypts gateway credentials
```

**Rotation:** DEK rotates after 1M uses or 1 hour. LRU cache of recently used DEKs.

**Required in production:** If `ENCRYPTION_KEY` is unset, server refuses to start.

**Why it matters:** If the database is compromised, gateway API keys remain encrypted. The master key never touches disk.

---

### 👁️ Eyes — Observability (OTEL + Structured Logging)

**What:** Three pillars of observability.

**Logging** (`internal/adapters/observability/logger.go`):
- Structured JSON to stdout
- 70+ event types
- Sensitive fields redacted (vpa, card_number, cvv, api_key, etc.)
- Error events require `error_code` + `trace_id` + `transaction_id`

**Metrics** (`internal/ports/metrics.go`):
- 69 metric names via OTEL
- Covers: transaction lifecycle, gateway latency, circuit breaker, rate limiting, outbox lag, fee accuracy

**Tracing** (`internal/ports/tracer.go`):
- 16 span names
- W3C traceparent propagation
- Sampled attributes: error, latency_exceeded, retry, fallback

**Why it matters:** When something goes wrong, you can trace the full journey of a request across services, gateways, and background jobs.

---

### 🗣️ Voice — Callback Dispatcher

**What:** POSTs status updates to the merchant's callback URL.

**File:** `internal/adapters/callback/dispatcher.go`

**Trigger:** `TRANSACTION_CALLBACK` outbox event.

**Payload:** `{transaction_id, status}`.

**Why it matters:** The booking engine (merchant) gets notified when payments succeed/fail without polling.

---

### 📡 Nerves — Tenant Webhook System

**What:** Per-tenant outbound webhooks (separate from the main outbox).

**File:** `internal/jobs/tenant_webhook/job.go`

**How:** Background job every 5s, POSTs to tenant-configured endpoints with HMAC-SHA256 signing.

**Headers:**
```
Content-Type: application/json
X-Webhook-Event: <event_type>
X-Delivery-ID: <delivery_id>
X-Webhook-Signature: sha256=<hmac_hex>
```

**Why it matters:** Tenants can integrate their own systems. Separate from the main outbox to avoid tenant webhook failures affecting internal event processing.

---

### 🧹 Kidneys — Partition Manager

**What:** Manages outbox table partitions (weekly).

**File:** `internal/jobs/partition_manager/manager.go`

**Lifecycle:**
1. **Pre-create** — current week + 2 weeks ahead
2. **Detach stale** — partitions older than 2 weeks (only if zero unpublished events)
3. **Drop detached** — partitions detached longer than 14 days

**Why it matters:** The outbox grows unboundedly. Partitioning keeps queries fast and enables efficient cleanup without downtime.

---

### 🏥 White Blood Cells — Reconciliation

**What:** Compares internal records against gateway settlement reports.

**When:** Every 5 minutes or on-demand via API.

**Detects:**

| Mismatch Type | Meaning |
|--------------|---------|
| `STATUS_MISMATCH` | Internal says CAPTURED but gateway says failed (or vice versa) |
| `AMOUNT_MISMATCH` | Internal amount ≠ gateway amount |
| `FEE_MISMATCH` | Internal fees ≠ gateway fees |
| `MISSING_GATEWAY` | Transaction exists internally but not in gateway report |
| `MISSING_INTERNAL` | Gateway report entry has no matching internal transaction |

**Why it matters:** Catches discrepancies between what the system thinks happened and what the gateway actually settled. The immune system that finds and flags "diseases" in the data.

---

### 🫁 Lungs — Outbox Relay + Postgres NOTIFY

**What:** The breathing rhythm of the system.

**File:** `internal/relay/relay.go`

**Hybrid mode:** Poll every 5s + Postgres `LISTEN outbox_insert` for immediate wakeup on new events.

**Shard assignment:**
- 64 shards
- Consistent-hash ring with replication factor 2
- `FOR UPDATE SKIP LOCKED` prevents double-delivery

**Dead letters:** Events that exhaust retries → `outbox_dead_letters` table → manual replay via API.

**Why it matters:** Ensures events are processed quickly (push) but reliably (poll fallback). No event is ever lost.

---

### 🩺 Pulse Monitor — Gateway Metrics Job

**What:** Snapshots circuit breaker state from Valkey → Postgres every 5 minutes.

**File:** `internal/jobs/gateway_metrics/job.go`

**Why it matters:** Makes ephemeral Valkey state persistent for dashboards, historical trends, and the `GET /api/v1/gateways` API.

---

### 🔐 Security — TLS/mTLS

**What:** TLS certificate management with optional mTLS for the API server.

**File:** `internal/adapters/security/tls.go`

**Modes:**
- OptionalMTLS: `tls.VerifyClientCertIfGiven`
- Strict: `tls.RequireAndVerifyClientCert`

**Hot reload:** Background goroutine reloads cert/key/CA from disk at configurable interval.

**Expiry warning:** Warns if cert expires within 14 days.

---

## How They Relate — The Event Flow

```
HTTP Request
    │
    ▼
[Middleware Chain] ──→ Context: request_id, trace_id, principal, tenant_id
    │
    ▼
[Handler] ──→ [App Service] ──→ [Domain]
    │                │
    │                ├──→ Audit Log (fire-and-forget record)
    │                ├──→ Event Bus (SSE to browser)
    │                └──→ Outbox Write (in same DB tx)
    │
    ▼
[Postgres NOTIFY] ──→ Relay wakeup signal
    │
    ▼
[Outbox Relay] ──→ Route by event type
    │
    ├──→ GATEWAY_INITIATE ──→ [Gateway Initiator] ──→ [Circuit Breaker check] ──→ Gateway API
    │                                                                      │
    │                                                                      ▼
    │                                                              [Processing Lease] (acquire)
    │                                                                      │
    │                                                                      ▼
    │                                                              [FinalizeGatewayResponse]
    │                                                                      │
    │                                                              [Lease release] + [Outbox Write: terminal event]
    │
    ├──→ TRANSACTION_CALLBACK ──→ [Callback Dispatcher] ──→ POST to merchant
    │
    └──→ Everything else ──→ [SNS Publisher] ──→ External consumers
                                                    │
                                                    ├──→ Tenant Webhook System
                                                    ├──→ Notification System
                                                    └──→ Reconciliation (reads from gateway directly)
```

---

## When to Use What

| Situation | Use |
|-----------|-----|
| Notify external systems of a state change | Outbox event |
| Record who did what for compliance | Audit log |
| Check if internal state matches gateway reality | Reconciliation |
| Prevent duplicate processing | Idempotency (middleware + app guard) |
| Prevent concurrent gateway calls | Processing lease |
| Protect against gateway downtime | Circuit breaker |
| Throttle abusive clients | Rate limiter |
| Encrypt secrets at rest | Envelope encryption |
| Track request across services | OTEL trace + RequestID |
| Notify merchant of payment result | Callback dispatcher |
| Allow tenant-specific integrations | Tenant webhooks |
| Keep outbox table manageable | Partition manager |
| Persist circuit breaker state for dashboards | Gateway metrics job |

---

## Outbox vs Audit Log vs Reconciliation

| Dimension | Outbox Events | Audit Log | Reconciliation |
|-----------|---------------|-----------|----------------|
| **Purpose** | Propagate state changes to external consumers | Compliance/forensic trail | Detect discrepancies with gateway |
| **Direction** | Internal → External | Internal (passive) | External → Internal (compare) |
| **Write trigger** | Every state change | Every state change + ops actions | Scheduled (5 min) or manual |
| **Atomicity** | Same DB tx as business mutation | Same DB tx, errors discarded | Standalone read+write job |
| **Consumer** | Relay → SNS / callbacks / gateway | Read-only queries | Scheduler + REST API |
| **Failure handling** | Retry → dead letter → manual replay | Silent discard | Job marked FAILED |
| **Relationship** | Drives the system forward | Records what happened | Validates correctness |

---

## Transaction States vs Outbox Events

Not every state change emits an outbox event. Here's the mapping:

| State | Outbox Event | Audit Log | Why |
|-------|-------------|-----------|-----|
| PENDING | `TRANSACTION_CREATED` | `STATE_CHANGE` | Initial creation |
| PROCESSING | `GATEWAY_INITIATE` | `STATE_CHANGE` | Gateway call initiated |
| AUTHORIZED | (none) | (none) | Intermediate — wait for capture |
| CAPTURED | `TRANSACTION_CAPTURED` | `STATE_CHANGE` | Terminal — money captured |
| SETTLED | (none) | (none) | Settlement is batch, not real-time |
| FAILED | `TRANSACTION_FAILED` | `STATE_CHANGE` | Terminal — payment failed |
| CANCELLED | `TRANSACTION_CANCELLED` | `STATE_CHANGE` | Terminal — payment cancelled |
| DISPUTED | `DISPUTE_CREATED` / `DISPUTE_UPDATED` | (via dispute service) | Dispute lifecycle |
| REFUND_PENDING | `REFUND_INITIATED` | `REFUND_INITIATED` | Refund started |
| PARTIALLY_REFUNDED | `REFUND_SUCCEEDED` | `STATE_CHANGE` | Partial refund complete |
| REFUNDED | `REFUND_SUCCEEDED` | `STATE_CHANGE` | Full refund complete |
| REFUND_FAILED | `REFUND_FAILED` | `STATE_CHANGE` | Refund failed |

**Key insight:** Terminal states (CAPTURED, FAILED, CANCELLED, REFUNDED) emit events because external systems need to know. Intermediate states (AUTHORIZED, PROCESSING) either emit events that trigger actions (GATEWAY_INITIATE) or are internal-only.
