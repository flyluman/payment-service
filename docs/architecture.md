# Payment Service Architecture

## Overview

This document provides a comprehensive architecture overview of the payment service system. The service is built using a hexagonal (ports and adapters) architecture pattern, designed for multi-tenant payment processing with support for multiple payment gateways.

### Key Design Principles

1. **Hexagonal Architecture** - Clear separation between domain logic, application services, and infrastructure adapters
2. **Multi-tenancy** - Tenant isolation at the database level with per-tenant configuration
3. **Event-driven** - Transactional outbox pattern for reliable event publishing
4. **Resilient** - Circuit breaker pattern, idempotency keys, and retry mechanisms
5. **Observable** - OpenTelemetry instrumentation for distributed tracing

## System Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                           API Layer                                 │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐  │
│  │ Payment │  │ Refund  │  │Dispute  │  │ Webhook │  │Reconcile│  │
│  │ Handler │  │ Handler │  │ Handler │  │ Handler │  │ Handler │  │
│  └────┬────┘  └────┬────┘  └────┬────┘  └────┬────┘  └────┬────┘  │
│       │            │            │            │            │         │
│  ┌────┴────────────┴────────────┴────────────┴────────────┴────┐   │
│  │                      Middleware Chain                        │   │
│  │  RequestID → TraceID → RequestLog → Recover → Auth → Rate  │   │
│  └─────────────────────────────┬───────────────────────────────┘   │
└────────────────────────────────┼────────────────────────────────────┘
                                 │
┌────────────────────────────────┼────────────────────────────────────┐
│                    Application Layer                                │
│  ┌─────────────────────────────┴───────────────────────────────┐   │
│  │                     Payment Service                         │   │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐       │   │
│  │  │ Create  │  │Authorize│  │ Capture │  │ Settle  │       │   │
│  │  │ Payment │  │ Payment │  │ Payment │  │ Payment │       │   │
│  │  └────┬────┘  └────┬────┘  └────┬────┘  └────┬────┘       │   │
│  └───────┼────────────┼────────────┼────────────┼─────────────┘   │
│          │            │            │            │                   │
│  ┌───────┴────────────┴────────────┴────────────┴─────────────┐   │
│  │                    Domain Layer                             │   │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │   │
│  │  │ Transaction  │  │    Refund    │  │   Dispute    │     │   │
│  │  │    Entity    │  │    Entity    │  │    Entity    │     │   │
│  │  └──────────────┘  └──────────────┘  └──────────────┘     │   │
│  └────────────────────────────────────────────────────────────┘   │
└────────────────────────────────────────────────────────────────────┘
                                 │
┌────────────────────────────────┼────────────────────────────────────┐
│                     Ports Layer                                     │
│  ┌─────────────────────────────┴───────────────────────────────┐   │
│  │  Gateway Port    │  Repository Port  │  Outbox Port  │ ...  │   │
│  └────────┬────────┴────────┬──────────┴────────┬───────┴─────┘   │
└───────────┼────────────────┼────────────────────┼──────────────────┘
            │                │                    │
┌───────────┼────────────────┼────────────────────┼──────────────────┐
│           │      Adapters Layer                │                    │
│  ┌────────┴────────┐  ┌────┴────┐  ┌───────────┴──────────┐       │
│  │   Gateways      │  │  Postgres│  │   Outbox Relay      │       │
│  │ ┌─────┐┌─────┐  │  │Adapter  │  │     Adapter         │       │
│  │ │Stripe││Razorpay│ │         │  │                     │       │
│  │ └─────┘└─────┘  │  │         │  │                     │       │
│  └─────────────────┘  └─────────┘  └─────────────────────┘       │
└────────────────────────────────────────────────────────────────────┘
```

## Domain Model

### Transaction Entity

The core domain entity representing a payment transaction. Located in `internal/domain/transaction/`.

#### State Machine

The transaction follows a 12-state lifecycle:

```
                    ┌─────────────────────┐
                    │      PENDING        │
                    └──────────┬──────────┘
                               │
                    ┌──────────▼──────────┐
                    │    PROCESSING       │
                    └──────────┬──────────┘
                               │
              ┌────────────────┼────────────────┐
              │                │                │
    ┌─────────▼─────────┐ ┌───▼────┐ ┌────────▼────────┐
    │    AUTHORIZED     │ │ FAILED │ │   CANCELLED     │
    └─────────┬─────────┘ └────────┘ └─────────────────┘
              │
    ┌─────────▼─────────┐
    │     CAPTURED      │
    └─────────┬─────────┘
              │
    ┌─────────▼─────────┐
    │      SETTLED      │
    └─────────┬─────────┘
              │
    ┌─────────▼─────────┐
    │   DISPUTED        │
    └─────────┬─────────┘
              │
    ┌─────────▼─────────┐
    │   REFUND_PENDING  │
    └─────────┬─────────┘
              │
    ┌─────────▼─────────┐
    │PARTIALLY_REFUNDED │
    └─────────┬─────────┘
              │
    ┌─────────▼─────────┐
    │     REFUNDED      │
    └─────────┬─────────┘
              │
    ┌─────────▼─────────┐
    │  REFUND_FAILED    │
    └───────────────────┘
```

#### State Transitions

| From State | To State | Trigger | Description |
|------------|----------|---------|-------------|
| PENDING | PROCESSING | Gateway call initiated | Payment sent to gateway |
| PROCESSING | AUTHORIZED | Gateway success | Payment authorized but not captured |
| PROCESSING | FAILED | Gateway failure | Payment failed at gateway |
| AUTHORIZED | CAPTURED | Manual capture | Funds captured from authorized payment |
| CAPTURED | SETTLED | Settlement | Funds settled to merchant |
| CAPTURED | REFUND_PENDING | Refund initiated | Refund requested |
| CAPTURED | DISPUTED | Dispute created | Customer initiated dispute |
| SETTLED | DISPUTED | Disputes after settlement | Late dispute filing |
| REFUND_PENDING | PARTIALLY_REFUNDED | Partial refund | Refund < transaction amount |
| REFUND_PENDING | REFUNDED | Full refund | Refund = transaction amount |
| REFUND_PENDING | REFUND_FAILED | Refund failure | Gateway refund failure |
| DISPUTED | CAPTURED | Dispute won | Dispute resolved in merchant favor |
| DISPUTED | REFUNDED | Dispute lost | Dispute resolved in customer favor |

#### Capture Modes

- **auto** (default): Gateway initiates capture during authorization
- **manual**: Gateway authorizes only; separate `/capture` call required

### Refund Entity

Manages refund transactions linked to a parent payment. Located in `internal/domain/refund/`.

#### Properties
- `ID` - Unique refund identifier
- `TransactionID` - Parent payment reference
- `Amount` - Refund amount (must be ≤ parent amount)
- `Status` - Current refund status
- `Reason` - Refund reason

#### Business Rules
1. Refunds only allowed for CAPTURED or SETTLED transactions
2. Partial refunds allowed if refund total < parent amount
3. Full refund transitions parent to REFUNDED state
4. Partial refunds transition parent to PARTIALLY_REFUNDED state

### Dispute Entity

Manages chargeback disputes linked to transactions. Located in `internal/domain/dispute/`.

#### Properties
- `ID` - Unique dispute identifier
- `TransactionID` - Payment reference
- `Status` - DISPUTE_OPEN, DISPUTE_WON, DISPUTE_LOST
- `Reason` - Dispute reason code
- `Evidence` - Supporting documentation

#### Business Rules
1. Disputes can be filed against CAPTURED or SETTLED transactions
2. Creating dispute transitions transaction to DISPUTED state
3. Won disputes transition transaction back to CAPTURED
4. Lost disputes transition transaction to REFUNDED state

### Fee Entity

Calculates fees using a reverse-inclusive formula. Located in `internal/domain/fees/fees.go`.

#### Fee Calculation Formula
```
serviceFee = amount / (1 - ratio/100) - amount
```

This ensures the service fee is calculated from the total amount, not added on top.

#### Exchange Rate Markup
```
effectiveRate = baseRate + (baseRate/100 * markupPct) + markupFixed
```

## Application Layer

### Payment Service

Orchestrates payment operations using domain services. Located in `internal/app/payment/`.

#### Methods

- `CreatePayment` - Initialize transaction (auto or manual capture)
- `AuthorizePayment` - Manual authorization (skips gateway)
- `CapturePayment` - Capture authorized payment via gateway
- `SettlePayment` - Transition to settled state
- `ProcessPayment` - Complete gateway authorization flow

#### Key Behaviors
1. For `auto` mode: Create immediately calls gateway
2. For `manual` mode: Create stores transaction, authorize captures
3. Idempotency keys prevent duplicate processing
4. Processing lease prevents concurrent gateway calls

### Refund Service

Handles refund initiation and processing. Located in `internal/app/refund/`.

#### Methods
- `InitiateRefund` - Start refund process (transitions to REFUND_PENDING)
- `ProcessRefund` - Execute refund via gateway

#### Partial Refund Logic
```go
if refund.Amount < parent.Amount {
    // PARTIALLY_REFUNDED
} else {
    // REFUNDED
}
```

### Dispute Service

Manages dispute lifecycle. Located in `internal/app/dispute/`.

#### Methods
- `CreateDispute` - File new dispute (transitions to DISPUTED)
- `UpdateDispute` - Process dispute outcome

#### Dispute Resolution Logic
```go
switch dispute.Status {
case DISPUTE_WON:
    // Transaction → CAPTURED
case DISPUTE_LOST:
    // Transaction → REFUNDED
}
```

### Webhook Service

Processes inbound webhooks from payment gateways. Located in `internal/app/webhook/`.

#### Processing Flow
1. Validate webhook signature
2. Parse transaction status
3. Update transaction state via outbox
4. Emit domain events

#### Status Mapping
```go
mapWebhookStatus(gatewayStatus) TransactionStatus {
    "authorized" → AUTHORIZED
    "captured" → CAPTURED
    "failed" → FAILED
    // etc.
}
```

## Background Services

### Notifications

Dispatches email notifications at terminal state transitions. Wired on payment, webhook, refund, and dispute services.

#### Dispatch Pattern
- Enqueue-only (non-blocking) via `NotificationDispatcher` port
- 2-second `ProcessQueue` poller drains the notification queue
- Transactional insert (`Insert` is tx-aware) ensures notifications are co-committed with state changes

#### Template Types
| Template | Trigger |
|----------|---------|
| `PAYMENT_SUCCESS` | Payment captured/settled |
| `PAYMENT_FAILURE` | Payment failed |
| `REFUND_COMPLETED` | Refund succeeded |
| `REFUND_FAILED` | Refund failed at gateway |
| `DISPUTE_OPENED` | Dispute created |
| `DISPUTE_WON` | Dispute resolved in merchant favor |
| `DISPUTE_LOST` | Dispute resolved in customer favor |

#### Template Data
Uses Go `html/template` syntax: `{{.amount}}`, `{{.currency}}`, `{{.transaction_id}}`, etc.

### Tenant Webhooks

Dispatches webhooks to tenant-registered URLs for real-time event notification.

#### Dispatcher Pattern
1. **Config Resolution**: Dispatcher looks up tenant's registered webhook URLs from `tenant_webhook_configs` table
2. **Delivery Writing**: Writes webhook delivery records to the database
3. **Worker Processing**: Background worker processes the delivery queue

#### Worker Configuration
- `MaxAttempts`: Maximum retry attempts per delivery (from tenant config)
- `MaxBackoffSec`: Maximum backoff between retries (from tenant config)
- Exponential backoff with jitter

#### Config Management API
```
POST /api/v1/tenant-webhooks/{tenant_id}  — Upsert webhook config
GET  /api/v1/tenant-webhooks              — List all webhook configs
```

### Audit Log

Records immutable audit entries for every state transition.

#### Wiring
- `SetAuditLogStore` wired on payment, webhook, refund, dispute, and cancel services
- Uses outbox event `AUDIT_STATE_CHANGE` for reliable delivery
- Each entry captures: entity type, entity ID, old state, new state, timestamp, actor

### Async Refunds

Refund processing is non-blocking for improved API responsiveness.

#### Flow
1. Refund handler returns `202 Accepted` immediately
2. Outbox event `REFUND_INITIATED` is emitted
3. Relay consumer picks up the event and executes the gateway refund call
4. Gateway response written back via `ProcessRefund`, updating refund status and transaction state

### ResponseCache

Valkey-backed idempotency response cache installed in the middleware chain.

#### Features
- 5-minute TTL on cached responses
- Applies to GET endpoints only
- Prevents duplicate responses for identical requests
- Keyed by request path + auth context

### SSE (Server-Sent Events)

Real-time transaction update streaming for connected clients.

#### Wiring
- `SetEventBus` wired on all terminal services (payment, webhook, refund, dispute)
- `GET /api/v1/payments/{id}/events` — SSE stream endpoint
- Auth inherits from the parent payment endpoint (service token or checkout token)

### Dispute Ingestion

Parses incoming dispute webhooks from gateways and routes them to the dispute service.

#### Stripe Integration
- Parses `charge.dispute.*` webhooks in the webhook handler
- Supports `charge.dispute.created`, `charge.dispute.closed` (won/lost)
- Maps Stripe dispute reason codes to internal dispute reasons
- Routes parsed disputes to dispute service via `IngestGatewayDispute`

## Ports Layer

### Gateway Port

Defines interface for payment gateway communication. Located in `internal/ports/gateway.go`.

#### Interface Methods
```go
type GatewayAdapter interface {
    CreatePayment(ctx, req) (resp, error)
    CapturePayment(ctx, req) (resp, error)
    InitiateRefund(ctx, req) (resp, error)
    GetPaymentStatus(ctx, id) (status, error)
    SupportsManualCapture() bool
}
```

#### Request/Response Types
- `GatewayPaymentRequest` - Payment creation details
- `GatewayPaymentResponse` - Creation result with gateway ID
- `GatewayCaptureRequest` - Capture amount/details
- `GatewayCaptureResponse` - Capture confirmation
- `GatewayRefundRequest` - Refund details
- `GatewayRefundResponse` - Refund confirmation

### Repository Port

Data persistence interface. Located in `internal/ports/repository.go`.

#### Transaction Repository
```go
type TransactionRepository interface {
    Create(ctx, transaction) error
    GetByID(ctx, id) (*Transaction, error)
    UpdateStatus(ctx, id, newStatus) error
    Update(ctx, transaction) error
    GetByIDempotencyKey(ctx, key) (*Transaction, error)
    GetByGatewayID(ctx, gatewayID) (*Transaction, error)
    List(ctx, filter) ([]*Transaction, error)
    UpdateProcessingLease(ctx, id, expiresAt) error
    GetExpiredProcessingLease(ctx, expiresAt) ([]*Transaction, error)
}
```

### NotificationDispatcher Port

Dispatches notifications (email) at terminal state transitions. Located in `internal/ports/`.

#### Interface
```go
type NotificationDispatcher interface {
    Dispatch(ctx, notification) error
}
```

- Enqueue-only (non-blocking) dispatch
- 7 template types: `PAYMENT_SUCCESS`, `PAYMENT_FAILURE`, `REFUND_COMPLETED`, `REFUND_FAILED`, `DISPUTE_OPENED`, `DISPUTE_WON`, `DISPUTE_LOST`
- Template data uses Go `html/template` syntax (`{{.amount}}`, `{{.currency}}`, etc.)
- Dispatch fires at terminal state transitions only (EMAIL channel)

### TenantWebhookDispatcher Port

Dispatches webhooks to tenant-registered URLs. Located in `internal/ports/`.

#### Interface
```go
type TenantWebhookDispatcher interface {
    Dispatch(ctx, event) error
}
```

- Config resolution: looks up tenant's registered webhook URLs from `tenant_webhook_configs` table
- Delivery: writes webhook delivery records; background worker processes the queue
- Worker honors `MaxAttempts` and `MaxBackoffSec` from tenant config for retry/backoff

### TenantWebhookConfigWriter Port

Manages tenant webhook configuration. Located in `internal/ports/`.

#### Interface
```go
type TenantWebhookConfigWriter interface {
    Upsert(ctx, config) error
    List(ctx) ([]*TenantWebhookConfig, error)
}
```

### Outbox Port

Event publishing interface. Located in `internal/ports/outbox.go`.

#### Event Types
```go
const (
    EventTypeTransactionCreated   = "TRANSACTION_CREATED"
    EventTypeTransactionCaptured  = "TRANSACTION_CAPTURED"
    EventTypeTransactionFailed    = "TRANSACTION_FAILED"
    EventTypeTransactionCancelled = "TRANSACTION_CANCELLED"

    EventTypeRefundInitiated = "REFUND_INITIATED"
    EventTypeRefundSucceeded = "REFUND_SUCCEEDED"
    EventTypeRefundFailed    = "REFUND_FAILED"

    EventTypeAuditStateChange = "AUDIT_STATE_CHANGE"

    EventTypeTransactionCallback = "TRANSACTION_CALLBACK"

    EventTypeGatewayInitiate = "GATEWAY_INITIATE"

    EventTypeDisputeCreated = "DISPUTE_CREATED"
    EventTypeDisputeUpdated = "DISPUTE_UPDATED"
)
```

#### Event Emission Points

| Event | Emitted From | Trigger |
|-------|-------------|---------|
| `TRANSACTION_CREATED` | `app/payment/service.go` | New payment initiated |
| `TRANSACTION_CAPTURED` | `app/payment/gateway.go`, `app/webhook/service.go` | Gateway returns captured status |
| `TRANSACTION_FAILED` | `app/payment/gateway.go`, `app/webhook/service.go` | Gateway returns failed status |
| `TRANSACTION_CANCELLED` | `app/payment/gateway.go`, `app/webhook/service.go` | Gateway returns cancelled status |
| `REFUND_INITIATED` | `app/refund/service.go` | Refund requested |
| `REFUND_SUCCEEDED` | `app/refund/process.go` | Refund completed successfully |
| `REFUND_FAILED` | `app/refund/process.go` | Refund failed at gateway |
| `AUDIT_STATE_CHANGE` | Multiple services | Any state transition |
| `TRANSACTION_CALLBACK` | `app/payment/gateway.go`, `app/webhook/service.go` | Callback URL configured |
| `GATEWAY_INITIATE` | `app/payment/service.go` | Payment sent to gateway |
| `DISPUTE_CREATED` | `app/dispute/service.go` | New dispute filed |
| `DISPUTE_UPDATED` | `app/dispute/service.go` | Dispute resolved (won/lost) |

## Adapters Layer

### Gateway Adapters

Implementations for specific payment gateways. Located in `internal/adapters/gateways/`.

#### Stripe Adapter
- Supports auto and manual capture
- Webhook signature verification
- Partial refund support

#### Razorpay Adapter
- Auto capture mode only
- Webhook verification
- Refund support

#### FIB Adapter
- Auto capture mode only
- QR code generation
- Limited refund support

#### Common Patterns
All adapters implement `GatewayAdapter` interface and use `ResolveConfig` closure for per-tenant credentials from `tenant_gateway_configs` table.

### Postgres Adapter

Database persistence implementation. Located in `internal/adapters/postgres/`.

#### Transaction Repository Implementation
- Uses `pgx` for database access
- Transaction isolation for concurrent operations
- `capture_mode` persisted in transactions table

#### Schema Design
- `transactions` table with 12-state status constraint
- `tenant_gateway_configs` for per-tenant gateway credentials
- `transaction_events` for audit trail

### Outbox Adapter

Reliable event publishing. Located in `internal/adapters/outbox/`.

#### Transactional Outbox Pattern
1. Domain events written to outbox table in same transaction as state change
2. Polling relay publishes events to message broker
3. Guarantees at-least-once delivery
4. 64 shards for parallel processing

## Infrastructure Components

### Circuit Breaker

Prevents cascade failures when gateways are unavailable. Located in `internal/adapters/circuitbreaker/`.

#### States
- **Closed**: Normal operation, requests pass through
- **Open**: Gateway failures exceed threshold, requests fail fast
- **Half-Open**: Limited requests to test gateway recovery

#### Implementation
- Valkey-backed state storage
- Configurable failure threshold
- Automatic reset after timeout

### Processing Lease

Prevents concurrent gateway calls for same transaction. Located in `internal/domain/transaction/`.

#### Mechanism
1. Acquire lease before gateway call (UPDATE ... WHERE lease_expires_at < now())
2. Lease expires after configurable timeout
3. Reaper job finds expired leases and marks transactions as FAILED

### Idempotency

Prevents duplicate payment processing. Located in `internal/domain/transaction/`.

#### Implementation
- Client-provided `IdempotencyKey` in request
- Stored on transaction record
- Checked before processing to prevent duplicates
- Returns existing transaction if key already used

### Rate Limiting

Token-bucket rate limiting using Lua scripts in Valkey. Located in `internal/adapters/valkey/`.

#### Features
- Per-tenant rate limits
- Configurable burst capacity
- Sliding window counters

## Background Jobs

Six background jobs run in separate goroutines:

### 1. Outbox Relay
Polls outbox table and publishes events to message broker.

### 2. Lease Reaper
Finds transactions with expired processing leases and marks them as FAILED.

### 3. Reconciliation Scheduler
Polls PENDING reconciliation jobs and runs them.

### 4. Dispute Poller
Periodically checks for dispute status updates from gateways.

### 5. Settlement Fetcher
Retrieves settlement reports from gateways for reconciliation.

### 6. Circuit Breaker Monitor
Checks gateway health and updates circuit breaker states.

## API Reference

### Payment Endpoints

#### Create Payment
```
POST /api/v1/payments
{
    "amount": 1000,
    "currency": "INR",
    "capture_mode": "manual", // or "auto" (default)
    "idempotency_key": "unique-key",
    "tenant_id": "tenant-123",
    "gateway_id": "stripe-1",
    "method": "card",
    "vpa": null,
    "upi_app": null,
    "purpose": "payment"
}
```

#### Authorize Payment
```
POST /api/v1/payments/{id}/authorize
{
    // Optional additional data
}
```

#### Capture Payment
```
POST /api/v1/payments/{id}/capture
{
    "amount": 500 // Partial capture amount
}
```

#### Settle Payment
```
POST /api/v1/payments/{id}/settle
{
    // Settlement confirmation
}
```

### Refund Endpoints

#### Create Refund
```
POST /api/v1/refunds
{
    "transaction_id": "txn-123",
    "amount": 500,
    "reason": "customer_request"
}
```

### Dispute Endpoints

#### Create Dispute
```
POST /api/v1/disputes
{
    "transaction_id": "txn-123",
    "reason": "unauthorized"
}
```

#### Update Dispute
```
PATCH /api/v1/disputes/{id}
{
    "status": "DISPUTE_WON",
    "evidence": "..."
}
```

### Webhook Endpoint
```
POST /webhooks/gateway/{gateway_id}
// Gateway-specific payload
```

### Reconciliation Endpoints
```
POST /api/v1/reconciliation/jobs
GET /api/v1/reconciliation/jobs/{id}
```

## Middleware Chain

Request processing pipeline (outermost to innermost):

1. **RequestID** - Generates unique request identifier
2. **TraceID** - Adds distributed tracing context
3. **RequestLog** - Logs request/response details
4. **Recover** - Catches panics and returns 500
5. **Auth** - Validates service/ops tokens
6. **RateLimit** - Applies rate limiting rules
7. **ResponseCache** - Valkey-backed idempotency response cache (5-min TTL, GET endpoints only)

## Security

### Authentication
- Service tokens for internal API calls
- Ops tokens for operations endpoints
- Checkout tokens for browser-facing flows

### Encryption
- mTLS for gateway communication
- Envelope encryption for gateway credentials (using `ENCRYPTION_KEY`)
- Credentials stored encrypted in `tenant_gateway_configs` table

### Authorization
- Tenant isolation via database schema
- Role-based access control

## Configuration

YAML-based configuration with environment variable overrides:

```yaml
server:
  port: 8080
  read_timeout: 30s
  write_timeout: 30s

database:
  host: localhost
  port: 5432
  name: payments
  user: postgres
  password: secret

redis:
  host: localhost
  port: 6379

gateways:
  stripe:
    api_key: sk_test_...
  razorpay:
    key_id: rzp_test_...
    key_secret: ...
  fib:
    api_key: ...

reconciliation:
  interval_seconds: 300
```

### Environment Variables
- `CONFIG_PATH` - Path to config file (broken, use config.yaml in working directory)
- `ENCRYPTION_KEY` - Hex-encoded 256-bit key for credential encryption
- `SERVICE_TOKENS` - Comma-separated service tokens
- `OPS_TOKENS` - Comma-separated ops tokens

## Observability

### OpenTelemetry Integration
- Distributed tracing across services
- Span attributes for transaction IDs, gateway IDs
- Metrics for latency, error rates, throughput

### Structured Logging
- JSON log format
- Request context propagation
- Error stack traces

### Metrics
- Transaction success/failure rates
- Gateway latency histograms
- Circuit breaker state changes

## Deployment

### Single Binary
Single binary builds from `cmd/server/main.go`:
```bash
go build -o payment-service ./cmd/server
```

### Process Model
One process spawns 3 goroutines:
1. HTTP API server
2. Outbox relay
3. Background jobs

### Database Migrations
Located in `migrations/` directory:
- `000001_initial_schema.up.sql` - Core tables
- `000002_add_capture_mode.up.sql` - Capture mode support

Apply with:
```bash
db-migrate up
```

## Testing

### Unit Tests
```bash
go test ./...
```

### Integration Tests
```bash
docker compose up -d postgres valkey
go test -tags=integration ./...
```

Integration tests auto-skip if infrastructure unavailable; each process gets isolated PG schema `test_<PID>`.

### Test Database
Override via environment:
```bash
PAYMENT_TEST_DB_HOST=localhost
PAYMENT_TEST_DB_PORT=5432
PAYMENT_TEST_DB_NAME=payments_test
PAYMENT_TEST_DB_USER=postgres
PAYMENT_TEST_DB_PASSWORD=secret
PAYMENT_TEST_DB_SSLMODE=disable
```

## Build and Run

### Development
```bash
go run ./cmd/server
```

### Production
```bash
go build -o payment-service ./cmd/server
./payment-service
```

## Conclusion

The payment service implements a robust, scalable payment processing system with:
- 12-state transaction lifecycle
- Multi-gateway support (Stripe, Razorpay, FIB)
- Event-driven architecture with reliable event delivery
- Comprehensive observability and security
- Extensible design for new payment methods and gateways