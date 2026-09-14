# Reform Plan — Payment Service

## Scope (IN)
1. Reconciliation (full)
2. Disputes / Chargebacks (full management)
3. Tenant Webhook Delivery (full delivery worker)
4. Gateway Fee Calculation (wire into payment flow)
5. Audit Log (full — state transitions, config changes, ops actions)
6. Transaction Search API (multi-field filters, cursor pagination, CSV export)
7. Dead Letter Management API (list, filter, replay)
8. Gateway Metrics Persistence (background job → Postgres)
9. Analytics / Reporting (basic — volume, success rates, gateway performance, settlement summaries)
10. Unit Tests (all uncovered handlers and adapters)
11. 3DS / Fraud Engine (3DS webhook handling + fraud rules engine)
12. Notification System (full — email/SMS, templates, background delivery)

## Scope (OUT)
Payouts, Automatic Gateway Routing, Subscriptions, Save Card / Vault, Customer Management, Invoicing

---

## Phase 1 — Foundation

### 1.1 Audit Log Service

**Implemented.**

**New files:**
- `internal/ports/audit.go` — `AuditLogStore` interface with methods:
  - `WriteEntry(ctx, *AuditEntry) error`
  - `ListByTransaction(ctx, transactionID, cursor, limit) ([]AuditEntry, error)`
- `internal/domain/audit/audit.go` — `Entry` struct with fields: `ID`, `TransactionID`, `EventType`, `Actor`, `PreviousState`, `NewState`, `Metadata`, `CreatedAt`. Event type constants: `StateChange`, `ConfigUpdate`, `OpsAction`, `WebhookReceived`, `RefundInitiated`
- `internal/adapters/postgres/audit.go` — `AuditLogStore` impl that writes to `audit_log` table
- `internal/ports/logger.go` — add `LogEventAuditWrite = "audit.write"` constant

**Modify:**
- `internal/app/payment/service.go` — inject `AuditLogStore`, call `WriteEntry` at every state transition (`Create`, `ProcessGatewayInitiate`, `finalize`)
- `internal/app/payment/gateway.go` — call `WriteEntry` after `finalize` sets new status
- `internal/app/refund/service.go` — call `WriteEntry` on refund state changes
- `internal/app/refund/process.go` — call `WriteEntry` on refund terminal transitions
- `internal/app/webhook/service.go` — call `WriteEntry` when webhook changes transaction state
- `internal/app/cancel/service.go` — call `WriteEntry` on cancel operations
- `internal/api/handlers/tenant_gateway.go` — call `WriteEntry` on config create/update/delete
- `internal/api/handlers/dead_letter.go` (will be created in Phase 5) — call `WriteEntry` on replay
- `internal/domain/transaction/state_machine.go` — add `TransitionEvent` type that includes `Actor` and `Reason` so audit can capture who/what caused transition

**Migration:** `migrations/000002_audit_log_extend.sql`
- Add columns to `audit_log`: `actor_type TEXT CHECK (actor_type IN ('system', 'tenant', 'ops', 'gateway'))`, `reason TEXT`
- Add index `idx_audit_log_event_type` on `(event_type, created_at DESC)`
- Fill `actor_type` from existing `actor` column

### 1.2 Gateway Fee Wiring

**Implemented.**

**Note:** The `gateway_fee_models` table and `ports.GatewayFeeModel.CalculateFee()` already exist. They're just never called.

The fee system was redesigned as part of the reconciliation work (commit `55ab19c`):

**Fee system redesign:**
- `internal/domain/fees/fees.go` — complete rewrite with nested 2-part breakdown:
  - `Breakdown{Summary, FeeDetail{Exchange}, Gateway}` — structured JSON output
  - `Calculate()` — two-layer formula: Layer 1 (exchange rate with markup), Layer 2 (reverse-inclusive service fee + fixed charge)
  - `Validate()` — consistency checks on breakdown
- `internal/api/handlers/gateway.go` — `FeeEstimator` interface for per-gateway fee estimation endpoint
- `internal/app/payment/service.go` — `fees.Calculate()` called in `ProcessGatewayInitiate()`, sets `txn.GatewayAmount`/`txn.GatewayCurrency`

**gateway_amount/gateway_currency columns:**
- Added `gateway_amount BIGINT` and `gateway_currency CHAR(3)` as first-class columns on `transactions` table (not just JSONB)
- Domain model: `transaction.Txn` has `GatewayAmount *int64` and `GatewayCurrency string`
- API responses: `paymentResponse` includes `gateway_amount` and `gateway_currency`
- Transaction filter: `MinGatewayAmount`, `MaxGatewayAmount`, `GatewayCurrency` added to `TransactionFilter`

**Modify:**
- `internal/domain/transaction/transaction.go` — add fields: `gateway_fee_estimate BIGINT`, `gateway_fee_currency CHAR(3)`, `gateway_fee_model_version INT`. Add accessor methods.
- `internal/app/payment/service.go` — in `Create` method: after `SelectGateway`, call `configStore.GetFeeModel(gatewayID, paymentMethod)`, call `feeModel.CalculateFee(amount)`, set `gatewayFeeEstimate` and `gatewayFeeCurrency` on transaction before insert.
- `internal/ports/config.go` — `ConfigStore` interface already has `GetFeeModel` — verify it's wired. Add `GetFeeModel(ctx, gatewayID, paymentMethod string) (*GatewayFeeModel, error)` method definition.
- `internal/adapters/postgres/config_store.go` — `GetFeeModel` already queries `gateway_fee_models` table. Verify it handles missing rows (return nil, nil).

**Migration:** `migrations/000003_add_txn_fee_fields.sql`
```sql
ALTER TABLE transactions ADD COLUMN gateway_fee_estimate BIGINT;
ALTER TABLE transactions ADD COLUMN gateway_fee_currency CHAR(3);
ALTER TABLE transactions ADD COLUMN gateway_fee_model_version INT;
```

### 1.3 Unit Tests

**Implemented.**

**New test files:**
- `internal/api/handlers/pay_test.go` — test `ServeCheckout`, `Success`, `Failure` with valid/invalid tokens, expired tokens, missing transactions
- `internal/api/handlers/sse_test.go` — test `Events` handler: SSE connection, event streaming, context cancellation, missing/invalid transaction IDs
- `internal/api/handlers/gateway_test.go` — test `List` with various circuit-breaker states, empty results, error cases
- `internal/adapters/callback/dispatcher_test.go` — test HTTP POST with 2xx, 4xx, 5xx, timeout, network error
- `internal/adapters/initiator/handler_test.go` — test handler calls `ProcessGatewayInitiate`, error propagation
- `internal/adapters/broadcast/bus_test.go` — test publish/subscribe/unsubscribe, multiple subscribers, buffered channel behavior, unsubscribe cleanup
- `internal/adapters/outbox/route_test.go` — test wildcard matching, specific event type matching, handler error propagation, multiple matching routes
- `internal/api/middleware/requestlog_test.go` — test request ID generation, log output format, status code recording
- `internal/bootstrap/bootstrap_test.go` — test gateway registry initialization, duplicate registration error, missing provider handler

---

## Phase 2 — Data Pipelines

### 2.1 Gateway Metrics Persistence

**Implemented.**

**New files:**
- `internal/jobs/gateway_metrics/job.go` — `Job` struct with `RunOnce(ctx)`:
  1. Read circuit breaker state from Valkey (`cbStore.ListStates`)
  2. Read Valkey latency/error rate data (rolling counters)
  3. UPSERT into `gateway_metrics` table (discrepancy rates, p99 latency, volume count, fx efficiency)
  4. UPSERT into `gateway_circuit_breaker_state` table (state, cooldown_until, consecutive_failures, last_known_reliability_score)
  5. Emit metrics via `MetricRecorder`
- `internal/ports/gateway_metrics.go` — `GatewayMetricsStore` interface for reading/writing metrics:
  - `UpsertMetrics(ctx, gatewayID, *GatewayMetricsSnapshot) error`
  - `UpsertCircuitBreakerState(ctx, gatewayID, *CircuitBreakerStateRecord) error`
  - `ListMetrics(ctx) ([]GatewayMetricsSnapshot, error)`
  - `GetMetrics(ctx, gatewayID) (*GatewayMetricsSnapshot, error)`

**Modify:**
- `cmd/server/jobs.go` — add `gateway_metrics` ticker at configurable interval (default 5min), run at startup
- `config/config.go` — add `Jobs.GatewayMetricsIntervalSec` (env `GATEWAY_METRICS_INTERVAL_SECONDS`, default 300)
- `cmd/server/main.go` — wire `GatewayMetricsStore` into `deps` and pass to `startJobs`
- `internal/ports/metrics.go` — add metric constants for gateway metrics persistence job (use existing `MetricGatewayDiscrepancyRate` etc.)

### 2.2 Dispute Management

**Implemented.**

**New files:**
- `internal/domain/dispute/dispute.go` — `Dispute` struct with fields: `ID`, `TransactionID`, `GatewayID`, `GatewayDisputeID`, `Reason`, `Status` (enum: `NEEDS_RESPONSE`, `UNDER_REVIEW`, `WON`, `LOST`, `ACCEPTED`, `EXPIRED`), `Amount`, `Currency`, `EvidenceDueBy`, `EvidenceSubmittedAt`, `CreatedAt`, `ResolvedAt`. Methods: `IsUrgent()`, `DaysUntilDue()`, `CanSubmitEvidence()`, `Transition()`
- `internal/domain/dispute/evidence.go` — `Evidence` struct with fields: `ID`, `DisputeID`, `Type` (enum: `RECEIPT`, `INVOICE`, `CUSTOMER_COMMS`, `TERMS_OF_SERVICE`, `REFUND_POLICY`, `CANCELLATION_POLICY`, `OTHER`), `FileURL`, `SubmittedAt`, `Notes`
- `internal/ports/dispute.go` — `DisputeStore` interface (CRUD), `DisputeWebhookHandler` interface
- `internal/app/dispute/service.go` — `Service` struct with methods:
  - `IngestWebhook(ctx, gatewayID, rawPayload) (*Dispute, error)` — parse, dedup, store
  - `GetDispute(ctx, id) (*Dispute, error)`
  - `ListDisputes(ctx, filters) ([]Dispute, error)`
  - `SubmitEvidence(ctx, disputeID, evidence) error`
  - `AcknowledgeDispute(ctx, disputeID) error`
- `internal/adapters/postgres/dispute.go` — `DisputeStore` impl
- `internal/api/handlers/dispute.go` — `DisputeHandler` with methods:
  - `List` — `GET /api/v1/disputes` (filter by status, gateway, date range, cursor pagination)
  - `Get` — `GET /api/v1/disputes/{id}`
  - `SubmitEvidence` — `POST /api/v1/disputes/{id}/evidence`
  - `ListEvidence` — `GET /api/v1/disputes/{id}/evidence`

**Modify:**
- `internal/adapters/gateways/stripe/webhook.go` — add handling for `charge.dispute.created`, `charge.dispute.updated`, `charge.dispute.closed` events → call `disputeSvc.IngestWebhook`
- `internal/adapters/gateways/razorpay/webhook.go` — add handling for `dispute.created`, `dispute.updated` events → call `disputeSvc.IngestWebhook`
- `internal/api/router.go` — add routes:
  - `GET /api/v1/disputes` → `deps.Dispute.List`
  - `GET /api/v1/disputes/{id}` → `deps.Dispute.Get`
  - `POST /api/v1/disputes/{id}/evidence` → `deps.Dispute.SubmitEvidence`
  - `GET /api/v1/disputes/{id}/evidence` → `deps.Dispute.ListEvidence`
- `internal/api/router.go` — add `Dispute *handlers.DisputeHandler` to `Deps` struct
- `cmd/server/main.go` — wire `DisputeStore`, `DisputeService`, `DisputeHandler` into `deps`
- `config/config.go` — add `Dispute.EvidenceDueWarningDays` (env `DISPUTE_EVIDENCE_DUE_WARNING_DAYS`, default 7)

**Migration:** `migrations/000004_disputes.sql`
```sql
CREATE TABLE disputes (
    id                  UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    transaction_id      UUID        NOT NULL REFERENCES transactions(id),
    gateway_id          TEXT        NOT NULL,
    gateway_dispute_id  TEXT        NOT NULL,
    reason              TEXT        NOT NULL,
    status              TEXT        NOT NULL DEFAULT 'NEEDS_RESPONSE'
                        CHECK (status IN ('NEEDS_RESPONSE', 'UNDER_REVIEW', 'WON', 'LOST', 'ACCEPTED', 'EXPIRED')),
    amount              BIGINT      NOT NULL,
    currency            CHAR(3)     NOT NULL,
    evidence_due_by     TIMESTAMPTZ,
    evidence_submitted_at TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at         TIMESTAMPTZ,
    UNIQUE (gateway_id, gateway_dispute_id)
);

CREATE TABLE dispute_evidence (
    id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    dispute_id  UUID        NOT NULL REFERENCES disputes(id),
    evidence_type TEXT      NOT NULL CHECK (evidence_type IN (
                    'RECEIPT', 'INVOICE', 'CUSTOMER_COMMS', 'TERMS_OF_SERVICE',
                    'REFUND_POLICY', 'CANCELLATION_POLICY', 'OTHER'
                )),
    file_url    TEXT        NOT NULL,
    notes       TEXT,
    submitted_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_disputes_transaction ON disputes (transaction_id);
CREATE INDEX idx_disputes_gateway ON disputes (gateway_id, status, created_at DESC);
CREATE INDEX idx_disputes_urgent ON disputes (evidence_due_by, status)
    WHERE status IN ('NEEDS_RESPONSE', 'UNDER_REVIEW') AND evidence_due_by IS NOT NULL;
CREATE INDEX idx_dispute_evidence_dispute ON dispute_evidence (dispute_id, submitted_at ASC);
```

### 2.3 3DS Webhook Handling

**Implemented.**

**Modify:**
- `internal/adapters/gateways/stripe/webhook.go`:
  - Add handling for `payment_intent.requires_action` event type
  - Parse `next_action.type = 'redirect_to_url'` or `'use_stripe_sdk'`
  - Update transaction status to `PROCESSING` (3DS auth in progress)
  - Emit `TRANSACTION_CALLBACK` event with `requires_3ds: true` flag so frontend can trigger authentication
- `internal/adapters/gateways/razorpay/webhook.go`:
  - Add handling for `payment.pending` where `method = 'card'` and `card.three_d_secure = 'required'`
  - Similar status update logic

**Note:** The actual 3DS challenge happens on the frontend (Stripe.js / Razorpay checkout). The backend just needs to:
1. Detect that 3DS is required from the gateway response in `InitiatePayment`
2. Return the appropriate `next_action` in the `gateway_metadata`
3. Handle the webhook callback once auth completes (stripe: `payment_intent.succeeded`, razorpay: `payment.captured`)

**Modify also:**
- `internal/adapters/gateways/stripe/stripe.go` — in `InitiatePayment`: parse `PaymentIntent.NextAction` from Stripe response, include `next_action` type and URL in `GatewayPaymentResponse.Metadata`
- `internal/adapters/gateways/razorpay/razorpay.go` — in `InitiatePayment`: detect 3DS required from Razorpay order/payment response, include in metadata
- `internal/ports/gateway.go` — `GatewayPaymentResponse` struct: add `NextAction *NextAction` field:
  ```go
  type NextAction struct {
      Type        string `json:"type"`        // "redirect_to_url", "use_stripe_sdk"
      RedirectURL string `json:"redirect_url,omitempty"`
      ClientSecret string `json:"client_secret,omitempty"`
  }
  ```

---

## Phase 3 — Delivery & Notifications

### 3.1 Tenant Webhook Delivery Worker

**Implemented.**

**Existing:** `tenant_webhook_deliveries` table, `ports.TenantWebhookWriter`, `postgres.TenantWebhookWriter.WriteDelivery`.

**New files:**
- `internal/jobs/tenant_webhook/job.go` — `Worker` struct with `RunOnce(ctx)`:
  1. Query `tenant_webhook_deliveries WHERE status = 'PENDING' AND next_attempt_at <= NOW() ORDER BY next_attempt_at ASC LIMIT batchSize`
  2. For each: HTTP POST to `endpoint_url` with payload, signature header (HMAC-SHA256 with tenant webhook secret)
  3. On 2xx: set `status = 'DELIVERED'`, `delivered_at = NOW()`
  4. On failure: increment `attempts`, set `last_error`, `last_attempt_at = NOW()`, compute `next_attempt_at` with exponential backoff (capped at max)
  5. On max attempts exhausted: set `status = 'FAILED'` (dead letter — no separate table, stays in `tenant_webhook_deliveries` for ops review)
- `internal/ports/tenant_webhook.go` — `TenantWebhookConfigStore` interface to get per-tenant webhook config (endpoint URL, secret, active status). Could be as simple as a new table `tenant_webhook_configs` or stored in existing tenant config.

**Modify:**
- `cmd/server/jobs.go` — add `tenant_webhook` ticker at configurable interval (default 5s), run at startup
- `config/config.go` — add `Jobs.TenantWebhookPollIntervalSec` (env `TENANT_WEBHOOK_POLL_INTERVAL_SECONDS`, default 5), `Jobs.TenantWebhookMaxAttempts` (env `TENANT_WEBHOOK_MAX_ATTEMPTS`, default 10), `Jobs.TenantWebhookMaxBackoffSec` (env `TENANT_WEBHOOK_MAX_BACKOFF_SECONDS`, default 3600)

**Migration:** `migrations/000005_tenant_webhook_configs.sql`
```sql
CREATE TABLE tenant_webhook_configs (
    tenant_id       UUID        PRIMARY KEY,
    endpoint_url    TEXT        NOT NULL,
    signing_secret  TEXT        NOT NULL,
    active          BOOLEAN     NOT NULL DEFAULT true,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

### 3.2 Notification System

**Implemented.**

**New files:**
- `internal/domain/notification/notification.go`:
  - `Notification` struct: `ID`, `TenantID`, `UserID`, `Type` (enum: `PAYMENT_SUCCESS`, `PAYMENT_FAILURE`, `REFUND_COMPLETED`, `REFUND_FAILED`, `DISPUTE_OPENED`, `DISPUTE_WON`, `DISPUTE_LOST`), `Channel` (enum: `EMAIL`, `SMS`), `Recipient`, `TemplateName`, `TemplateData map[string]any`, `Status` (enum: `PENDING`, `SENT`, `FAILED`), `Attempts`, `LastError`, `CreatedAt`
  - `Template` struct: `Name`, `Subject` (email), `BodyText`, `BodyHTML` (email), `SMSText`
  - `Preference` struct: `TenantID`, `UserID`, `NotificationType`, `Channel`, `Enabled`, `RecipientOverride`
- `internal/ports/notification.go`:
  - `NotificationStore` interface: `Insert(ctx, *Notification) error`, `ListPending(ctx, limit) ([]Notification, error)`, `MarkSent(ctx, id) error`, `MarkFailed(ctx, id, error) error`
  - `TemplateStore` interface: `GetTemplate(ctx, name) (*Template, error)`, `ListTemplates(ctx) ([]Template, error)`
  - `PreferenceStore` interface: `GetPreferences(ctx, tenantID, userID) ([]Preference, error)`
  - `EmailSender` interface: `SendEmail(ctx, to, subject, bodyText, bodyHTML) error`
  - `SMSSender` interface: `SendSMS(ctx, to, text) error`
- `internal/adapters/notification/smtp.go` — `SMTPEmailSender`: connect to SMTP server via config, send HTML+text emails. Config: host, port, username, password, from address.
- `internal/adapters/notification/sms.go` — `SMSSender` impl (could be Twilio API, or a stub for dev). Config: provider, API key, from number.
- `internal/adapters/notification/template.go` — `TemplateStore` impl: store templates in DB or embed defaults. Ship with default English templates for each notification type.
- `internal/adapters/notification/postgres.go` — `NotificationStore` impl: `notifications` table
- `internal/app/notification/service.go` — `Service`:
  - `Dispatch(ctx, notification) error` — check preferences, render template, send via channel, record result
  - `ProcessQueue(ctx)` — poll pending notifications, dispatch
  - `RegisterEventHandlers(eventBus)` — subscribe to `StatusEvent` from payment processing, create notifications

**Modify:**
- `internal/adapters/outbox/route.go` — add notification dispatch as new outbox route for relevant event types (`TRANSACTION_SUCCEEDED`, `TRANSACTION_FAILED`, `REFUND_SUCCEEDED`, `REFUND_FAILED`)
- `cmd/server/relay.go` — wire notification service's handler into the outbox router
- `cmd/server/jobs.go` — add notification queue processing job (ticker, default 2s)
- `config/config.go` — add `Notifications` config section: `SMTPHost`, `SMTPPort`, `SMTPUser`, `SMTPPassword`, `SMTPFromAddress`, `SMSProvider`, `SMSAPIKey`, `SMSFromNumber`
- `cmd/server/main.go` — wire `NotificationService`, `EmailSender`, `SMSSender`, `TemplateStore`

**Migration:** `migrations/000006_notifications.sql`
```sql
CREATE TABLE notifications (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       UUID        NOT NULL,
    user_id         UUID,
    notification_type TEXT      NOT NULL CHECK (notification_type IN (
                        'PAYMENT_SUCCESS', 'PAYMENT_FAILURE', 'REFUND_COMPLETED',
                        'REFUND_FAILED', 'DISPUTE_OPENED', 'DISPUTE_WON', 'DISPUTE_LOST'
                    )),
    channel         TEXT        NOT NULL CHECK (channel IN ('EMAIL', 'SMS')),
    recipient       TEXT        NOT NULL,
    template_name   TEXT        NOT NULL,
    template_data   JSONB       NOT NULL DEFAULT '{}',
    status          TEXT        NOT NULL DEFAULT 'PENDING'
                    CHECK (status IN ('PENDING', 'SENT', 'FAILED')),
    attempts        INT         NOT NULL DEFAULT 0,
    last_error      TEXT,
    sent_at         TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE notification_templates (
    name        TEXT        PRIMARY KEY,
    subject     TEXT,
    body_text   TEXT        NOT NULL,
    body_html   TEXT,
    sms_text    TEXT,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE notification_preferences (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       UUID        NOT NULL,
    user_id         UUID,
    notification_type TEXT      NOT NULL,
    channel         TEXT        NOT NULL,
    enabled         BOOLEAN     NOT NULL DEFAULT true,
    recipient_override TEXT,
    UNIQUE (tenant_id, user_id, notification_type, channel)
);

CREATE INDEX idx_notifications_pending ON notifications (status, created_at ASC) WHERE status = 'PENDING';
CREATE INDEX idx_notifications_tenant ON notifications (tenant_id, created_at DESC);

-- Seed default templates
INSERT INTO notification_templates (name, subject, body_text, body_html, sms_text) VALUES
('PAYMENT_SUCCESS', 'Payment Successful', 'Your payment of {{amount}} {{currency}} was successful. Reference: {{transaction_id}}', '<h1>Payment Successful</h1><p>Your payment of {{amount}} {{currency}} was successful.</p><p>Reference: {{transaction_id}}</p>', 'Payment of {{amount}}{{currency}} successful. Ref: {{transaction_id}}'),
('PAYMENT_FAILURE', 'Payment Failed', 'Your payment of {{amount}} {{currency}} has failed. Reason: {{reason}}', '<h1>Payment Failed</h1><p>Your payment of {{amount}} {{currency}} has failed.</p><p>Reason: {{reason}}</p>', 'Payment of {{amount}}{{currency}} failed: {{reason}}'),
('REFUND_COMPLETED', 'Refund Completed', 'A refund of {{amount}} {{currency}} has been processed for transaction {{transaction_id}}.', '<h1>Refund Completed</h1><p>A refund of {{amount}} {{currency}} has been processed.</p>', 'Refund of {{amount}}{{currency}} completed for {{transaction_id}}'),
('REFUND_FAILED', 'Refund Failed', 'A refund for {{amount}} {{currency}} has failed for transaction {{transaction_id}}. Reason: {{reason}}', '<h1>Refund Failed</h1><p>A refund has failed.</p><p>Reason: {{reason}}</p>', 'Refund of {{amount}}{{currency}} failed: {{reason}}'),
('DISPUTE_OPENED', 'Dispute Opened', 'A dispute has been opened for transaction {{transaction_id}}. Reason: {{reason}}. Evidence due by {{evidence_due_by}}.', '<h1>Dispute Opened</h1><p>A dispute has been opened for transaction {{transaction_id}}.</p><p>Reason: {{reason}}</p><p>Evidence due by: {{evidence_due_by}}</p>', 'Dispute opened for {{transaction_id}}: {{reason}}. Due: {{evidence_due_by}}'),
('DISPUTE_WON', 'Dispute Won', 'The dispute for transaction {{transaction_id}} has been resolved in your favor.', '<h1>Dispute Won</h1><p>The dispute for transaction {{transaction_id}} has been resolved in your favor.</p>', 'Dispute won for {{transaction_id}}'),
('DISPUTE_LOST', 'Dispute Lost', 'The dispute for transaction {{transaction_id}} has been resolved against you.', '<h1>Dispute Lost</h1><p>The dispute for transaction {{transaction_id}} has been resolved against you.</p>', 'Dispute lost for {{transaction_id}}');
```

---

## Phase 4 — Reconciliation

### 4.1 SettlementReportFetcher — Per-Gateway

**Implemented.**

**New files:**
- `internal/adapters/gateways/stripe/settlement.go`:
  - Call Stripe's BalanceTransaction list API: `GET /v1/balance_transactions` with `created` range filter
  - Map `BalanceTransaction` fields to `SettlementEntry` (amount is in cents, Stripe fee is in `fee` field, exchange rate in `exchange_rate`)
  - Return `SettlementReport` struct
- `internal/adapters/gateways/razorpay/settlement.go`:
  - Call Razorpay's payment listing API: `GET /v1/payments` with date range filter
  - Map fields appropriately (fee from `fee` field, status from `status`)
- `internal/adapters/gateways/fib/settlement.go`:
  - FIB stub — returns empty report (no dedicated settlement endpoint available)

**Modify:**
- `internal/adapters/gateways/registry.go` — added `SettlementFetcher(gatewayID)` method and `RegisterSettlementFetcher` for per-gateway settlement fetcher access
- `internal/bootstrap/bootstrap.go` — register settlement fetchers for all 3 gateways after adapter registration

### 4.2 Reconciliation App Service

**Implemented.**

**New files:**
- `internal/app/reconciliation/service.go`:
  - `Service` struct with deps: `ReconciliationStore`, `TransactionReader`, `TransactionLister`, `GatewayRegistry`, `Logger`, `Metrics`
  - `CreateJob(ctx, gatewayID, periodStart, periodEnd, transactionID, triggeredBy)` — insert job with `PENDING` status
  - `RunJob(ctx, jobID)` — fetch settlement report, compare against internal transactions, create entries for mismatches
  - `reconcileSingle(ctx, job)` — single-transaction reconciliation using txn.CreatedAt ±24h window
  - `reconcileBatch(ctx, job)` — batch reconciliation: fetch internal txns by gateway+period, compare against settlement report
  - `compareTransaction(txn, settlementEntry, jobID)` — compare status, amount, fees; return Entry for mismatches or nil if match
  - `ListJobs`, `GetJob`, `GetEntries`, `ResolveEntry` — CRUD passthrough to store
- `internal/adapters/postgres/reconciliation.go` — `ReconciliationStore` with methods:
  - `CreateJob`, `UpdateJob`, `GetJob`, `ListJobs`
  - `InsertEntries` (batch), `GetEntries`, `UpdateEntry`
  - `GetPendingJobs`, `GetAutoResolutionConfig`

### 4.3 Reconciliation Background Job

**Implemented.**

**New files:**
- `internal/jobs/reconciliation/job.go` — `Scheduler` struct:
  - `RunOnce(ctx)` — query pending jobs (status=PENDING, triggered_by=system), call `reconSvc.RunJob(ctx, jobID)` for each
  - Config: `Interval` (default 1h), `BatchSize` (default 5)

**Modify:**
- `cmd/server/jobs.go` — add reconciliation ticker (configurable interval, default 5min), run at startup
- `config/config.go` — added `Jobs.ReconciliationIntervalSec` (env `RECONCILIATION_INTERVAL_SECONDS`, default 300)

### 4.4 Reconciliation API

**Implemented.**

**New files:**
- `internal/api/handlers/reconciliation.go` — `ReconciliationHandler`:
  - `CreateJob` — `POST /api/v1/reconciliation/jobs` (body: `{gateway_id, period_start, period_end}` or `{transaction_id}`)
  - `RunJob` — `POST /api/v1/reconciliation/jobs/{id}/run`
  - `GetJob` — `GET /api/v1/reconciliation/jobs/{id}`
  - `ListJobs` — `GET /api/v1/reconciliation/jobs` (filter by gateway, status)
  - `GetEntries` — `GET /api/v1/reconciliation/jobs/{id}/entries` (filter by mismatch_type, resolution_status)
  - `ResolveEntry` — `POST /api/v1/reconciliation/jobs/{id}/entries/{entry_id}/resolve` (body: `{notes}`)
- `internal/ports/reconciliation.go` — `ReconciliationStore`, `SettlementReportFetcher`, `SettlementReport`, `SettlementEntry`, filter types

**Modify:**
- `internal/api/router.go`:
  - Added `Reconciliation *handlers.ReconciliationHandler` to `Deps`
  - Added 6 reconciliation routes under `/api/v1/reconciliation/`
- `cmd/server/main.go` — wired `ReconciliationStore`, `ReconciliationService`, `ReconciliationHandler` into `deps`
- `internal/bootstrap/bootstrap.go` — register settlement fetchers for all 3 gateways

---

## Phase 5 — Ops & Analytics

### 5.1 Transaction Search API

**Implemented.**

**Modify:**
- `internal/ports/transaction_filters.go` (new) — `TransactionFilter` struct:
  ```go
  type TransactionFilter struct {
      TenantID    *uuid.UUID
      UserID      *uuid.UUID
      Status      []string
      GatewayID   []string
      MinAmount   *int64
      MaxAmount   *int64
      Currency    *string
      PaymentMethod []string
      DateFrom    *time.Time
      DateTo      *time.Time
      Cursor      *string    // opaque cursor for pagination
      Limit       int
  }

  type TransactionListResult struct {
      Transactions []TransactionSummary
      NextCursor   *string
      TotalCount   int64
  }
  ```
- `internal/adapters/postgres/transaction.go` — add `List(ctx, filter) (*TransactionListResult, error)`:
  - Build dynamic `SELECT` query with `WHERE` clauses for each non-nil filter
  - Use cursor-based pagination: cursor is base64-encoded `created_at + id`
  - Add count query for `TotalCount`
  - Return `TransactionSummary` (subset of fields: id, tenant_id, user_id, amount, currency, status, gateway_id, created_at)
- `internal/app/payment/service.go` — add `ListTransactions(ctx, filter) (*TransactionListResult, error)` method that calls `txnRepo.List`
- `internal/api/handlers/payment.go` — add `List` handler:
  - `GET /api/v1/payments?tenant_id=&status=&gateway_id=&min_amount=&max_amount=&currency=&payment_method=&date_from=&date_to=&cursor=&limit=`
  - Parse query params into `TransactionFilter`
  - Return JSON with `{ data: [...], next_cursor: "...", total_count: N }`
  - Optional `Accept: text/csv` → return CSV with headers

**Modify also:**
- `internal/api/router.go` — add route:
  - `GET /api/v1/payments` → `deps.Payment.List` (requires service/ops token)
- Need to handle route conflict: currently `GET /api/v1/payments/{id}` is registered. In Go 1.22+ ServeMux, `GET /api/v1/payments` and `GET /api/v1/payments/{id}` can coexist.

### 5.2 Dead Letter Management API

**Implemented.**

**Modify:**
- `internal/adapters/postgres/outbox.go` — add `ListDeadLetters(ctx, filter) ([]DeadLetter, error)`:
  - Query `outbox_dead_letters` with filters: `resolved_at IS NULL`, event_type, date range, cursor pagination
- `internal/ports/outbox.go` — add `ListDeadLetters(ctx, filter)` to `OutboxWriter` interface
  - Add `DeadLetterFilter` struct: `Resolved *bool`, `EventType *string`, `DateFrom, DateTo *time.Time`, `Cursor *string`, `Limit int`

**New files:**
- `internal/api/handlers/dead_letter.go` — `DeadLetterHandler`:
  - `List` — `GET /api/v1/dead-letters` (filter by event type, date range, resolved/unresolved)
  - `Replay` — `POST /api/v1/dead-letters/{id}/replay` (calls `outboxWriter.ReplayDeadLetter`)

**Modify:**
- `internal/api/router.go`:
  - Add `DeadLetter *handlers.DeadLetterHandler` to `Deps`
  - Add routes:
    - `GET /api/v1/dead-letters`
    - `POST /api/v1/dead-letters/{id}/replay`
- `cmd/server/main.go` — wire `DeadLetterHandler`

### 5.3 Analytics / Reporting API

**Implemented.**

**New files:**
- `internal/app/analytics/service.go`:
  - `GetVolumeTrends(ctx, periodStart, periodEnd, granularity) (*VolumeReport, error)` — daily/hourly transaction count and volume grouped by status, currency
  - `GetGatewayPerformance(ctx, periodStart, periodEnd) (*GatewayPerformanceReport, error)` — success rate, avg latency, error rate per gateway
  - `GetSettlementSummary(ctx, periodStart, periodEnd) (*SettlementSummary, error)` — total settled volume, gateway fees, net amounts
  - `GetDashboardSummary(ctx) (*DashboardSummary, error)` — today's volume, success rate, pending disputes, pending reconciliations
- `internal/adapters/postgres/analytics.go`:
  - `TransactionVolumeByPeriod(ctx, start, end, granularity) ([]VolumeBucket, error)` — time-bucketed aggregation queries using `date_trunc` and `GROUP BY`
  - `GatewaySuccessRate(ctx, start, end) ([]GatewayStats, error)` — per-gateway success/failure counts and latency
  - `SettlementSummary(ctx, start, end) (*SettlementSummary, error)` — aggregate queries on transactions joined with reconciliation data
- `internal/api/handlers/analytics.go` — `AnalyticsHandler`:
  - `GET /api/v1/analytics/volume` — `?period_start=&period_end=&granularity=day|hour`
  - `GET /api/v1/analytics/gateway-performance` — `?period_start=&period_end=`
  - `GET /api/v1/analytics/settlement-summary` — `?period_start=&period_end=`
  - `GET /api/v1/analytics/dashboard` — today's summary

**Modify:**
- `internal/api/router.go`:
  - Add `Analytics *handlers.AnalyticsHandler` to `Deps`
  - Add routes:
    - `GET /api/v1/analytics/volume`
    - `GET /api/v1/analytics/gateway-performance`
    - `GET /api/v1/analytics/settlement-summary`
    - `GET /api/v1/analytics/dashboard`
- `cmd/server/main.go` — wire `AnalyticsService`, `AnalyticsHandler` into `deps`

---

## Phase 6 — Fraud Engine

### 6.1 Fraud Rules Engine

**Implemented.**

**New files:**
- `internal/domain/fraud/rule.go`:
  - `Rule` interface: `Evaluate(ctx, context) (*Verdict, error)`
  - `RuleContext` struct: `Transaction` (the proposed payment), `TenantID`, `UserID`, `CardFingerprint`, `IPAddress`, `UserAgent`, `DeviceID`, `RecentActivity []RecentTransaction`, `VelocityCounts map[string]int`
  - `Verdict` struct: `Action` (enum: `ALLOW`, `REVIEW`, `BLOCK`), `Score` (0-100), `Reasons []string`, `RuleName`
  - Concrete rules:
    - `VelocityRule` — max N transactions per tenant/card/IP in time window. Configurable threshold + window.
    - `AmountThresholdRule` — block/review transactions above absolute amount or % deviation from user average
    - `BlacklistRule` — check card fingerprint, IP, email against blacklist (stored in Valkey or separate table)
    - `CountryMismatchRule` — flag if card issuing country ≠ IP country ≠ shipping country
    - `TimeOfDayRule` — flag transactions outside normal hours for user
- `internal/ports/fraud.go`:
  - `FraudStore` interface: `RecordAttempt`, `GetVelocity`, `IsBlacklisted`, `AddToBlacklist`
  - `FraudEngine` interface: `Evaluate(ctx, context) (*Verdict, error)`
- `internal/app/fraud/engine.go`:
  - `Engine` struct with ordered list of `Rule`s
  - `Evaluate(ctx, context) (*Verdict, error)` — run through rules in order, aggregate scores, return highest-severity action
  - Configurable per-tenant thresholds
- `internal/adapters/fraud/engine.go` — `Engine` with dependency-injected rule list
- `internal/adapters/valkey/fraud.go`:
  - `FraudStore` impl: velocity counters as Valkey sorted sets with TTL, blacklist as sets
  - Lua script for atomic velocity increment + expiry

**Modify:**
- `internal/app/payment/service.go` — in `Create`:
  1. Before processing, build `FraudContext` from transaction data
  2. Call `fraudEngine.Evaluate(ctx, context)`
  3. If `BLOCK`: return error with `fraud_blocked` reason, write audit log entry
  4. If `REVIEW`: set transaction metadata `fraud_review = true`, proceed but flag for ops
  5. If `ALLOW`: proceed normally
- `config/config.go` — add `Fraud` config section:
  - `Enabled` (bool, default false)
  - `VelocityMaxPerTenant`, `VelocityWindowSec`
  - `AmountThresholdSingle`, `AmountThresholdPercentDeviation`
  - `BlockOnCountryMismatch`, `BlockOnBlacklist`
- `cmd/server/main.go` — wire `FraudStore`, `FraudEngine` into `deps`, inject into `paymentSvc`
- `internal/api/handlers/payment.go` — `Create` handler: map fraud block error to 403 response

**Migration:** `migrations/000007_fraud.sql`
```sql
CREATE TABLE fraud_blacklist (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    list_type       TEXT        NOT NULL CHECK (list_type IN ('card_fingerprint', 'ip_address', 'email', 'device_id')),
    list_value      TEXT        NOT NULL,
    reason          TEXT,
    created_by      TEXT        NOT NULL DEFAULT 'system',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at      TIMESTAMPTZ,
    UNIQUE (list_type, list_value)
);

CREATE TABLE fraud_review_queue (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    transaction_id  UUID        NOT NULL REFERENCES transactions(id),
    rule_name       TEXT        NOT NULL,
    score           INT         NOT NULL CHECK (score >= 0 AND score <= 100),
    reasons         TEXT[]      NOT NULL DEFAULT '{}',
    status          TEXT        NOT NULL DEFAULT 'PENDING_REVIEW'
                    CHECK (status IN ('PENDING_REVIEW', 'APPROVED', 'REJECTED')),
    reviewed_by     TEXT,
    reviewed_at     TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_fraud_blacklist_lookup ON fraud_blacklist (list_type, list_value);
CREATE INDEX idx_fraud_review_pending ON fraud_review_queue (status, created_at ASC) WHERE status = 'PENDING_REVIEW';
CREATE INDEX idx_fraud_review_transaction ON fraud_review_queue (transaction_id);
```

---

## Summary of All New Migrations

| # | Migration File | Purpose |
|---|---------------|---------|
| `000002` | `audit_log_extend.sql` | Extend audit_log with actor_type, reason; add index |
| `000003` | `add_txn_fee_fields.sql` | Add gateway_fee fields to transactions |
| `000004` | `disputes.sql` | disputes + dispute_evidence tables |
| `000005` | `tenant_webhook_configs.sql` | tenant_webhook_configs table |
| `000006` | `notifications.sql` | notifications, templates, preferences tables |
| `000007` | `fraud.sql` | fraud_blacklist, fraud_review_queue tables |

Each migration has a corresponding `.down.sql` that reverses the changes.

---

## Summary of All New Files

```
internal/
├── domain/
│   ├── audit/audit.go
│   ├── dispute/dispute.go
│   ├── dispute/evidence.go
│   ├── notification/notification.go
│   └── fraud/rule.go
├── ports/
│   ├── audit.go
│   ├── dispute.go
│   ├── gateway_metrics.go
│   ├── tenant_webhook.go
│   ├── notification.go
│   ├── transaction_filters.go
│   └── fraud.go
├── app/
│   ├── audit/service.go
│   ├── dispute/service.go
│   ├── reconciliation/service.go
│   ├── analytics/service.go
│   ├── notification/service.go
│   └── fraud/engine.go
├── adapters/
│   ├── postgres/
│   │   ├── audit.go
│   │   ├── dispute.go
│   │   ├── reconciliation.go
│   │   ├── analytics.go
│   │   ├── notification.go
│   │   └── gateway_metrics.go
│   ├── gateways/
│   │   ├── stripe/settlement.go
│   │   ├── razorpay/settlement.go
│   │   └── fib/settlement.go
│   ├── notification/
│   │   ├── smtp.go
│   │   ├── sms.go
│   │   ├── template.go
│   │   └── postgres.go
│   └── fraud/
│       └── engine.go
├── api/
│   └── handlers/
│       ├── dispute.go
│       ├── reconciliation.go
│       ├── analytics.go
│       ├── dead_letter.go
│       └── *_test.go (all uncovered handlers)
└── jobs/
    ├── gateway_metrics/job.go
    ├── tenant_webhook/job.go
    ├── reconciliation/job.go
    └── notification/job.go
```

## Summary of All Modified Files

```
config/config.go                          — multiple new config sections
cmd/server/main.go                        — wire new services into deps
cmd/server/jobs.go                        — add 4 new background jobs
cmd/server/relay.go                       — wire notification outbox route
internal/api/router.go                    — add new routes + Deps fields
internal/app/payment/service.go           — audit, fees, fraud injection
internal/app/payment/gateway.go           — audit at finalize
internal/app/refund/service.go            — audit at state changes
internal/app/refund/process.go            — audit at terminal transitions
internal/app/webhook/service.go           — audit at state changes
internal/app/cancel/service.go            — audit at cancel
internal/domain/transaction/transaction.go — fee fields
internal/ports/outbox.go                  — ListDeadLetters, DeadLetterFilter
internal/ports/gateway.go                 — NextAction on GatewayPaymentResponse
internal/adapters/postgres/outbox.go      — ListDeadLetters
internal/adapters/postgres/transaction.go — List (search)
internal/adapters/gateways/stripe/webhook.go      — dispute + 3DS events
internal/adapters/gateways/razorpay/webhook.go    — dispute + 3DS events
internal/adapters/gateways/stripe/stripe.go       — 3DS next_action in response
internal/adapters/gateways/razorpay/razorpay.go   — 3DS next_action in response
internal/adapters/gateways/registry.go             — SettlementReportFetcher access
internal/api/handlers/payment.go                  — List handler
internal/api/handlers/tenant_gateway.go            — audit on config changes
internal/adapters/outbox/route.go                  — notification event handler
migrations/000002_*.sql through 000007_*.sql       — 6 new migration pairs
```

---

## Phase 7 — API Cleanup & Documentation

### 7.1 API Response Shape Cleanup (Implemented)

**Commit:** `bf244fb` — removed 12 overhead fields, added 4 missing fields, fixed 2 pointer bugs, standardised timestamps.

**Changes:**

| Endpoint | Field | Action | Rationale |
|----------|-------|--------|-----------|
| Envelope | `timestamp` | Removed | 17 bytes/response, frontend has `Date` header |
| Envelope | `success` | Added back | Frontend-friendly for generic error handling (commit `1d4a89c`) |
| `POST /api/v1/payments` | `checkout_url` | Removed | Browser goes to `/pay/{id}?token={token}` directly |
| `POST /api/v1/payments` | `baseURL` removed from handler | — | Token is opaque, URL constructed client-side |
| `GET /api/v1/payments` | `total_count` → `has_more` | Changed | No COUNT(*) query needed, cursor pagination only |
| `GET /api/v1/payments` | `tenant_id` removed from items | Removed | Redundant with auth context |
| `GET /api/v1/payments/{id}` | `gateway` removed | Removed | Available in `gateway_id` on List |
| `GET /api/v1/payments/{id}` | `customer_id` | Added | Missing from response, present in DB |
| `GET /api/v1/payments/{id}` | `customer_email` | Added | Missing from response, present in DB |
| `GET /api/v1/payments/{id}` | `description` | Added | Missing from response, present in DB |
| `GET /api/v1/payments/{id}` | `metadata` | Added | Missing from response, present in DB |
| `POST /api/v1/payments/{id}/refunds` | `initiated_by` | Removed | Derived from auth context |
| `POST /api/v1/payments/{id}/refunds` | `gateway_refund_id` → `*string` | Fixed | Was `string` (empty on pending), now `omitempty` pointer |
| `POST /api/v1/payments/{id}/cancel` | `actor`, `via` | Removed from request | Derived from auth context |
| `POST /api/v1/payments/{id}/cancel` | `outcome` | Removed from response | Status field is sufficient |
| `POST /webhooks/gateway/{id}` | `received` | Removed from response | Server timestamp, not useful to caller |
| `GET /api/v1/gateways` | `is_active` | Removed | All returned gateways are active |
| `GET /api/v1/tenants/{id}/gateways` | `config_version` | Removed | Internal versioning, not API contract |
| `GET /api/v1/tenants/{id}/gateways` | `created_at`, `updated_at` | Removed | Internal timestamps, not useful |
| `GET /api/v1/disputes/{id}` | `gateway_dispute_id` | Removed | Internal ID, not useful |
| `GET /api/v1/dead-letters` | `aggregate_type` | Removed | Always "transaction" |
| `GET /api/v1/dead-letters` | `resolved_by` → `*string` | Fixed | Was `string` (empty on unresolved), now `omitempty` pointer |
| All timestamps | Standardized to `RFC3339Nano` | Changed | Consistent, sub-second precision |

### 7.2 Squirrel Query Builder (Implemented)

**Commit:** `6ac99f7`

- `internal/adapters/postgres/transaction.go` — `List` method rewritten with `github.com/Masterminds/squirrel`
- Eliminated manual arg-index counting for 12+ dynamic WHERE clauses
- Squirrel builder: `squirrel.StatementBuilder.PlaceholderFormat(squirrel.Dollar)`

### 7.3 Dead Letter Alerting (Implemented)

**Commit:** `6ac99f7`

- `internal/adapters/postgres/outbox.go` — `OutboxWriter` has `ports.Logger` field via `SetLogger` method
- `MarkExhausted` emits `LogEventOutboxDeadLetter` ERROR log with event ID, aggregate ID, error, attempts
- `cmd/server/relay.go` — wired `d.outboxWriter.SetLogger(logger)` at startup

### 7.4 Seed Data (Implemented)

**File:** `seed/seed.sql`

FIB-only seed data for development:
- Gateway catalog: FIB (IQD, card, cancel supported, no partial refund)
- Gateway timeouts: 30s + 300s buffer
- Gateway fee models: no fees (dev sandbox)
- Gateway metadata schemas: QR code, readable code, app links
- Tenant gateway config: tenant `00000000-...-0001` → FIB (plaintext for dev)
- Example completed transaction: 50,000 IQD, SUCCEEDED, with gateway_metadata
- Notification templates: PAYMENT_SUCCESS, PAYMENT_FAILURE, REFUND_COMPLETED, REFUND_FAILED
- Circuit breaker + metrics: closed state, zero metrics

### 7.5 OpenAPI Specification (Implemented)

**File:** `openapi.yaml`

Full OpenAPI 3.0.3 spec covering all 22 endpoints:
- All request/response schemas match current implementation exactly
- Envelope format: `{ success, data, error, request_id }`
- Auth: serviceToken (header), opsToken (header), checkoutToken (query)
- FIB examples throughout
- Reusable parameters and response definitions
- SSE endpoint documented (text/event-stream)
- Browser checkout pages documented (HTML responses)
- Error response schema with code + message

### 7.6 Postman Collection (Implemented)

**Files:** `postman/collection.json`, `postman/environment.json`

Postman v2.1 collection with:
- All 22 endpoints organized by feature (Health, Gateways, Tenant Gateways, Payments, Refunds, Cancels, Disputes, Dead Letters, Webhooks, Browser Checkout, SSE)
- Auto-save script: Create Payment auto-saves `transaction_id` and `token` as collection variables
- FIB-specific request bodies with example values
- Environment variables: `base_url`, `service_token`, `ops_token`, `tenant_id`, `transaction_id`, `token`
- Ops token auth override for refund/cancel endpoints
- Idempotency-Key auto-generated via `{{$guid}}`

### 7.7 Integration Documentation (Implemented)

**Files:** `docs/order-service-integration.md`, `docs/frontend-integration.md`

**Order Service Integration** (server-to-server):
- Step-by-step: Create Payment → Redirect Browser → Handle Callback → Poll Status → Refund → Cancel → List
- Full curl examples with request/response bodies
- Error handling table (all HTTP status codes + error codes)
- Security checklist

**Frontend Integration** (browser):
- Architecture diagram (Frontend → Backend → Payment Service)
- Step-by-step: Initiate Payment → Redirect to Checkout → Real-Time Updates → Custom UI (optional)
- SSE event handling code examples
- Polling fallback implementation
- FIB-specific UI description (QR code, app links, countdown timer)
- Security rules table
- Troubleshooting guide

### 7.8 Plan & Status Updates (Implemented)

**File:** `plan.md` — updated throughout with implementation status markers.
