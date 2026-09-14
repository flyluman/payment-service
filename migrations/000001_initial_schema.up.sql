-- Initial schema — compacted from incremental migrations.
-- Includes all tables, indexes, partition seed, and outbox notify trigger.
-- Compatible with golang-migrate and testsupport.applyMigrations().

-- ── Gateway catalog ─────────────────────────────────────────────────────

CREATE TABLE gateway_config (
    gateway_id                 TEXT        PRIMARY KEY,
    display_name               TEXT        NOT NULL,
    is_active                  BOOLEAN     NOT NULL DEFAULT true,
    min_amount                 BIGINT      NOT NULL DEFAULT 0,
    max_amount                 BIGINT      NOT NULL,
    supported_currencies       TEXT[]      NOT NULL DEFAULT '{}',
    supported_methods          TEXT[]      NOT NULL DEFAULT '{}',
    idempotency_capable        BOOLEAN     NOT NULL DEFAULT true,
    supports_cancel            BOOLEAN     NOT NULL DEFAULT false,
    supports_partial_refund    BOOLEAN     NOT NULL DEFAULT false,
    priority                   INT         NOT NULL DEFAULT 100,
    credentials_secret_arn     TEXT,
    webhook_secret_arn         TEXT,
    webhook_ip_allowlist       TEXT[],
    webhook_replay_window_sec  INT         NOT NULL DEFAULT 300,
    webhook_clock_skew_sec     INT         NOT NULL DEFAULT 30,
    created_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE gateway_timeouts (
    gateway_id                 TEXT    NOT NULL REFERENCES gateway_config(gateway_id),
    payment_method             TEXT    NOT NULL CHECK (payment_method IN ('card', 'upi', 'netbanking', 'wallet')),
    gateway_timeout_sec        INT     NOT NULL,
    payment_method_buffer_sec  INT     NOT NULL,
    estimated_timeout_sec      INT     NOT NULL GENERATED ALWAYS AS (gateway_timeout_sec + payment_method_buffer_sec) STORED,
    PRIMARY KEY (gateway_id, payment_method)
);

CREATE TABLE gateway_fee_models (
    gateway_id                              TEXT        NOT NULL REFERENCES gateway_config(gateway_id),
    payment_method                          TEXT        NOT NULL CHECK (payment_method IN ('card', 'upi', 'netbanking', 'wallet')),
    fixed_fee                               BIGINT      NOT NULL DEFAULT 0,
    percentage_bps                          BIGINT      NOT NULL DEFAULT 0,
    interchange_cap                         BIGINT,
    discount_volume_threshold               BIGINT      NOT NULL DEFAULT 0,
    charges_currency                        CHAR(3),
    created_at                              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at                              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (gateway_id, payment_method)
);

CREATE TABLE gateway_metadata_schemas (
    gateway_id     TEXT        PRIMARY KEY REFERENCES gateway_config(gateway_id),
    allowed_keys   TEXT[]      NOT NULL DEFAULT '{}',
    required_keys  TEXT[]      NOT NULL DEFAULT '{}',
    max_size_bytes INT         NOT NULL DEFAULT 4096,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE gateway_circuit_breaker_state (
    gateway_id                   TEXT        PRIMARY KEY REFERENCES gateway_config(gateway_id),
    state                        TEXT        NOT NULL DEFAULT 'CLOSED' CHECK (state IN ('CLOSED', 'OPEN', 'HALF_OPEN')),
    cooldown_until               TIMESTAMPTZ,
    consecutive_failures         INT         NOT NULL DEFAULT 0,
    last_known_reliability_score INT         NOT NULL DEFAULT 100,
    updated_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── Transactions & related ───────────────────────────────────────────────

CREATE TABLE transactions (
    id                       UUID        PRIMARY KEY,
    tenant_id                UUID        NOT NULL,
    user_id                  UUID        NOT NULL,
    amount                   BIGINT      NOT NULL CHECK (amount > 0),
    currency                 CHAR(3)     NOT NULL CHECK (currency = upper(currency)),
    payment_method           TEXT        NOT NULL
                             CHECK (payment_method IN ('card', 'upi', 'netbanking', 'wallet')),
    status                   TEXT        NOT NULL DEFAULT 'PENDING'
                             CHECK (status IN (
                                 'PENDING', 'PROCESSING', 'SUCCEEDED', 'FAILED',
                                 'CANCELLED', 'REFUNDED', 'REFUND_FAILED'
                             )),
    version                  INT         NOT NULL DEFAULT 1 CHECK (version >= 1),
    gateway_id               TEXT        NOT NULL,
    gateway_reference_id     TEXT,
    gateway_idempotency_key  TEXT,
    attempted_gateway        TEXT,
    actual_gateway           TEXT,
    original_gateway         TEXT,
    estimated_timeout_seconds INT        NOT NULL CHECK (estimated_timeout_seconds > 0),
    gateway_fee_estimate       BIGINT,
    gateway_fee_currency       CHAR(3),
    gateway_fee_model_version  INT        NOT NULL DEFAULT 0,
    failure_reason           JSONB,
    method_details           JSONB,
    metadata                 JSONB,
    CONSTRAINT metadata_size CHECK (pg_column_size(metadata) <= 4096),
    description              TEXT,
    customer_id              UUID,
    customer_email           TEXT,
    cancel_intent            BOOLEAN     NOT NULL DEFAULT false,
    cancel_requested_by      TEXT
                             CHECK (cancel_requested_by IN ('system', 'tenant', 'ops', 'gateway') OR cancel_requested_by IS NULL),
    cancel_requested_at      TIMESTAMPTZ,
    cancel_requested_via     TEXT
                             CHECK (cancel_requested_via IN ('api', 'dashboard', 'ops-tool') OR cancel_requested_via IS NULL),
    processing_started_at    TIMESTAMPTZ,
    processing_timeout       INTERVAL,
    token_hash               TEXT,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    callback_url             TEXT,
    redirect_url             TEXT,
    fee_breakdown            JSONB,
    gateway_amount           BIGINT,
    gateway_currency         CHAR(3)
);

CREATE TABLE reconciliation_jobs (
    id             UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    gateway_id     TEXT        NOT NULL,
    transaction_id UUID        REFERENCES transactions(id),
    period_start   TIMESTAMPTZ,
    period_end     TIMESTAMPTZ,
    status         TEXT        NOT NULL DEFAULT 'PENDING'
                   CHECK (status IN ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED')),
    triggered_by   TEXT        NOT NULL CHECK (triggered_by IN ('system', 'ops')),
    actor          TEXT        NOT NULL DEFAULT 'system',
    mismatch_count INT         NOT NULL DEFAULT 0,
    error          TEXT,
    started_at     TIMESTAMPTZ,
    completed_at   TIMESTAMPTZ,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT job_scope CHECK (
        (transaction_id IS NOT NULL AND period_start IS NULL     AND period_end IS NULL)
        OR
        (transaction_id IS NULL     AND period_start IS NOT NULL AND period_end IS NOT NULL)
    )
);

CREATE TABLE reconciliation_entries (
    id                     UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id                 UUID        NOT NULL REFERENCES reconciliation_jobs(id),
    transaction_id         UUID        NOT NULL REFERENCES transactions(id),
    internal_status        TEXT        NOT NULL,
    gateway_status         TEXT        NOT NULL,
    internal_amount        BIGINT      NOT NULL,
    gateway_amount         BIGINT      NOT NULL,
    internal_fees          BIGINT,
    gateway_fees           BIGINT,
    fx_rate_applied        NUMERIC(18,8),
    fx_rate_at_settlement  NUMERIC(18,8),
    fee_mismatch_reason    TEXT,
    mismatch_type          TEXT        NOT NULL
                           CHECK (mismatch_type IN ('STATUS_MISMATCH', 'AMOUNT_MISMATCH', 'FEE_MISMATCH', 'MISSING_INTERNAL', 'MISSING_GATEWAY')),
    resolution_status      TEXT        NOT NULL DEFAULT 'UNRESOLVED'
                           CHECK (resolution_status IN ('UNRESOLVED', 'RESOLVED', 'AUTO_REFUND_ISSUED', 'AUTO_INVOICED')),
    resolved_by            TEXT,
    resolved_at            TIMESTAMPTZ,
    notes                  TEXT,
    auto_resolution_action TEXT,
    auto_resolution_at     TIMESTAMPTZ,
    auto_resolution_by     TEXT,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE settlement_auto_resolution_log (
    id                       UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    settlement_id            UUID        NOT NULL,
    discrepancy_amount_paise BIGINT      NOT NULL,
    threshold_bps            INT         NOT NULL,
    absolute_cap_paise       BIGINT      NOT NULL,
    qualified_percentage     BOOLEAN     NOT NULL,
    qualified_absolute       BOOLEAN     NOT NULL,
    action                   TEXT,
    executed_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    executed_by              TEXT        NOT NULL DEFAULT 'system'
);

CREATE TABLE idempotency_keys (
    id                UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    composite_key_hash TEXT        NOT NULL UNIQUE,
    request_hash       TEXT        NOT NULL,
    response           JSONB       NOT NULL,
    status             TEXT        NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at         TIMESTAMPTZ NOT NULL DEFAULT (NOW() + INTERVAL '24 hours')
);

CREATE TABLE processing_lease (
    idempotency_key   UUID        PRIMARY KEY,
    payment_intent_id UUID        NOT NULL REFERENCES transactions(id),
    lease_acquired_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    lease_ttl_sec     INT         NOT NULL DEFAULT 30 CHECK (lease_ttl_sec > 0),
    cached_response   JSONB,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE transaction_gateway_metadata (
    id             UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    transaction_id UUID        NOT NULL REFERENCES transactions(id),
    gateway_id     TEXT        NOT NULL,
    metadata       JSONB       NOT NULL,
    captured_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE audit_log (
    id             UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    transaction_id UUID        REFERENCES transactions(id),
    event_type     TEXT        NOT NULL,
    actor          TEXT        NOT NULL
                   CHECK (actor IN ('system', 'tenant', 'ops', 'gateway')),
    reason         TEXT,
    previous_state TEXT,
    new_state      TEXT        NOT NULL,
    metadata       JSONB,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_audit_log_transaction ON audit_log (transaction_id, created_at ASC);
CREATE INDEX idx_audit_log_event_type ON audit_log (event_type, created_at DESC);

CREATE TABLE webhook_events (
    event_id    TEXT        NOT NULL,
    gateway_id  TEXT        NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (event_id, gateway_id)
);

CREATE TABLE refunds (
    id                 UUID        PRIMARY KEY,
    transaction_id     UUID        NOT NULL REFERENCES transactions(id),
    amount             BIGINT      NOT NULL CHECK (amount > 0),
    reason             TEXT,
    status             TEXT        NOT NULL DEFAULT 'REFUND_INITIATED'
                       CHECK (status IN ('REFUND_INITIATED', 'REFUND_PROCESSING', 'REFUNDED', 'REFUND_FAILED')),
    initiated_by       TEXT        NOT NULL,
    gateway_refund_id  TEXT,
    version            INT         NOT NULL DEFAULT 1 CHECK (version >= 1),
    attempted_gateway  TEXT,
    actual_gateway     TEXT,
    attempts           INT         NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    failure_reason     JSONB,
    initiated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at        TIMESTAMPTZ
);

CREATE TABLE tenant_webhook_deliveries (
    id              UUID        PRIMARY KEY,
    tenant_id       UUID        NOT NULL,
    transaction_id  UUID        NOT NULL REFERENCES transactions(id),
    event_type      TEXT        NOT NULL,
    payload         JSONB       NOT NULL,
    endpoint_url    TEXT        NOT NULL,
    status          TEXT        NOT NULL DEFAULT 'PENDING'
                    CHECK (status IN ('PENDING', 'DELIVERED', 'FAILED')),
    attempts        INT         NOT NULL DEFAULT 0,
    last_attempt_at TIMESTAMPTZ,
    last_error      TEXT,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    delivered_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_transactions_tenant_status ON transactions (tenant_id, status);
CREATE INDEX idx_transactions_user ON transactions (tenant_id, user_id);
CREATE INDEX idx_transactions_gateways ON transactions (attempted_gateway, actual_gateway) WHERE actual_gateway IS NOT NULL;
CREATE INDEX idx_transactions_lease_expiry ON transactions (processing_started_at, processing_timeout) WHERE status = 'PROCESSING';
CREATE INDEX idx_transactions_gateway_reference ON transactions (gateway_reference_id) WHERE gateway_reference_id IS NOT NULL;
CREATE INDEX idx_transactions_metadata ON transactions USING GIN (metadata);
CREATE INDEX idx_transactions_gateway_amount ON transactions (tenant_id, gateway_currency) WHERE gateway_amount IS NOT NULL;
CREATE INDEX idx_idempotency_keys_expires ON idempotency_keys (expires_at);
CREATE INDEX idx_raw_metadata_transaction ON transaction_gateway_metadata (transaction_id, captured_at DESC);
CREATE INDEX idx_refunds_transaction_status ON refunds (transaction_id, status);
CREATE INDEX idx_tenant_webhook_pending ON tenant_webhook_deliveries (status, next_attempt_at) WHERE status = 'PENDING';
CREATE INDEX idx_tenant_webhook_transaction ON tenant_webhook_deliveries (transaction_id);
CREATE INDEX idx_reconciliation_jobs_gateway_status ON reconciliation_jobs (gateway_id, status, created_at DESC);
CREATE INDEX idx_reconciliation_jobs_transaction ON reconciliation_jobs (transaction_id) WHERE transaction_id IS NOT NULL;
CREATE INDEX idx_reconciliation_entries_job ON reconciliation_entries (job_id);
CREATE INDEX idx_reconciliation_entries_transaction ON reconciliation_entries (transaction_id);
CREATE INDEX idx_reconciliation_entries_unresolved ON reconciliation_entries (mismatch_type, created_at DESC) WHERE resolution_status = 'UNRESOLVED';
CREATE INDEX idx_auto_resolution_log_settlement ON settlement_auto_resolution_log (settlement_id, executed_at DESC);

-- ── Tenant gateway configs ───────────────────────────────────────────────

CREATE TABLE tenant_gateway_configs (
    tenant_id        UUID         NOT NULL,
    gateway_id       TEXT         NOT NULL,
    provider         TEXT         NOT NULL CHECK (provider IN ('stripe', 'razorpay', 'fib')),
    encrypted_config BYTEA        NOT NULL,
    config_version   INT          NOT NULL DEFAULT 1,
    is_active        BOOLEAN      NOT NULL DEFAULT true,
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, gateway_id)
);

CREATE INDEX idx_tenant_gateway_configs_tenant ON tenant_gateway_configs (tenant_id);
CREATE INDEX idx_tenant_gateway_configs_provider ON tenant_gateway_configs (provider);
CREATE INDEX idx_tenant_gateway_configs_active ON tenant_gateway_configs (tenant_id, provider) WHERE is_active = true;

-- ── Tenant currency rates (exchange rate markup) ───────────────────────

CREATE TABLE tenant_currency_rates (
    tenant_id          UUID            NOT NULL,
    from_currency      CHAR(3)         NOT NULL,
    to_currency        CHAR(3)         NOT NULL,
    rate               NUMERIC(18,8)   NOT NULL CHECK (rate > 0),
    markup_pct         NUMERIC(8,4)    NOT NULL DEFAULT 0,
    markup_fixed       NUMERIC(18,8)   NOT NULL DEFAULT 0,
    created_at         TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, from_currency, to_currency)
);

CREATE INDEX idx_tenant_currency_rates_lookup ON tenant_currency_rates (tenant_id, from_currency, to_currency);

-- ── Tenant webhook configs ──────────────────────────────────────────────

CREATE TABLE tenant_webhook_configs (
    tenant_id       UUID        PRIMARY KEY,
    endpoint_url    TEXT        NOT NULL,
    signing_secret  TEXT        NOT NULL,
    active          BOOLEAN     NOT NULL DEFAULT true,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── Disputes ─────────────────────────────────────────────────────────────

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

-- ── Notifications ────────────────────────────────────────────────────────

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

INSERT INTO notification_templates (name, subject, body_text, body_html, sms_text) VALUES
('PAYMENT_SUCCESS', 'Payment Successful', 'Your payment of {{.amount}} {{.currency}} was successful. Reference: {{.transaction_id}}', '<h1>Payment Successful</h1><p>Your payment of {{.amount}} {{.currency}} was successful.</p><p>Reference: {{.transaction_id}}</p>', 'Payment of {{.amount}}{{.currency}} successful. Ref: {{.transaction_id}}'),
('PAYMENT_FAILURE', 'Payment Failed', 'Your payment of {{.amount}} {{.currency}} has failed. Reason: {{.reason}}', '<h1>Payment Failed</h1><p>Your payment of {{.amount}} {{.currency}} has failed.</p><p>Reason: {{.reason}}</p>', 'Payment of {{.amount}}{{.currency}} failed: {{.reason}}'),
('REFUND_COMPLETED', 'Refund Completed', 'A refund of {{.amount}} {{.currency}} has been processed for transaction {{.transaction_id}}.', '<h1>Refund Completed</h1><p>A refund of {{.amount}} {{.currency}} has been processed.</p>', 'Refund of {{.amount}}{{.currency}} completed for {{.transaction_id}}'),
('REFUND_FAILED', 'Refund Failed', 'A refund for {{.amount}} {{.currency}} has failed for transaction {{.transaction_id}}. Reason: {{.reason}}', '<h1>Refund Failed</h1><p>A refund has failed.</p><p>Reason: {{.reason}}</p>', 'Refund of {{.amount}}{{.currency}} failed: {{.reason}}'),
('DISPUTE_OPENED', 'Dispute Opened', 'A dispute has been opened for transaction {{.transaction_id}}. Reason: {{.reason}}. Evidence due by {{.evidence_due_by}}.', '<h1>Dispute Opened</h1><p>A dispute has been opened for transaction {{.transaction_id}}.</p><p>Reason: {{.reason}}</p><p>Evidence due by: {{.evidence_due_by}}</p>', 'Dispute opened for {{.transaction_id}}: {{.reason}}. Due: {{.evidence_due_by}}'),
('DISPUTE_WON', 'Dispute Won', 'The dispute for transaction {{.transaction_id}} has been resolved in your favor.', '<h1>Dispute Won</h1><p>The dispute for transaction {{.transaction_id}} has been resolved in your favor.</p>', 'Dispute won for {{.transaction_id}}'),
('DISPUTE_LOST', 'Dispute Lost', 'The dispute for transaction {{.transaction_id}} has been resolved against you.', '<h1>Dispute Lost</h1><p>The dispute for transaction {{.transaction_id}} has been resolved against you.</p>', 'Dispute lost for {{.transaction_id}}');

-- ── Outbox (partitioned) ─────────────────────────────────────────────────

CREATE TABLE outbox_events (
    id               UUID        NOT NULL,
    aggregate_id     UUID        NOT NULL,
    aggregate_type   TEXT        NOT NULL,
    event_type       TEXT        NOT NULL,
    payload          JSONB       NOT NULL,
    event_version    INT         NOT NULL DEFAULT 1 CHECK (event_version >= 1),
    status           TEXT        NOT NULL DEFAULT 'PENDING'
                     CHECK (status IN ('PENDING', 'PUBLISHING', 'PUBLISHED', 'FAILED')),
    shard_index      INT         NOT NULL DEFAULT 0,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at     TIMESTAMPTZ,
    next_attempt_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    attempts         INT         NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    last_error       TEXT,
    locked_at        TIMESTAMPTZ,
    aggregate_version INT        NOT NULL DEFAULT 1,
    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);

DO $$
DECLARE
    base    TIMESTAMPTZ := (date_trunc('week', (now() AT TIME ZONE 'UTC')) AT TIME ZONE 'UTC') - INTERVAL '7 days';
    p_start TIMESTAMPTZ;
    p_end   TIMESTAMPTZ;
    p_name  TEXT;
    i       INT;
BEGIN
    FOR i IN 0..3 LOOP
        p_start := base + (i * INTERVAL '7 days');
        p_end   := p_start + INTERVAL '7 days';
        p_name  := 'outbox_' || to_char(p_start AT TIME ZONE 'UTC', 'IYYY') || '_W' || to_char(p_start AT TIME ZONE 'UTC', 'IW');
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF outbox_events FOR VALUES FROM (%L) TO (%L)',
            p_name, p_start, p_end
        );
    END LOOP;
END $$;

CREATE TABLE outbox_default PARTITION OF outbox_events DEFAULT;

CREATE TABLE outbox_dead_letters (
    id                UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    original_event_id UUID        NOT NULL,
    aggregate_id      UUID        NOT NULL,
    aggregate_type    TEXT        NOT NULL,
    event_type        TEXT        NOT NULL,
    payload           JSONB       NOT NULL,
    event_version     INT         NOT NULL DEFAULT 1,
    failure_reason    TEXT        NOT NULL,
    failed_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at       TIMESTAMPTZ,
    resolved_by       TEXT,
    aggregate_version INT         NOT NULL DEFAULT 1
);

CREATE TABLE partition_management_log (
    id             UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    partition_name TEXT        NOT NULL,
    action         TEXT        NOT NULL CHECK (action IN ('create', 'detach', 'drop')),
    wal_lag_mb     BIGINT,
    replica_lag_ms INT,
    duration_ms    INT,
    executed_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_outbox_shard ON outbox_events (shard_index, status, next_attempt_at, attempts, created_at) WHERE status IN ('PENDING', 'PUBLISHING');
CREATE INDEX idx_dead_letters_unresolved ON outbox_dead_letters (failed_at DESC) WHERE resolved_at IS NULL;
CREATE INDEX idx_dead_letters_original_event ON outbox_dead_letters (original_event_id);
CREATE INDEX idx_partition_log_name_action ON partition_management_log (partition_name, action, executed_at DESC);

-- ── Outbox notify trigger ────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION notify_outbox_insert()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify('outbox_insert', NEW.shard_index::TEXT);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_outbox_insert_notify
    AFTER INSERT ON outbox_events
    FOR EACH ROW
    EXECUTE FUNCTION notify_outbox_insert();
