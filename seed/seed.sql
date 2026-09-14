-- Seed data for Payment Service — dev/testing.
-- Run AFTER migrations are applied. For development/testing.
-- Uses hardcoded UUIDs for reproducible testing.

BEGIN;

-- ── Gateway catalog ─────────────────────────────────────────────────────
-- All three gateways (Stripe, Razorpay, FIB).

INSERT INTO gateway_config (
    gateway_id, display_name, is_active,
    min_amount, max_amount,
    supported_currencies, supported_methods,
    idempotency_capable, supports_cancel, supports_partial_refund,
    priority, webhook_replay_window_sec, webhook_clock_skew_sec
) VALUES
    ('stripe', 'Stripe', true, 100, 100000000, ARRAY['USD','EUR','GBP']::TEXT[], ARRAY['card']::TEXT[], true, false, true, 100, 300, 30),
    ('razorpay', 'Razorpay', true, 100, 100000000, ARRAY['INR']::TEXT[], ARRAY['card','upi']::TEXT[], true, false, true, 100, 300, 30),
    ('fib', 'FIB (Fast Iraqi Bank)', true, 100, 100000000, ARRAY['IQD']::TEXT[], ARRAY['card']::TEXT[], false, true, false, 100, 300, 30)
ON CONFLICT (gateway_id) DO UPDATE SET
    display_name = EXCLUDED.display_name,
    is_active = EXCLUDED.is_active,
    max_amount = EXCLUDED.max_amount,
    supported_currencies = EXCLUDED.supported_currencies,
    supported_methods = EXCLUDED.supported_methods,
    idempotency_capable = EXCLUDED.idempotency_capable,
    supports_cancel = EXCLUDED.supports_cancel,
    supports_partial_refund = EXCLUDED.supports_partial_refund,
    updated_at = NOW();

-- ── Gateway timeouts ────────────────────────────────────────────────────
-- For FIB card: 30s gateway timeout + 300s buffer = 330s estimated.

INSERT INTO gateway_timeouts (gateway_id, payment_method, gateway_timeout_sec, payment_method_buffer_sec)
VALUES ('fib', 'card', 30, 300)
ON CONFLICT (gateway_id, payment_method) DO UPDATE SET
    gateway_timeout_sec = EXCLUDED.gateway_timeout_sec,
    payment_method_buffer_sec = EXCLUDED.payment_method_buffer_sec;

-- ── Gateway fee models ──────────────────────────────────────────────────
-- FIB: charges in IQD, reverse-inclusive 2.5% service fee + 500 IQD fixed.

INSERT INTO gateway_fee_models (gateway_id, payment_method, fixed_fee, percentage_bps, interchange_cap, discount_volume_threshold, charges_currency)
VALUES ('fib', 'card', 500, 250, NULL, 0, 'IQD')
ON CONFLICT (gateway_id, payment_method) DO UPDATE SET
    fixed_fee = EXCLUDED.fixed_fee,
    percentage_bps = EXCLUDED.percentage_bps,
    interchange_cap = EXCLUDED.interchange_cap,
    discount_volume_threshold = EXCLUDED.discount_volume_threshold,
    charges_currency = EXCLUDED.charges_currency;

-- ── Gateway metadata schemas ────────────────────────────────────────────
-- FIB returns: qr_code, readable_code, personal_app_link, business_app_link, corporate_app_link, valid_until.

INSERT INTO gateway_metadata_schemas (gateway_id, allowed_keys, required_keys, max_size_bytes)
VALUES (
    'fib',
    ARRAY['qr_code', 'readable_code', 'personal_app_link', 'business_app_link', 'corporate_app_link', 'valid_until']::TEXT[],
    ARRAY['qr_code', 'readable_code']::TEXT[],
    4096
) ON CONFLICT (gateway_id) DO UPDATE SET
    allowed_keys = EXCLUDED.allowed_keys,
    required_keys = EXCLUDED.required_keys,
    max_size_bytes = EXCLUDED.max_size_bytes;

-- ── Tenant gateway configs (FIB) ────────────────────────────────────────
-- Placeholders only — set client_id/client_secret from FIB portal before seeding.
-- Config stored as plaintext for dev (no ENCRYPTION_KEY needed).
-- In production, credentials must be encrypted. Never commit real secrets.

INSERT INTO tenant_gateway_configs (tenant_id, gateway_id, provider, encrypted_config, is_active)
VALUES
    ('11111111-1111-1111-1111-111111111111', 'fib', 'fib',
     decode('{"client_id":"fib-client-id-dev","client_secret":"fib-client-secret-dev","base_url":"https://fib-stage.fib.iq","callback_url":"http://localhost:8080/webhooks/gateway/fib"}', 'escape'),
     true)
ON CONFLICT (tenant_id, gateway_id) DO UPDATE SET
    provider = EXCLUDED.provider,
    encrypted_config = EXCLUDED.encrypted_config,
    is_active = EXCLUDED.is_active;

-- ── Example completed transaction ───────────────────────────────────────
-- Shows what a completed FIB payment looks like in the database.

INSERT INTO transactions (
    id, tenant_id, user_id, amount, currency, payment_method,
    status, version, gateway_id, gateway_reference_id,
    estimated_timeout_seconds, description, customer_id, customer_email,
    metadata, callback_url, redirect_url, created_at
) VALUES (
    '00000000-0000-0000-0000-000000000001',  -- transaction_id
    '00000000-0000-0000-0000-000000000001',  -- tenant_id
    '00000000-0000-0000-0000-000000000002',  -- user_id
    50000,                                    -- 50,000 IQD
    'IQD',
    'card',
    'CAPTURED',
    1,
    'fib',
    'fib_pay_demo_001',
    330,
    'Order #12345 - Premium Plan',
    '11111111-1111-1111-1111-111111111111',
    'customer@example.com',
    '{"order_id":"12345","product":"Premium Plan"}'::jsonb,
    'http://myapp.com/api/webhooks/payment',
    'https://myapp.com/order/12345/complete',
    NOW() - INTERVAL '1 hour'
) ON CONFLICT (id) DO NOTHING;

-- ── Example raw metadata for the transaction ────────────────────────────
-- FIB-specific output (QR code, app links, etc.)

INSERT INTO transaction_gateway_metadata (transaction_id, gateway_id, metadata)
VALUES (
    '00000000-0000-0000-0000-000000000001',
    'fib',
    '{
        "qr_code": "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==",
        "readable_code": "FIB-DEMO-001",
        "personal_app_link": "https://fib-sandbox.example.com/pay/personal/fib_pay_demo_001",
        "business_app_link": "https://fib-sandbox.example.com/pay/business/fib_pay_demo_001",
        "corporate_app_link": "https://fib-sandbox.example.com/pay/corporate/fib_pay_demo_001",
        "valid_until": "2026-07-31T23:59:59Z"
    }'::jsonb
) ON CONFLICT DO NOTHING;

-- ── Notification templates ──────────────────────────────────────────────
-- Default English templates for payment lifecycle events.

INSERT INTO notification_templates (name, subject, body_text, body_html, sms_text)
VALUES
('PAYMENT_SUCCESS', 'Payment Successful',
 'Your payment of {{.amount}} {{.currency}} was successful. Reference: {{.transaction_id}}',
 '<h1>Payment Successful</h1><p>Your payment of {{.amount}} {{.currency}} was successful.</p><p>Reference: {{.transaction_id}}</p>',
 'Payment of {{.amount}}{{.currency}} successful. Ref: {{.transaction_id}}'),
('PAYMENT_FAILURE', 'Payment Failed',
 'Your payment of {{.amount}} {{.currency}} has failed. Reason: {{.reason}}',
 '<h1>Payment Failed</h1><p>Your payment of {{.amount}} {{.currency}} has failed.</p><p>Reason: {{.reason}}</p>',
 'Payment of {{.amount}}{{.currency}} failed: {{.reason}}'),
('REFUND_COMPLETED', 'Refund Completed',
 'A refund of {{.amount}} {{.currency}} has been processed for transaction {{.transaction_id}}.',
 '<h1>Refund Completed</h1><p>A refund of {{.amount}} {{.currency}} has been processed for transaction {{.transaction_id}}.</p>',
 'Refund of {{.amount}}{{.currency}} completed for {{.transaction_id}}'),
('REFUND_FAILED', 'Refund Failed',
 'A refund for {{.amount}} {{.currency}} has failed for transaction {{.transaction_id}}. Reason: {{.reason}}',
 '<h1>Refund Failed</h1><p>A refund for {{.amount}} {{.currency}} has failed.</p><p>Reason: {{.reason}}</p>',
 'Refund of {{.amount}}{{.currency}} failed: {{.reason}}'),
('DISPUTE_OPENED', 'Payment Disputed',
 'A dispute has been opened for transaction {{.transaction_id}}. Reason: {{.reason}}. Evidence due by: {{.evidence_due_by}}.',
 '<h1>Payment Disputed</h1><p>A dispute has been opened for transaction {{.transaction_id}}.</p><p>Reason: {{.reason}}</p><p>Evidence due by: {{.evidence_due_by}}</p>',
 'Dispute opened for {{.transaction_id}}: {{.reason}}'),
('DISPUTE_WON', 'Dispute Won',
 'The dispute for transaction {{.transaction_id}} has been won.',
 '<h1>Dispute Won</h1><p>The dispute for transaction {{.transaction_id}} has been won.</p>',
 'Dispute won for {{.transaction_id}}'),
('DISPUTE_LOST', 'Dispute Lost',
 'The dispute for transaction {{.transaction_id}} has been lost.',
 '<h1>Dispute Lost</h1><p>The dispute for transaction {{.transaction_id}} has been lost.</p>',
 'Dispute lost for {{.transaction_id}}')
ON CONFLICT (name) DO UPDATE SET
    subject = EXCLUDED.subject,
    body_text = EXCLUDED.body_text,
    body_html = EXCLUDED.body_html,
    sms_text = EXCLUDED.sms_text;

-- ── Gateway circuit breaker state ───────────────────────────────────────
-- Start in CLOSED state (healthy).

INSERT INTO gateway_circuit_breaker_state (gateway_id, state, consecutive_failures)
VALUES
    ('stripe', 'CLOSED', 0),
    ('razorpay', 'CLOSED', 0),
    ('fib', 'CLOSED', 0)
ON CONFLICT (gateway_id) DO UPDATE SET
    state = EXCLUDED.state,
    consecutive_failures = EXCLUDED.consecutive_failures;

-- ── Currency exchange rates ──────────────────────────────────────────────
-- Example: USD→IQD with 3% markup, IQD→USD with 2% markup.

INSERT INTO tenant_currency_rates (tenant_id, from_currency, to_currency, rate, markup_pct, markup_fixed)
VALUES
    ('11111111-1111-1111-1111-111111111111', 'USD', 'IQD', 1500, 3, 0),
    ('11111111-1111-1111-1111-111111111111', 'IQD', 'USD', 0.000667, 2, 0)
ON CONFLICT (tenant_id, from_currency, to_currency) DO UPDATE SET
    rate = EXCLUDED.rate,
    markup_pct = EXCLUDED.markup_pct,
    markup_fixed = EXCLUDED.markup_fixed;

COMMIT;

-- ── Summary ─────────────────────────────────────────────────────────────
-- After running this seed:
--
-- Gateway catalog:
--   stripe  — Stripe, USD/EUR/GBP, card payments
--   razorpay — Razorpay, INR, card + UPI payments
--   fib     — FIB (Fast Iraqi Bank), IQD only, card payments, cancel supported
--
-- Tenant gateway config:
--   tenant 11111111-...-1111 → fib (edit placeholders in this file before seeding)
--
-- Example transaction:
--   id: 00000000-...-0001
--   tenant: 00000000-...-0001
--   amount: 50,000 IQD
--   status: CAPTURED
--   gateway_ref: fib_pay_demo_001
--   raw_metadata: QR code + app links
--
-- Auth tokens (set in env):
--   SERVICE_TOKENS=dev-service-token=00000000-0000-0000-0000-000000000001:00000000-0000-0000-0000-000000000002
--   OPS_TOKENS=dev-ops-token
