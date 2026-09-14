ALTER TABLE refunds ADD COLUMN updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

CREATE INDEX idx_refunds_stale ON refunds (status, updated_at)
    WHERE status IN ('REFUND_INITIATED', 'REFUND_PROCESSING');
