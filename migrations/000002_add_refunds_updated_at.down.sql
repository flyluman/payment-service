DROP INDEX IF EXISTS idx_refunds_stale;

ALTER TABLE refunds DROP COLUMN IF EXISTS updated_at;
