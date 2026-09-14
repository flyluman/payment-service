ALTER TABLE reconciliation_jobs DROP CONSTRAINT IF EXISTS reconciliation_jobs_triggered_by_check;

ALTER TABLE reconciliation_jobs ADD CONSTRAINT reconciliation_jobs_triggered_by_check
    CHECK (triggered_by IN ('system', 'ops'));

DROP INDEX IF EXISTS idx_reconciliation_jobs_tenant;

ALTER TABLE reconciliation_jobs DROP COLUMN IF EXISTS tenant_id;
