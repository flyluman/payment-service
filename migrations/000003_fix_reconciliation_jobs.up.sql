-- Reconciliation batch jobs are tenant-scoped; a missing CHECK fix that
-- rejected service-created jobs would have been broken end-to-end.

ALTER TABLE reconciliation_jobs ADD COLUMN tenant_id UUID;

CREATE INDEX idx_reconciliation_jobs_tenant ON reconciliation_jobs (tenant_id);

ALTER TABLE reconciliation_jobs DROP CONSTRAINT IF EXISTS reconciliation_jobs_triggered_by_check;

ALTER TABLE reconciliation_jobs ADD CONSTRAINT reconciliation_jobs_triggered_by_check
    CHECK (triggered_by = 'system' OR triggered_by = 'ops' OR triggered_by LIKE 'service:%');
