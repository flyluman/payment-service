CREATE INDEX CONCURRENTLY IF NOT EXISTS processing_lease_expiry_idx ON processing_lease (lease_acquired_at);
