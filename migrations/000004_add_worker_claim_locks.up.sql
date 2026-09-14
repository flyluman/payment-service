ALTER TABLE notifications ADD COLUMN locked_at TIMESTAMPTZ;

ALTER TABLE tenant_webhook_deliveries ADD COLUMN locked_at TIMESTAMPTZ;
