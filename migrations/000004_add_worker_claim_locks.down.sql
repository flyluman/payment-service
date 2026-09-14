ALTER TABLE tenant_webhook_deliveries DROP COLUMN locked_at;

ALTER TABLE notifications DROP COLUMN locked_at;
