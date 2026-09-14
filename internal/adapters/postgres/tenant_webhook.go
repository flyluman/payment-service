package postgres

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/crownroutes/payment-service/internal/ports"
)

type TenantWebhookDeliveryReader struct {
	db *DB
}

func NewTenantWebhookDeliveryReader(db *DB) *TenantWebhookDeliveryReader {
	return &TenantWebhookDeliveryReader{db: db}
}

func (r *TenantWebhookDeliveryReader) ListPending(ctx context.Context, limit int) ([]ports.TenantWebhookDelivery, error) {
	rows, err := r.db.Pool().Query(ctx, `
		SELECT id, tenant_id, transaction_id, event_type, payload, endpoint_url
		FROM tenant_webhook_deliveries
		WHERE status = 'PENDING' AND next_attempt_at <= NOW()
		ORDER BY next_attempt_at ASC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var deliveries []ports.TenantWebhookDelivery
	for rows.Next() {
		var d ports.TenantWebhookDelivery
		if err := rows.Scan(&d.ID, &d.TenantID, &d.TransactionID, &d.EventType, &d.Payload, &d.EndpointURL); err != nil {
			return nil, err
		}
		deliveries = append(deliveries, d)
	}
	return deliveries, rows.Err()
}

type TenantWebhookDeliveryUpdater struct {
	db *DB
}

func NewTenantWebhookDeliveryUpdater(db *DB) *TenantWebhookDeliveryUpdater {
	return &TenantWebhookDeliveryUpdater{db: db}
}

func (u *TenantWebhookDeliveryUpdater) MarkDelivered(ctx context.Context, id uuid.UUID) error {
	_, err := u.db.Pool().Exec(ctx, `
		UPDATE tenant_webhook_deliveries
		SET status = 'DELIVERED', delivered_at = NOW()
		WHERE id = $1
	`, id)
	return err
}

func (u *TenantWebhookDeliveryUpdater) MarkFailed(ctx context.Context, id uuid.UUID, attempts int, lastError string, nextAttemptAt time.Time) error {
	_, err := u.db.Pool().Exec(ctx, `
		UPDATE tenant_webhook_deliveries
		SET status = CASE WHEN $2 >= 10 THEN 'FAILED' ELSE 'PENDING' END,
		    attempts = $2,
		    last_error = $3,
		    last_attempt_at = NOW(),
		    next_attempt_at = $4
		WHERE id = $1
	`, id, attempts, lastError, nextAttemptAt)
	return err
}

type TenantWebhookConfigStore struct {
	db *DB
}

func NewTenantWebhookConfigStore(db *DB) *TenantWebhookConfigStore {
	return &TenantWebhookConfigStore{db: db}
}

func (s *TenantWebhookConfigStore) Get(ctx context.Context, tenantID uuid.UUID) (*ports.TenantWebhookConfig, error) {
	row := s.db.Pool().QueryRow(ctx, `
		SELECT tenant_id, endpoint_url, signing_secret, active
		FROM tenant_webhook_configs
		WHERE tenant_id = $1
	`, tenantID)

	var cfg ports.TenantWebhookConfig
	err := row.Scan(&cfg.TenantID, &cfg.EndpointURL, &cfg.SigningSecret, &cfg.IsActive)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &cfg, nil
}
