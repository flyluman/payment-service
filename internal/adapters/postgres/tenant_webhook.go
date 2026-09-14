package postgres

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/crownroutes/payment-service/internal/ports"
)

// tenantWebhookClaimTTL is how long a claimed-but-undelivered webhook stays
// reserved before another worker may retry it (covers crashed workers).
const tenantWebhookClaimTTL = 5 * time.Minute

type TenantWebhookDeliveryReader struct {
	db *DB
}

func NewTenantWebhookDeliveryReader(db *DB) *TenantWebhookDeliveryReader {
	return &TenantWebhookDeliveryReader{db: db}
}

func (r *TenantWebhookDeliveryReader) ListPending(ctx context.Context, limit int) ([]ports.TenantWebhookDelivery, error) {
	claimTTLSec := int64(tenantWebhookClaimTTL.Seconds())
	rows, err := r.db.Pool().Query(ctx, `
		UPDATE tenant_webhook_deliveries d
		SET locked_at = NOW()
		FROM (
			SELECT id
			FROM tenant_webhook_deliveries
			WHERE status = 'PENDING'
			  AND next_attempt_at <= NOW()
			  AND (locked_at IS NULL OR locked_at < NOW() - make_interval(secs => $2))
			ORDER BY next_attempt_at ASC
			FOR UPDATE SKIP LOCKED
			LIMIT $1
		) AS claimed
		WHERE d.id = claimed.id
		RETURNING d.id, d.tenant_id, d.transaction_id, d.event_type,
		          d.payload, d.endpoint_url, d.attempts
	`, limit, claimTTLSec)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var deliveries []ports.TenantWebhookDelivery
	for rows.Next() {
		var d ports.TenantWebhookDelivery
		if err := rows.Scan(&d.ID, &d.TenantID, &d.TransactionID, &d.EventType, &d.Payload, &d.EndpointURL, &d.Attempts); err != nil {
			return nil, err
		}
		deliveries = append(deliveries, d)
	}
	return deliveries, rows.Err()
}

type TenantWebhookDeliveryUpdater struct {
	db          *DB
	maxAttempts int
}

func NewTenantWebhookDeliveryUpdater(db *DB) *TenantWebhookDeliveryUpdater {
	return &TenantWebhookDeliveryUpdater{db: db, maxAttempts: 10}
}

func NewTenantWebhookDeliveryUpdaterWithMaxAttempts(db *DB, maxAttempts int) *TenantWebhookDeliveryUpdater {
	if maxAttempts <= 0 {
		maxAttempts = 10
	}
	return &TenantWebhookDeliveryUpdater{db: db, maxAttempts: maxAttempts}
}

func (u *TenantWebhookDeliveryUpdater) MarkDelivered(ctx context.Context, id uuid.UUID) error {
	_, err := u.db.Pool().Exec(ctx, `
		UPDATE tenant_webhook_deliveries
		SET status = 'DELIVERED', delivered_at = NOW(), locked_at = NULL
		WHERE id = $1
	`, id)
	return err
}

func (u *TenantWebhookDeliveryUpdater) MarkFailed(ctx context.Context, id uuid.UUID, attempts int, lastError string, nextAttemptAt time.Time) error {
	_, err := u.db.Pool().Exec(ctx, `
		UPDATE tenant_webhook_deliveries
		SET status = CASE WHEN $2 >= $5 THEN 'FAILED' ELSE 'PENDING' END,
		    attempts = $2,
		    last_error = $3,
		    last_attempt_at = NOW(),
		    next_attempt_at = $4,
		    locked_at = NULL
		WHERE id = $1
	`, id, attempts, lastError, nextAttemptAt, u.maxAttempts)
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

func (s *TenantWebhookConfigStore) Upsert(ctx context.Context, cfg *ports.TenantWebhookConfig) error {
	_, err := s.db.Pool().Exec(ctx, `
		INSERT INTO tenant_webhook_configs (tenant_id, endpoint_url, signing_secret, active, updated_at)
		VALUES ($1, $2, $3, $4, NOW())
		ON CONFLICT (tenant_id) DO UPDATE
		SET endpoint_url = EXCLUDED.endpoint_url,
		    signing_secret = EXCLUDED.signing_secret,
		    active = EXCLUDED.active,
		    updated_at = NOW()
	`, cfg.TenantID, cfg.EndpointURL, cfg.SigningSecret, cfg.IsActive)
	return err
}

func (s *TenantWebhookConfigStore) List(ctx context.Context) ([]*ports.TenantWebhookConfig, error) {
	rows, err := s.db.Pool().Query(ctx, `
		SELECT tenant_id, endpoint_url, signing_secret, active
		FROM tenant_webhook_configs
		ORDER BY tenant_id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var configs []*ports.TenantWebhookConfig
	for rows.Next() {
		var cfg ports.TenantWebhookConfig
		if err := rows.Scan(&cfg.TenantID, &cfg.EndpointURL, &cfg.SigningSecret, &cfg.IsActive); err != nil {
			return nil, err
		}
		configs = append(configs, &cfg)
	}
	return configs, rows.Err()
}

func (s *TenantWebhookConfigStore) ListByTenant(ctx context.Context, tenantID uuid.UUID) ([]*ports.TenantWebhookConfig, error) {
	rows, err := s.db.Pool().Query(ctx, `
		SELECT tenant_id, endpoint_url, signing_secret, active
		FROM tenant_webhook_configs
		WHERE tenant_id = $1
		ORDER BY tenant_id
	`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var configs []*ports.TenantWebhookConfig
	for rows.Next() {
		var cfg ports.TenantWebhookConfig
		if err := rows.Scan(&cfg.TenantID, &cfg.EndpointURL, &cfg.SigningSecret, &cfg.IsActive); err != nil {
			return nil, err
		}
		configs = append(configs, &cfg)
	}
	return configs, rows.Err()
}
