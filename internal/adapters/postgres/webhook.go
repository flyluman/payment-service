package postgres

import (
	"context"
	"fmt"

	"github.com/google/uuid"
)

type WebhookRepository struct {
	db *DB
}

func NewWebhookRepository(db *DB) *WebhookRepository {
	return &WebhookRepository{db: db}
}

func (r *WebhookRepository) RecordEvent(ctx context.Context, eventID, gatewayID string) (bool, error) {
	tx, err := txFromContext(ctx)
	if err != nil {
		return false, err
	}
	tag, err := tx.Exec(ctx, `INSERT INTO webhook_events (event_id, gateway_id) VALUES ($1, $2)
ON CONFLICT (event_id, gateway_id) DO NOTHING`, eventID, gatewayID)
	if err != nil {
		return false, fmt.Errorf("webhook: record event %s/%s: %w", gatewayID, eventID, err)
	}
	return tag.RowsAffected() == 1, nil
}

func (r *WebhookRepository) GetGatewayMetadata(ctx context.Context, transactionID uuid.UUID) ([]byte, error) {
	var payload string
	err := r.db.ReadPool().QueryRow(ctx,
		`SELECT metadata FROM transaction_gateway_metadata WHERE transaction_id = $1 ORDER BY id DESC LIMIT 1`,
		transactionID,
	).Scan(&payload)
	if err != nil {
		return nil, fmt.Errorf("webhook: get raw metadata for %s: %w", transactionID, err)
	}
	return []byte(payload), nil
}

func (r *WebhookRepository) InsertGatewayMetadata(ctx context.Context, transactionID uuid.UUID, gatewayID string, payload []byte) error {
	tx, err := txFromContext(ctx)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO transaction_gateway_metadata (transaction_id, gateway_id, metadata)
VALUES ($1, $2, $3)`, transactionID, gatewayID, string(payload)); err != nil {
		return fmt.Errorf("webhook: insert raw metadata for %s: %w", transactionID, err)
	}
	return nil
}
