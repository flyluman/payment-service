package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type LeaseRepository struct {
	db *DB
}

func NewLeaseRepository(db *DB) *LeaseRepository {
	return &LeaseRepository{db: db}
}

func (r *LeaseRepository) Acquire(ctx context.Context, leaseKey, transactionID uuid.UUID, ttlSec int) (bool, []byte, error) {
	tx, err := txFromContext(ctx)
	if err != nil {
		return false, nil, err
	}

	tag, err := tx.Exec(ctx, `INSERT INTO processing_lease (idempotency_key, payment_intent_id, lease_ttl_sec)
VALUES ($1, $2, $3)
ON CONFLICT (idempotency_key) DO NOTHING`, leaseKey, transactionID, ttlSec)
	if err != nil {
		return false, nil, fmt.Errorf("lease: acquire %s: %w", leaseKey, err)
	}
	if tag.RowsAffected() == 1 {
		return true, nil, nil
	}

	var cached []byte
	err = tx.QueryRow(ctx, `SELECT cached_response FROM processing_lease WHERE idempotency_key = $1`, leaseKey).Scan(&cached)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil, nil
	}
	if err != nil {
		return false, nil, fmt.Errorf("lease: read cached response %s: %w", leaseKey, err)
	}
	return false, cached, nil
}

func (r *LeaseRepository) WriteCachedResponse(ctx context.Context, leaseKey uuid.UUID, response []byte) error {
	tx, err := txFromContext(ctx)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `UPDATE processing_lease SET cached_response = $2 WHERE idempotency_key = $1`, leaseKey, string(response))
	if err != nil {
		return fmt.Errorf("lease: write cached response %s: %w", leaseKey, err)
	}
	return nil
}

func (r *LeaseRepository) TryAcquireDirect(ctx context.Context, leaseKey, transactionID uuid.UUID, ttlSec int) (bool, error) {
	tag, err := r.db.pool.Exec(ctx, `INSERT INTO processing_lease (idempotency_key, payment_intent_id, lease_ttl_sec)
VALUES ($1, $2, $3)
ON CONFLICT (idempotency_key) DO NOTHING`, leaseKey, transactionID, ttlSec)
	if err != nil {
		return false, fmt.Errorf("lease: try acquire direct %s: %w", leaseKey, err)
	}
	return tag.RowsAffected() == 1, nil
}
