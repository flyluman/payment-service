package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

type IdempotencyRepository struct {
	db *DB
}

func NewIdempotencyRepository(db *DB) *IdempotencyRepository {
	return &IdempotencyRepository{db: db}
}

func (r *IdempotencyRepository) Reserve(ctx context.Context, compositeHash, requestHash string) (bool, error) {
	tag, err := queryer(ctx, r.db.pool).Exec(ctx, `INSERT INTO idempotency_keys (composite_key_hash, request_hash, response, status)
VALUES ($1, $2, '{}'::jsonb, 'PROCESSING')
ON CONFLICT (composite_key_hash) DO NOTHING`, compositeHash, requestHash)
	if err != nil {
		return false, fmt.Errorf("idempotency: reserve %s: %w", compositeHash, err)
	}
	return tag.RowsAffected() == 1, nil
}

func (r *IdempotencyRepository) Lookup(ctx context.Context, compositeHash string) (found bool, requestHash, status string, response []byte, err error) {
	err = queryer(ctx, r.db.pool).QueryRow(ctx, `SELECT request_hash, status, response
FROM idempotency_keys
WHERE composite_key_hash = $1`, compositeHash).Scan(&requestHash, &status, &response)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, "", "", nil, nil
	}
	if err != nil {
		return false, "", "", nil, fmt.Errorf("idempotency: lookup %s: %w", compositeHash, err)
	}
	return true, requestHash, status, response, nil
}

func (r *IdempotencyRepository) Complete(ctx context.Context, compositeHash string, response []byte) error {
	if _, err := queryer(ctx, r.db.pool).Exec(ctx, `UPDATE idempotency_keys
SET response = $2, status = 'COMPLETED'
WHERE composite_key_hash = $1`, compositeHash, string(response)); err != nil {
		return fmt.Errorf("idempotency: complete %s: %w", compositeHash, err)
	}
	return nil
}

func (r *IdempotencyRepository) Release(ctx context.Context, compositeHash string) error {
	if _, err := queryer(ctx, r.db.pool).Exec(ctx, `DELETE FROM idempotency_keys WHERE composite_key_hash = $1`, compositeHash); err != nil {
		return fmt.Errorf("idempotency: release %s: %w", compositeHash, err)
	}
	return nil
}

func (r *IdempotencyRepository) SweepStaleProcessing(ctx context.Context, olderThan time.Duration) (int64, error) {
	tag, err := r.db.pool.Exec(ctx, `DELETE FROM idempotency_keys
WHERE status = 'PROCESSING'
  AND created_at < NOW() - ($1 * INTERVAL '1 second')`, int64(olderThan.Seconds()))
	if err != nil {
		return 0, fmt.Errorf("idempotency: sweep stale processing: %w", err)
	}
	return tag.RowsAffected(), nil
}

func (r *IdempotencyRepository) DeleteExpired(ctx context.Context) (int64, error) {
	tag, err := r.db.pool.Exec(ctx, `DELETE FROM idempotency_keys WHERE expires_at < NOW()`)
	if err != nil {
		return 0, fmt.Errorf("idempotency: delete expired: %w", err)
	}
	return tag.RowsAffected(), nil
}
