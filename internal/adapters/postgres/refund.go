package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/crownroutes/payment-service/internal/domain/refund"
)

type RefundRepository struct {
	db *DB
}

func NewRefundRepository(db *DB) *RefundRepository {
	return &RefundRepository{db: db}
}

var ErrRefundNotFound = errors.New("refund not found")
var ErrRefundVersionConflict = errors.New("refund version conflict")

func (r *RefundRepository) Insert(ctx context.Context, rf *refund.Refund) error {
	tx, err := txFromContext(ctx)
	if err != nil {
		return err
	}

	failureReason, err := json.Marshal(rf.FailureReason)
	if err != nil {
		return fmt.Errorf("refund: marshal failure_reason: %w", err)
	}

	_, err = tx.Exec(ctx, `INSERT INTO refunds (
    id, transaction_id, amount, reason, status,
    initiated_by, gateway_refund_id, attempted_gateway, actual_gateway,
    attempts, failure_reason, initiated_at, resolved_at, version
) VALUES (
    $1, $2, $3, $4, $5,
    $6, $7, $8, $9,
    $10, $11, $12, $13, $14
)`,
		rf.ID, rf.TransactionID, rf.Amount, rf.Reason, rf.Status,
		rf.InitiatedBy, rf.GatewayRefundID, rf.AttemptedGateway, rf.ActualGateway,
		rf.Attempts, string(failureReason), rf.InitiatedAt, rf.ResolvedAt, rf.Version,
	)
	if err != nil {
		return fmt.Errorf("refund: insert %s: %w", rf.ID, err)
	}
	return nil
}

func (r *RefundRepository) GetByID(ctx context.Context, id uuid.UUID) (*refund.Refund, error) {
	row := queryer(ctx, r.db.pool).QueryRow(ctx, `SELECT
    id, transaction_id, amount, reason, status,
    initiated_by, gateway_refund_id, attempted_gateway, actual_gateway,
    attempts, failure_reason, initiated_at, resolved_at, version
FROM refunds
WHERE id = $1`, id)
	return scanRefund(row)
}

func (r *RefundRepository) SumActiveRefunds(ctx context.Context, transactionID uuid.UUID) (int64, error) {
	tx, err := txFromContext(ctx)
	if err != nil {
		return 0, err
	}

	var sum int64
	err = tx.QueryRow(ctx, `SELECT COALESCE(SUM(amount), 0)
FROM refunds
WHERE transaction_id = $1
  AND status IN ('REFUND_INITIATED', 'REFUND_PROCESSING', 'REFUNDED')`, transactionID).Scan(&sum)
	if err != nil {
		return 0, fmt.Errorf("refund: sum active refunds for transaction %s: %w", transactionID, err)
	}
	return sum, nil
}

func (r *RefundRepository) UpdateStatus(ctx context.Context, rf *refund.Refund) error {
	tx, err := txFromContext(ctx)
	if err != nil {
		return err
	}

	failureReason, err := json.Marshal(rf.FailureReason)
	if err != nil {
		return fmt.Errorf("refund: marshal failure_reason: %w", err)
	}

	var newVersion int
	err = tx.QueryRow(ctx, `UPDATE refunds SET
    status            = $1,
    version           = version + 1,
    gateway_refund_id = $2,
    actual_gateway    = $3,
    attempts          = $4,
    failure_reason    = $5,
    resolved_at       = $6,
    updated_at        = NOW()
WHERE id = $7
  AND version = $8
RETURNING version`,
		rf.Status, rf.GatewayRefundID, rf.ActualGateway,
		rf.Attempts, string(failureReason), rf.ResolvedAt,
		rf.ID, rf.Version,
	).Scan(&newVersion)
	if errors.Is(err, pgx.ErrNoRows) {
		var exists bool
		if e := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM refunds WHERE id = $1)`, rf.ID).Scan(&exists); e != nil {
			return fmt.Errorf("refund: disambiguate update %s: %w", rf.ID, e)
		}
		if !exists {
			return ErrRefundNotFound
		}
		return ErrRefundVersionConflict
	}
	if err != nil {
		return fmt.Errorf("refund: update status %s: %w", rf.ID, err)
	}
	rf.Version = newVersion
	return nil
}

func (r *RefundRepository) ExistsByReason(ctx context.Context, transactionID uuid.UUID, reason string) (bool, error) {
	var exists bool
	err := queryer(ctx, r.db.pool).QueryRow(ctx, `SELECT EXISTS(
    SELECT 1 FROM refunds WHERE transaction_id = $1 AND reason = $2
)`, transactionID, reason).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("refund: exists by reason %s/%s: %w", transactionID, reason, err)
	}
	return exists, nil
}

func (r *RefundRepository) LockParentTransaction(ctx context.Context, transactionID uuid.UUID) error {
	tx, err := txFromContext(ctx)
	if err != nil {
		return err
	}

	var id uuid.UUID
	err = tx.QueryRow(ctx, `SELECT id FROM transactions
WHERE id = $1
FOR UPDATE`, transactionID).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("refund: lock parent transaction %s: %w", transactionID, err)
	}
	return nil
}

// ClaimProcessing atomically moves a refund into REFUND_PROCESSING and bumps
// its attempt count. It is safe to run outside a transaction: the guarded
// single-row UPDATE is the concurrency gate between the relay and the reaper,
// so only one caller ever drives the gateway for a given refund.
func (r *RefundRepository) ClaimProcessing(ctx context.Context, rf *refund.Refund) (bool, error) {
	var newVersion int
	err := queryer(ctx, r.db.pool).QueryRow(ctx, `UPDATE refunds SET
    status     = 'REFUND_PROCESSING',
    version    = version + 1,
    attempts   = attempts + 1,
    updated_at = NOW()
WHERE id = $1
  AND status IN ('REFUND_INITIATED', 'REFUND_PROCESSING')
  AND version = $2
RETURNING version`, rf.ID, rf.Version).Scan(&newVersion)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("refund: claim processing %s: %w", rf.ID, err)
	}
	rf.Status = refund.StatusProcessing
	rf.Attempts++
	rf.Version = newVersion
	return true, nil
}

// ListStaleRefunds returns refund IDs stuck in a non-terminal state whose last
// update predates olderThan. Used by the refund reaper to re-drive refunds the
// relay left in-flight (ambiguous or timed-out gateway responses).
func (r *RefundRepository) ListStaleRefunds(ctx context.Context, olderThan time.Duration, maxAttempts, limit int) ([]uuid.UUID, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := r.db.pool.Query(ctx, `SELECT id FROM refunds
WHERE status IN ('REFUND_INITIATED', 'REFUND_PROCESSING')
  AND attempts < $1
  AND updated_at < NOW() - $2::interval
ORDER BY updated_at ASC
LIMIT $3`, maxAttempts, olderThan, limit)
	if err != nil {
		return nil, fmt.Errorf("refund: list stale refunds: %w", err)
	}
	defer rows.Close()

	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("refund: scan stale refund: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("refund: iterate stale refunds: %w", err)
	}
	return ids, nil
}

func scanRefund(row pgx.Row) (*refund.Refund, error) {
	var rf refund.Refund
	var failureReasonRaw []byte

	err := row.Scan(
		&rf.ID, &rf.TransactionID, &rf.Amount, &rf.Reason, &rf.Status,
		&rf.InitiatedBy, &rf.GatewayRefundID, &rf.AttemptedGateway, &rf.ActualGateway,
		&rf.Attempts, &failureReasonRaw, &rf.InitiatedAt, &rf.ResolvedAt, &rf.Version,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrRefundNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("refund: scan: %w", err)
	}

	if len(failureReasonRaw) > 0 && string(failureReasonRaw) != "null" {
		if err := json.Unmarshal(failureReasonRaw, &rf.FailureReason); err != nil {
			return nil, fmt.Errorf("refund: unmarshal failure_reason: %w", err)
		}
	}

	return &rf, nil
}
