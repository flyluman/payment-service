package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Masterminds/squirrel"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/crownroutes/payment-service/internal/domain/transaction"
	"github.com/crownroutes/payment-service/internal/ports"
)

type TransactionRepository struct {
	db    *DB
	cache TransactionCache
}

type TransactionCache interface {
	Get(ctx context.Context, id uuid.UUID) (*transaction.Txn, error)
	Set(ctx context.Context, txn *transaction.Txn) error
	Del(ctx context.Context, id uuid.UUID) error
	GetGatewayRef(ctx context.Context, gatewayID, reference string) (uuid.UUID, error)
	SetGatewayRef(ctx context.Context, gatewayID, reference string, txnID uuid.UUID) error
}

func NewTransactionRepository(db *DB) *TransactionRepository {
	return &TransactionRepository{db: db}
}

func (r *TransactionRepository) SetCache(c TransactionCache) { r.cache = c }

var ErrNotFound = errors.New("transaction not found")
var ErrVersionConflict = errors.New("transaction version conflict")

func (r *TransactionRepository) Insert(ctx context.Context, t *transaction.Txn) error {
	tx, err := txFromContext(ctx)
	if err != nil {
		return err
	}

	methodDetails, err := json.Marshal(t.MethodDetails)
	if err != nil {
		return fmt.Errorf("transaction: marshal method_details: %w", err)
	}

	failureReason, err := json.Marshal(t.FailureReason)
	if err != nil {
		return fmt.Errorf("transaction: marshal failure_reason: %w", err)
	}

	metadata, err := json.Marshal(t.Metadata)
	if err != nil {
		return fmt.Errorf("transaction: marshal metadata: %w", err)
	}

	_, err = tx.Exec(ctx, `INSERT INTO transactions (
    id, tenant_id, user_id, amount, currency, payment_method, status, version,
    gateway_id, gateway_reference_id, gateway_idempotency_key,
    attempted_gateway, actual_gateway, original_gateway,
    estimated_timeout_seconds, failure_reason, method_details, metadata,
    description, customer_id, customer_email,
    cancel_intent, cancel_requested_by, cancel_requested_at, cancel_requested_via,
    processing_started_at, processing_timeout,
    callback_url, redirect_url, token_hash,
    gateway_fee_estimate, gateway_fee_currency, gateway_fee_model_version,
    created_at, updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8,
    $9, $10, $11,
    $12, $13, $14,
    $15, $16, $17, $18,
    $19, $20, $21,
    $22, $23, $24, $25,
    $26, $27,
    $28, $29, $30,
    $31, $32, $33,
    $34, $35
)`,
		t.ID, t.TenantID, t.UserID, t.Amount, t.Currency, t.PaymentMethod, t.Status, t.Version,
		t.GatewayID, t.GatewayReferenceID, t.GatewayIdempotencyKey,
		t.AttemptedGateway, t.ActualGateway, t.OriginalGateway,
		t.EstimatedTimeoutSeconds, string(failureReason), string(methodDetails), string(metadata),
		t.Description, t.CustomerID, t.CustomerEmail,
		t.CancelIntent, nullIfEmpty(string(t.CancelRequestedBy)), t.CancelRequestedAt, nullIfEmpty(string(t.CancelRequestedVia)),
		t.ProcessingStartedAt, t.ProcessingTimeout,
		nullIfEmpty(t.CallbackURL), nullIfEmpty(t.RedirectURL), nullIfEmpty(t.TokenHash),
		t.GatewayFeeEstimate, nullIfEmpty(t.GatewayFeeCurrency), t.GatewayFeeModelVersion,
		t.CreatedAt, t.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("transaction: insert %s: %w", t.ID, err)
	}
	return nil
}

func (r *TransactionRepository) GetByID(ctx context.Context, id uuid.UUID) (*transaction.Txn, error) {
	if r.cache != nil {
		txn, err := r.cache.Get(ctx, id)
		if err == nil && txn != nil {
			return txn, nil
		}
	}

	row := readQueryer(ctx, r.db).QueryRow(ctx, `SELECT
    id, tenant_id, user_id, amount, currency, payment_method, status, version,
    gateway_id, gateway_reference_id, gateway_idempotency_key,
    attempted_gateway, actual_gateway, original_gateway,
    estimated_timeout_seconds, failure_reason, method_details, metadata,
    description, customer_id, customer_email,
    cancel_intent, cancel_requested_by, cancel_requested_at, cancel_requested_via,
    processing_started_at, EXTRACT(EPOCH FROM processing_timeout)::double precision,
    callback_url, redirect_url, token_hash,
    gateway_fee_estimate, gateway_fee_currency, gateway_fee_model_version,
    created_at, updated_at
FROM transactions
WHERE id = $1`, id)
	txn, err := scanTransaction(row)
	if err != nil {
		return nil, err
	}

	if r.cache != nil {
		_ = r.cache.Set(ctx, txn)
	}
	return txn, nil
}

func (r *TransactionRepository) GetByGatewayReference(ctx context.Context, gatewayID, reference string) (*transaction.Txn, error) {
	// Try cache first: check if we have a known txnID for this (gatewayID, reference) pair.
	if r.cache != nil && reference != "" {
		if txnID, err := r.cache.GetGatewayRef(ctx, gatewayID, reference); err == nil && txnID != uuid.Nil {
			txn, err := r.GetByID(ctx, txnID)
			if err == nil {
				return txn, nil
			}
		}
	}

	row := readQueryer(ctx, r.db).QueryRow(ctx, `SELECT
    id, tenant_id, user_id, amount, currency, payment_method, status, version,
    gateway_id, gateway_reference_id, gateway_idempotency_key,
    attempted_gateway, actual_gateway, original_gateway,
    estimated_timeout_seconds, failure_reason, method_details, metadata,
    description, customer_id, customer_email,
    cancel_intent, cancel_requested_by, cancel_requested_at, cancel_requested_via,
    processing_started_at, EXTRACT(EPOCH FROM processing_timeout)::double precision,
    callback_url, redirect_url, token_hash,
    gateway_fee_estimate, gateway_fee_currency, gateway_fee_model_version,
    created_at, updated_at
FROM transactions
WHERE gateway_id = $1 AND gateway_reference_id = $2`, gatewayID, reference)
	t, err := scanTransaction(row)
	if errors.Is(err, ErrNotFound) {
		return nil, nil
	}
	return t, err
}

func (r *TransactionRepository) invalidateCache(ctx context.Context, id uuid.UUID) {
	if r.cache != nil {
		_ = r.cache.Del(ctx, id)
	}
}

func (r *TransactionRepository) UpdateStatus(ctx context.Context, t *transaction.Txn) error {
	defer r.invalidateCache(ctx, t.ID)
	tx, err := txFromContext(ctx)
	if err != nil {
		return err
	}

	methodDetails, err := json.Marshal(t.MethodDetails)
	if err != nil {
		return fmt.Errorf("transaction: marshal method_details: %w", err)
	}

	failureReason, err := json.Marshal(t.FailureReason)
	if err != nil {
		return fmt.Errorf("transaction: marshal failure_reason: %w", err)
	}

	var newVersion int
	err = tx.QueryRow(ctx, `UPDATE transactions SET
    status                = $1,
    version               = version + 1,
    actual_gateway        = $2,
    gateway_reference_id  = $3,
    failure_reason        = $4,
    method_details        = $5,
    processing_started_at = $6,
    processing_timeout    = $7,
    updated_at            = $8
WHERE id = $9
  AND version = $10
RETURNING version`,
		t.Status,
		t.ActualGateway,
		t.GatewayReferenceID,
		string(failureReason),
		string(methodDetails),
		t.ProcessingStartedAt,
		t.ProcessingTimeout,
		time.Now().UTC(),
		t.ID,
		t.Version,
	).Scan(&newVersion)
	if errors.Is(err, pgx.ErrNoRows) {
		var exists bool
		if e := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM transactions WHERE id = $1)`, t.ID).Scan(&exists); e != nil {
			return fmt.Errorf("transaction: disambiguate update %s: %w", t.ID, e)
		}
		if !exists {
			return ErrNotFound
		}
		return ErrVersionConflict
	}
	if err != nil {
		return fmt.Errorf("transaction: update status %s: %w", t.ID, err)
	}

	t.Version = newVersion
	return nil
}

func (r *TransactionRepository) SetCancelIntent(ctx context.Context, id uuid.UUID, by transaction.Actor, via transaction.CancelVia) (bool, error) {
	defer r.invalidateCache(ctx, id)
	tag, err := queryer(ctx, r.db.pool).Exec(ctx, `UPDATE transactions
SET
    cancel_intent        = true,
    cancel_requested_by  = $2,
    cancel_requested_at  = NOW(),
    cancel_requested_via = $3,
    updated_at           = NOW()
WHERE id = $1
  AND cancel_intent = false`, id, by, via)
	if err != nil {
		return false, fmt.Errorf("transaction: set cancel intent %s: %w", id, err)
	}
	return tag.RowsAffected() == 1, nil
}

func (r *TransactionRepository) ListExpiredLeaseIDs(ctx context.Context) ([]uuid.UUID, error) {
	rows, err := readQueryer(ctx, r.db).Query(ctx, `SELECT id FROM transactions
WHERE status = 'PROCESSING'
  AND processing_started_at IS NOT NULL
  AND processing_timeout IS NOT NULL
  AND NOW() > processing_started_at + processing_timeout`)
	if err != nil {
		return nil, fmt.Errorf("transaction: list expired lease ids: %w", err)
	}
	defer rows.Close()

	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("transaction: scan expired lease id: %w", err)
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (r *TransactionRepository) UpdateGatewayReference(ctx context.Context, id uuid.UUID, gatewayRefID string, expectedVersion int) error {
	defer r.invalidateCache(ctx, id)
	tx, err := txFromContext(ctx)
	if err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `UPDATE transactions
SET gateway_reference_id = $2, updated_at = NOW()
WHERE id = $1 AND version = $3`, id, gatewayRefID, expectedVersion)
	if err != nil {
		return fmt.Errorf("transaction: update gateway reference %s: %w", id, err)
	}
	if tag.RowsAffected() == 0 {
		var exists bool
		if e := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM transactions WHERE id = $1)`, id).Scan(&exists); e != nil {
			return fmt.Errorf("transaction: disambiguate gateway ref update %s: %w", id, e)
		}
		if !exists {
			return ErrNotFound
		}
		return ErrVersionConflict
	}
	if r.cache != nil && gatewayRefID != "" {
		var gwID string
		if e := tx.QueryRow(ctx, `SELECT gateway_id FROM transactions WHERE id = $1`, id).Scan(&gwID); e == nil && gwID != "" {
			_ = r.cache.SetGatewayRef(ctx, gwID, gatewayRefID, id)
		}
	}
	return nil
}

func scanTransaction(row pgx.Row) (*transaction.Txn, error) {
	var t transaction.Txn
	var failureReasonRaw []byte
	var methodDetailsRaw []byte
	var processingTimeoutSec *float64
	var cancelBy, cancelVia *string
	var callbackURL, redirectURL, tokenHash *string
	var gatewayFeeCurrency *string

	err := row.Scan(
		&t.ID, &t.TenantID, &t.UserID, &t.Amount, &t.Currency, &t.PaymentMethod, &t.Status, &t.Version,
		&t.GatewayID, &t.GatewayReferenceID, &t.GatewayIdempotencyKey,
		&t.AttemptedGateway, &t.ActualGateway, &t.OriginalGateway,
		&t.EstimatedTimeoutSeconds, &failureReasonRaw, &methodDetailsRaw, &t.Metadata,
		&t.Description, &t.CustomerID, &t.CustomerEmail,
		&t.CancelIntent, &cancelBy, &t.CancelRequestedAt, &cancelVia,
		&t.ProcessingStartedAt, &processingTimeoutSec,
		&callbackURL, &redirectURL, &tokenHash,
		&t.GatewayFeeEstimate, &gatewayFeeCurrency, &t.GatewayFeeModelVersion,
		&t.CreatedAt, &t.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("transaction: scan: %w", err)
	}

	if cancelBy != nil {
		t.CancelRequestedBy = transaction.Actor(*cancelBy)
	}
	if cancelVia != nil {
		t.CancelRequestedVia = transaction.CancelVia(*cancelVia)
	}
	if gatewayFeeCurrency != nil {
		t.GatewayFeeCurrency = *gatewayFeeCurrency
	}

	if len(failureReasonRaw) > 0 && string(failureReasonRaw) != "null" {
		if err := json.Unmarshal(failureReasonRaw, &t.FailureReason); err != nil {
			return nil, fmt.Errorf("transaction: unmarshal failure_reason: %w", err)
		}
	}

	if len(methodDetailsRaw) > 0 && string(methodDetailsRaw) != "null" {
		if err := json.Unmarshal(methodDetailsRaw, &t.MethodDetails); err != nil {
			return nil, fmt.Errorf("transaction: unmarshal method_details: %w", err)
		}
	}

	if processingTimeoutSec != nil {
		d := time.Duration(*processingTimeoutSec * float64(time.Second))
		t.ProcessingTimeout = &d
	}

	if callbackURL != nil {
		t.CallbackURL = *callbackURL
	}
	if redirectURL != nil {
		t.RedirectURL = *redirectURL
	}
	if tokenHash != nil {
		t.TokenHash = *tokenHash
	}

	return &t, nil
}

func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func (r *TransactionRepository) List(ctx context.Context, filter ports.TransactionFilter) (*ports.TransactionListResult, error) {
	psql := squirrel.StatementBuilder.PlaceholderFormat(squirrel.Dollar)

	// Build WHERE clause from filter
	where := squirrel.Eq{}
	if filter.TenantID != nil {
		where["tenant_id"] = *filter.TenantID
	}
	if filter.UserID != nil {
		where["user_id"] = *filter.UserID
	}
	if filter.Currency != nil {
		where["currency"] = *filter.Currency
	}
	if len(filter.Status) > 0 {
		where["status"] = filter.Status
	}
	if len(filter.GatewayID) > 0 {
		where["attempted_gateway"] = filter.GatewayID
	}
	if len(filter.PaymentMethod) > 0 {
		where["payment_method"] = filter.PaymentMethod
	}

	// Data query — fetch limit+1 to compute has_more
	limit := filter.Limit
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	dataBuilder := psql.
		Select("id", "tenant_id", "user_id", "amount", "currency", "status", "attempted_gateway", "payment_method", "created_at").
		From("transactions").
		Where(where).
		OrderBy("created_at DESC, id DESC").
		Limit(uint64(limit + 1))

	if filter.MinAmount != nil {
		dataBuilder = dataBuilder.Where("amount >= ?", *filter.MinAmount)
	}
	if filter.MaxAmount != nil {
		dataBuilder = dataBuilder.Where("amount <= ?", *filter.MaxAmount)
	}
	if filter.DateFrom != nil {
		dataBuilder = dataBuilder.Where("created_at >= ?", *filter.DateFrom)
	}
	if filter.DateTo != nil {
		dataBuilder = dataBuilder.Where("created_at <= ?", *filter.DateTo)
	}

	dataSQL, dataArgs, err := dataBuilder.ToSql()
	if err != nil {
		return nil, fmt.Errorf("build data query: %w", err)
	}

	rows, err := r.db.Pool().Query(ctx, dataSQL, dataArgs...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var txns []ports.TransactionSummary
	for rows.Next() {
		var t ports.TransactionSummary
		var gatewayID, paymentMethod *string
		if err := rows.Scan(&t.ID, &t.TenantID, &t.UserID, &t.Amount, &t.Currency,
			&t.Status, &gatewayID, &paymentMethod, &t.CreatedAt); err != nil {
			return nil, err
		}
		if gatewayID != nil {
			t.GatewayID = *gatewayID
		}
		if paymentMethod != nil {
			t.PaymentMethod = *paymentMethod
		}
		txns = append(txns, t)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	hasMore := len(txns) > limit
	var nextCursor *string
	if hasMore {
		txns = txns[:limit]
		last := txns[len(txns)-1]
		cursor := last.CreatedAt.Format(time.RFC3339Nano) + ":" + last.ID.String()
		nextCursor = &cursor
	}

	return &ports.TransactionListResult{
		Transactions: txns,
		NextCursor:   nextCursor,
		HasMore:      hasMore,
	}, nil
}
