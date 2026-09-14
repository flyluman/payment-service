package postgres

import (
	"context"
	"strconv"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/crownroutes/payment-service/internal/domain/dispute"
	"github.com/crownroutes/payment-service/internal/ports"
)

type DisputeStore struct {
	db *DB
}

func NewDisputeStore(db *DB) *DisputeStore {
	return &DisputeStore{db: db}
}

func (s *DisputeStore) Create(ctx context.Context, d *dispute.Dispute) error {
	_, err := s.db.Pool().Exec(ctx, `
		INSERT INTO disputes (id, transaction_id, gateway_id, gateway_dispute_id, reason, status, amount, currency, evidence_due_by, evidence_submitted_at, created_at, resolved_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
	`, d.ID, d.TransactionID, d.GatewayID, d.GatewayDisputeID, d.Reason, string(d.Status),
		d.Amount, d.Currency, d.EvidenceDueBy, d.EvidenceSubmittedAt, d.CreatedAt, d.ResolvedAt)
	return err
}

func (s *DisputeStore) GetByID(ctx context.Context, id uuid.UUID) (*dispute.Dispute, error) {
	row := s.db.Pool().QueryRow(ctx, `
		SELECT id, transaction_id, gateway_id, gateway_dispute_id, reason, status, amount, currency,
		       evidence_due_by, evidence_submitted_at, created_at, resolved_at
		FROM disputes WHERE id = $1
	`, id)
	return scanDispute(row)
}

func (s *DisputeStore) GetByGatewayDisputeID(ctx context.Context, gatewayID, gatewayDisputeID string) (*dispute.Dispute, error) {
	row := s.db.Pool().QueryRow(ctx, `
		SELECT id, transaction_id, gateway_id, gateway_dispute_id, reason, status, amount, currency,
		       evidence_due_by, evidence_submitted_at, created_at, resolved_at
		FROM disputes WHERE gateway_id = $1 AND gateway_dispute_id = $2
	`, gatewayID, gatewayDisputeID)
	return scanDispute(row)
}

func (s *DisputeStore) Update(ctx context.Context, d *dispute.Dispute) error {
	_, err := s.db.Pool().Exec(ctx, `
		UPDATE disputes
		SET reason = $2, status = $3, evidence_due_by = $4, evidence_submitted_at = $5, resolved_at = $6
		WHERE id = $1
	`, d.ID, d.Reason, string(d.Status), d.EvidenceDueBy, d.EvidenceSubmittedAt, d.ResolvedAt)
	return err
}

func (s *DisputeStore) List(ctx context.Context, filters ports.DisputeFilters) ([]*dispute.Dispute, error) {
	query := `
		SELECT id, transaction_id, gateway_id, gateway_dispute_id, reason, status, amount, currency,
		       evidence_due_by, evidence_submitted_at, created_at, resolved_at
		FROM disputes WHERE 1=1
	`
	args := []any{}
	argIdx := 1

	if filters.Status != nil {
		query += " AND status = $" + strconv.Itoa(argIdx)
		args = append(args, string(*filters.Status))
		argIdx++
	}
	if filters.GatewayID != nil {
		query += " AND gateway_id = $" + strconv.Itoa(argIdx)
		args = append(args, *filters.GatewayID)
		argIdx++
	}
	if filters.DateFrom != nil {
		query += " AND created_at >= $" + strconv.Itoa(argIdx)
		args = append(args, *filters.DateFrom)
		argIdx++
	}
	if filters.DateTo != nil {
		query += " AND created_at <= $" + strconv.Itoa(argIdx)
		args = append(args, *filters.DateTo)
		argIdx++
	}

	query += " ORDER BY created_at DESC"

	if filters.Limit > 0 {
		query += " LIMIT $" + strconv.Itoa(argIdx)
		args = append(args, filters.Limit)
		argIdx++
	}
	if filters.Offset > 0 {
		query += " OFFSET $" + strconv.Itoa(argIdx)
		args = append(args, filters.Offset)
		argIdx++
	}

	rows, err := s.db.Pool().Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var disputes []*dispute.Dispute
	for rows.Next() {
		d, err := scanDisputeRow(rows)
		if err != nil {
			return nil, err
		}
		disputes = append(disputes, d)
	}
	return disputes, rows.Err()
}

type DisputeEvidenceStore struct {
	db *DB
}

func NewDisputeEvidenceStore(db *DB) *DisputeEvidenceStore {
	return &DisputeEvidenceStore{db: db}
}

func (s *DisputeEvidenceStore) Add(ctx context.Context, e *dispute.Evidence) error {
	_, err := s.db.Pool().Exec(ctx, `
		INSERT INTO dispute_evidence (id, dispute_id, evidence_type, file_url, notes, submitted_at)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, e.ID, e.DisputeID, string(e.Type), e.FileURL, e.Notes, e.SubmittedAt)
	return err
}

func (s *DisputeEvidenceStore) ListByDisputeID(ctx context.Context, disputeID uuid.UUID) ([]*dispute.Evidence, error) {
	rows, err := s.db.Pool().Query(ctx, `
		SELECT id, dispute_id, evidence_type, file_url, notes, submitted_at
		FROM dispute_evidence WHERE dispute_id = $1 ORDER BY submitted_at ASC
	`, disputeID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var evidence []*dispute.Evidence
	for rows.Next() {
		e, err := scanEvidenceRow(rows)
		if err != nil {
			return nil, err
		}
		evidence = append(evidence, e)
	}
	return evidence, rows.Err()
}

type disputeRow interface {
	Scan(dest ...any) error
}

func scanDispute(row disputeRow) (*dispute.Dispute, error) {
	var d dispute.Dispute
	var status string
	err := row.Scan(&d.ID, &d.TransactionID, &d.GatewayID, &d.GatewayDisputeID, &d.Reason,
		&status, &d.Amount, &d.Currency, &d.EvidenceDueBy, &d.EvidenceSubmittedAt,
		&d.CreatedAt, &d.ResolvedAt)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, dispute.ErrNotFound
		}
		return nil, err
	}
	d.Status = dispute.Status(status)
	return &d, nil
}

func scanDisputeRow(rows pgx.Rows) (*dispute.Dispute, error) {
	return scanDispute(rows)
}

func scanEvidenceRow(rows pgx.Rows) (*dispute.Evidence, error) {
	var e dispute.Evidence
	var etype string
	err := rows.Scan(&e.ID, &e.DisputeID, &etype, &e.FileURL, &e.Notes, &e.SubmittedAt)
	if err != nil {
		return nil, err
	}
	e.Type = dispute.EvidenceType(etype)
	return &e, nil
}
