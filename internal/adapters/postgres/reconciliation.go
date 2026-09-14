package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/crownroutes/payment-service/internal/domain/reconciliation"
	"github.com/crownroutes/payment-service/internal/ports"
)

type ReconciliationStore struct {
	db *DB
}

func NewReconciliationStore(db *DB) *ReconciliationStore {
	return &ReconciliationStore{db: db}
}

func (s *ReconciliationStore) CreateJob(ctx context.Context, job *reconciliation.Job) error {
	_, err := s.db.Pool().Exec(ctx, `INSERT INTO reconciliation_jobs
		(id, gateway_id, transaction_id, period_start, period_end, status, triggered_by, actor, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		job.ID, job.GatewayID, job.TransactionID, job.PeriodStart, job.PeriodEnd,
		string(job.Status), string(job.TriggeredBy), job.Actor, job.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("reconciliation: create job: %w", err)
	}
	return nil
}

func (s *ReconciliationStore) UpdateJob(ctx context.Context, job *reconciliation.Job) error {
	_, err := s.db.Pool().Exec(ctx, `UPDATE reconciliation_jobs SET
		status = $1, mismatch_count = $2, error = $3, started_at = $4, completed_at = $5
		WHERE id = $6`,
		string(job.Status), job.MismatchCount, job.Error, job.StartedAt, job.CompletedAt, job.ID,
	)
	if err != nil {
		return fmt.Errorf("reconciliation: update job: %w", err)
	}
	return nil
}

func (s *ReconciliationStore) GetJob(ctx context.Context, id uuid.UUID) (*reconciliation.Job, error) {
	row := s.db.Pool().QueryRow(ctx, `SELECT
		id, gateway_id, transaction_id, period_start, period_end,
		status, triggered_by, actor, mismatch_count, error,
		started_at, completed_at, created_at
		FROM reconciliation_jobs WHERE id = $1`, id)

	var job reconciliation.Job
	err := row.Scan(
		&job.ID, &job.GatewayID, &job.TransactionID, &job.PeriodStart, &job.PeriodEnd,
		&job.Status, &job.TriggeredBy, &job.Actor, &job.MismatchCount, &job.Error,
		&job.StartedAt, &job.CompletedAt, &job.CreatedAt,
	)
	if err == pgx.ErrNoRows {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("reconciliation: get job: %w", err)
	}
	return &job, nil
}

func (s *ReconciliationStore) ListJobs(ctx context.Context, filter ports.ReconciliationJobFilter) ([]*reconciliation.Job, error) {
	query := `SELECT id, gateway_id, transaction_id, period_start, period_end,
		status, triggered_by, actor, mismatch_count, error,
		started_at, completed_at, created_at
		FROM reconciliation_jobs WHERE 1=1`
	args := []any{}
	argIdx := 1

	if filter.GatewayID != nil {
		query += fmt.Sprintf(" AND gateway_id = $%d", argIdx)
		args = append(args, *filter.GatewayID)
		argIdx++
	}
	if filter.Status != nil {
		query += fmt.Sprintf(" AND status = $%d", argIdx)
		args = append(args, *filter.Status)
		argIdx++
	}
	if filter.TransactionID != nil {
		query += fmt.Sprintf(" AND transaction_id = $%d", argIdx)
		args = append(args, *filter.TransactionID)
		argIdx++
	}
	if filter.DateFrom != nil {
		query += fmt.Sprintf(" AND created_at >= $%d", argIdx)
		args = append(args, *filter.DateFrom)
		argIdx++
	}
	if filter.DateTo != nil {
		query += fmt.Sprintf(" AND created_at <= $%d", argIdx)
		args = append(args, *filter.DateTo)
		argIdx++
	}

	query += " ORDER BY created_at DESC"

	limit := filter.Limit
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	query += fmt.Sprintf(" LIMIT $%d", argIdx)
	args = append(args, limit)
	argIdx++

	if filter.Offset > 0 {
		query += fmt.Sprintf(" OFFSET $%d", argIdx)
		args = append(args, filter.Offset)
	}

	rows, err := s.db.Pool().Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("reconciliation: list jobs: %w", err)
	}
	defer rows.Close()

	var jobs []*reconciliation.Job
	for rows.Next() {
		var j reconciliation.Job
		if err := rows.Scan(
			&j.ID, &j.GatewayID, &j.TransactionID, &j.PeriodStart, &j.PeriodEnd,
			&j.Status, &j.TriggeredBy, &j.Actor, &j.MismatchCount, &j.Error,
			&j.StartedAt, &j.CompletedAt, &j.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("reconciliation: scan job: %w", err)
		}
		jobs = append(jobs, &j)
	}
	return jobs, rows.Err()
}

func (s *ReconciliationStore) InsertEntries(ctx context.Context, entries []reconciliation.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, e := range entries {
		batch.Queue(`INSERT INTO reconciliation_entries
			(id, job_id, transaction_id, internal_status, gateway_status,
			 internal_amount, gateway_amount, internal_fees, gateway_fees,
			 fx_rate_applied, fx_rate_at_settlement, fee_mismatch_reason,
			 mismatch_type, resolution_status, created_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)`,
			e.ID, e.JobID, e.TransactionID, e.InternalStatus, e.GatewayStatus,
			e.InternalAmount, e.GatewayAmount, e.InternalFees, e.GatewayFees,
			e.FXRateApplied, e.FXRateAtSettlement, e.FeeMismatchReason,
			string(e.MismatchType), string(e.ResolutionStatus), time.Now().UTC(),
		)
	}
	br := s.db.Pool().SendBatch(ctx, batch)
	defer br.Close()
	for i := 0; i < len(entries); i++ {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("reconciliation: insert entry %d: %w", i, err)
		}
	}
	return nil
}

func (s *ReconciliationStore) GetEntries(ctx context.Context, jobID uuid.UUID, filter ports.ReconciliationEntryFilter) ([]*reconciliation.Entry, error) {
	query := `SELECT id, job_id, transaction_id, internal_status, gateway_status,
		internal_amount, gateway_amount, internal_fees, gateway_fees,
		fx_rate_applied, fx_rate_at_settlement, fee_mismatch_reason,
		mismatch_type, resolution_status, resolved_by, resolved_at, notes,
		auto_resolution_action, auto_resolution_at, auto_resolution_by, created_at
		FROM reconciliation_entries WHERE job_id = $1`
	args := []any{jobID}
	argIdx := 2

	if filter.MismatchType != nil {
		query += fmt.Sprintf(" AND mismatch_type = $%d", argIdx)
		args = append(args, *filter.MismatchType)
		argIdx++
	}
	if filter.ResolutionStatus != nil {
		query += fmt.Sprintf(" AND resolution_status = $%d", argIdx)
		args = append(args, *filter.ResolutionStatus)
		argIdx++
	}

	query += " ORDER BY created_at ASC"

	limit := filter.Limit
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	query += fmt.Sprintf(" LIMIT $%d", argIdx)
	args = append(args, limit)

	rows, err := s.db.Pool().Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("reconciliation: list entries: %w", err)
	}
	defer rows.Close()

	var entries []*reconciliation.Entry
	for rows.Next() {
		var e reconciliation.Entry
		if err := rows.Scan(
			&e.ID, &e.JobID, &e.TransactionID, &e.InternalStatus, &e.GatewayStatus,
			&e.InternalAmount, &e.GatewayAmount, &e.InternalFees, &e.GatewayFees,
			&e.FXRateApplied, &e.FXRateAtSettlement, &e.FeeMismatchReason,
			&e.MismatchType, &e.ResolutionStatus, &e.ResolvedBy, &e.ResolvedAt, &e.Notes,
			&e.AutoResolutionAction, &e.AutoResolutionAt, &e.AutoResolutionBy, &e.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("reconciliation: scan entry: %w", err)
		}
		entries = append(entries, &e)
	}
	return entries, rows.Err()
}

func (s *ReconciliationStore) UpdateEntry(ctx context.Context, entry *reconciliation.Entry) error {
	_, err := s.db.Pool().Exec(ctx, `UPDATE reconciliation_entries SET
		resolution_status = $1, resolved_by = $2, resolved_at = $3, notes = $4,
		auto_resolution_action = $5, auto_resolution_at = $6, auto_resolution_by = $7
		WHERE id = $8`,
		string(entry.ResolutionStatus), entry.ResolvedBy, entry.ResolvedAt, entry.Notes,
		entry.AutoResolutionAction, entry.AutoResolutionAt, entry.AutoResolutionBy, entry.ID,
	)
	if err != nil {
		return fmt.Errorf("reconciliation: update entry: %w", err)
	}
	return nil
}

func (s *ReconciliationStore) GetPendingJobs(ctx context.Context, limit int) ([]*reconciliation.Job, error) {
	if limit <= 0 {
		limit = 5
	}
	rows, err := s.db.Pool().Query(ctx, `SELECT
		id, gateway_id, transaction_id, period_start, period_end,
		status, triggered_by, actor, mismatch_count, error,
		started_at, completed_at, created_at
		FROM reconciliation_jobs
		WHERE status = 'PENDING' AND triggered_by = 'system'
		ORDER BY created_at ASC
		LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("reconciliation: get pending jobs: %w", err)
	}
	defer rows.Close()

	var jobs []*reconciliation.Job
	for rows.Next() {
		var j reconciliation.Job
		if err := rows.Scan(
			&j.ID, &j.GatewayID, &j.TransactionID, &j.PeriodStart, &j.PeriodEnd,
			&j.Status, &j.TriggeredBy, &j.Actor, &j.MismatchCount, &j.Error,
			&j.StartedAt, &j.CompletedAt, &j.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("reconciliation: scan pending job: %w", err)
		}
		jobs = append(jobs, &j)
	}
	return jobs, rows.Err()
}

func (s *ReconciliationStore) GetAutoResolutionConfig(ctx context.Context, gatewayID, paymentMethod string) (*reconciliation.AutoResolutionConfig, error) {
	// Default auto-resolution config — could be stored in DB in future
	return &reconciliation.AutoResolutionConfig{
		PaymentMethod:    paymentMethod,
		ThresholdBPS:     10,
		AbsoluteCapPaise: 100000,
		Action:           "AUTO_INVOICED",
	}, nil
}

var _ ports.ReconciliationStore = (*ReconciliationStore)(nil)
