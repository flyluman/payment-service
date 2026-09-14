package reconciliation

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/domain/reconciliation"
	"github.com/crownroutes/payment-service/internal/domain/transaction"
	"github.com/crownroutes/payment-service/internal/ports"
)

type TransactionReader interface {
	GetByID(ctx context.Context, id uuid.UUID) (*transaction.Txn, error)
}

type TransactionLister interface {
	List(ctx context.Context, filter ports.TransactionFilter) (*ports.TransactionListResult, error)
}

type GatewayRegistry interface {
	SettlementFetcher(gatewayID string) (ports.SettlementReportFetcher, bool)
}

type Service struct {
	store     ports.ReconciliationStore
	txnRepo   TransactionReader
	txnLister TransactionLister
	registry  GatewayRegistry
	log       ports.Logger
	metrics   ports.MetricRecorder
}

func NewService(
	store ports.ReconciliationStore,
	txnRepo TransactionReader,
	txnLister TransactionLister,
	registry GatewayRegistry,
	log ports.Logger,
	metrics ports.MetricRecorder,
) *Service {
	return &Service{
		store:     store,
		txnRepo:   txnRepo,
		txnLister: txnLister,
		registry:  registry,
		log:       log,
		metrics:   metrics,
	}
}

func (s *Service) CreateJob(ctx context.Context, gatewayID string, periodStart, periodEnd *time.Time, transactionID *uuid.UUID, triggeredBy string) (*reconciliation.Job, error) {
	job := &reconciliation.Job{
		ID:            uuid.Must(uuid.NewV7()),
		GatewayID:     gatewayID,
		TransactionID: transactionID,
		PeriodStart:   periodStart,
		PeriodEnd:     periodEnd,
		Status:        reconciliation.JobStatusPending,
		TriggeredBy:   reconciliation.TriggerSource(triggeredBy),
		Actor:         triggeredBy,
		CreatedAt:     time.Now().UTC(),
	}
	if err := s.store.CreateJob(ctx, job); err != nil {
		return nil, err
	}
	return job, nil
}

func (s *Service) RunJob(ctx context.Context, jobID uuid.UUID) error {
	job, err := s.store.GetJob(ctx, jobID)
	if err != nil {
		return fmt.Errorf("reconciliation: load job: %w", err)
	}
	if job.Status != reconciliation.JobStatusPending {
		return nil
	}

	now := time.Now().UTC()
	job.Status = reconciliation.JobStatusRunning
	job.StartedAt = &now
	if err := s.store.UpdateJob(ctx, job); err != nil {
		return fmt.Errorf("reconciliation: mark running: %w", err)
	}

	s.log.Info("reconciliation.started", map[string]any{
		"job_id":     job.ID.String(),
		"gateway_id": job.GatewayID,
	})

	var entries []reconciliation.Entry

	if job.TransactionID != nil {
		entries, err = s.reconcileSingle(ctx, job)
	} else if job.PeriodStart != nil && job.PeriodEnd != nil {
		entries, err = s.reconcileBatch(ctx, job)
	} else {
		err = fmt.Errorf("job has neither transaction_id nor period range")
	}

	if err != nil {
		now = time.Now().UTC()
		job.Status = reconciliation.JobStatusFailed
		job.Error = err.Error()
		job.CompletedAt = &now
		_ = s.store.UpdateJob(ctx, job)
		return fmt.Errorf("reconciliation: run job: %w", err)
	}

	if len(entries) > 0 {
		if err := s.store.InsertEntries(ctx, entries); err != nil {
			return fmt.Errorf("reconciliation: insert entries: %w", err)
		}
	}

	job.MismatchCount = len(entries)
	now = time.Now().UTC()
	job.Status = reconciliation.JobStatusCompleted
	job.CompletedAt = &now
	if err := s.store.UpdateJob(ctx, job); err != nil {
		return fmt.Errorf("reconciliation: mark completed: %w", err)
	}

	s.log.Info("reconciliation.completed", map[string]any{
		"job_id":         job.ID.String(),
		"mismatch_count": job.MismatchCount,
	})

	s.metrics.Increment("reconciliation.job_completed", map[string]string{"gateway_id": job.GatewayID})

	return nil
}

func (s *Service) reconcileSingle(ctx context.Context, job *reconciliation.Job) ([]reconciliation.Entry, error) {
	txn, err := s.txnRepo.GetByID(ctx, *job.TransactionID)
	if err != nil {
		return nil, fmt.Errorf("load transaction: %w", err)
	}

	fetcher, ok := s.registry.SettlementFetcher(txn.GatewayID)
	if !ok {
		return nil, fmt.Errorf("no settlement fetcher for gateway %s", txn.GatewayID)
	}

	// Use transaction's CreatedAt ±24h as the settlement query window.
	windowStart := txn.CreatedAt.Add(-24 * time.Hour)
	windowEnd := txn.CreatedAt.Add(24 * time.Hour)

	report, err := fetcher.FetchSettlementReport(ctx, txn.TenantID, txn.GatewayID, windowStart, windowEnd)
	if err != nil {
		return nil, fmt.Errorf("fetch settlement: %w", err)
	}

	var entries []reconciliation.Entry
	foundInReport := false
	for _, se := range report.Entries {
		if se.TransactionRef != txn.GatewayReferenceID {
			continue
		}
		foundInReport = true
		entry := s.compareTransaction(txn, &se, job.ID)
		if entry != nil {
			entries = append(entries, *entry)
		}
	}

	if !foundInReport {
		entry := reconciliation.Entry{
			ID:               uuid.Must(uuid.NewV7()),
			JobID:            job.ID,
			TransactionID:    txn.ID,
			InternalStatus:   string(txn.Status),
			GatewayStatus:    "NOT_FOUND_IN_REPORT",
			InternalAmount:   txn.Amount,
			GatewayAmount:    0,
			MismatchType:     reconciliation.MismatchMissingGateway,
			ResolutionStatus: reconciliation.ResolutionUnresolved,
		}
		entries = append(entries, entry)
	}

	return entries, nil
}

func (s *Service) reconcileBatch(ctx context.Context, job *reconciliation.Job) ([]reconciliation.Entry, error) {
	fetcher, ok := s.registry.SettlementFetcher(job.GatewayID)
	if !ok {
		return nil, fmt.Errorf("no settlement fetcher for gateway %s", job.GatewayID)
	}

	report, err := fetcher.FetchSettlementReport(ctx, uuid.Nil, job.GatewayID, *job.PeriodStart, *job.PeriodEnd)
	if err != nil {
		return nil, fmt.Errorf("fetch settlement: %w", err)
	}

	// Fetch all internal transactions for this gateway in the period.
	gw := job.GatewayID
	filter := ports.TransactionFilter{
		GatewayID: []string{gw},
		DateFrom:  job.PeriodStart,
		DateTo:    job.PeriodEnd,
		Limit:     10000,
	}
	result, err := s.txnLister.List(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("list internal transactions: %w", err)
	}

	// Index internal transactions by gateway_reference_id for fast lookup.
	type txnRef struct {
		txn          *ports.TransactionSummary
		reconciled   bool
	}
	internalByRef := make(map[string]*txnRef, len(result.Transactions))
	for i := range result.Transactions {
		// We need the full txn to get GatewayReferenceID; Summary doesn't have it.
		// Fall back to fetching each individually if needed.
		t := &result.Transactions[i]
		internalByRef[t.ID.String()] = &txnRef{txn: t}
	}

	// Fetch full transactions to get GatewayReferenceID.
	var entries []reconciliation.Entry
	matchedInternal := make(map[uuid.UUID]bool)

	for _, se := range report.Entries {
		// Try to find matching internal transaction by ref.
		found := false
		for id, ref := range internalByRef {
			uid, _ := uuid.Parse(id)
			fullTxn, err := s.txnRepo.GetByID(ctx, uid)
			if err != nil {
				continue
			}
			if fullTxn.GatewayReferenceID == se.TransactionRef {
				entry := s.compareTransactionFull(fullTxn, &se, job.ID)
				if entry != nil {
					entries = append(entries, *entry)
				}
				matchedInternal[uid] = true
				ref.reconciled = true
				found = true
				break
			}
		}
		if !found {
			// Gateway entry with no matching internal transaction.
			entries = append(entries, reconciliation.Entry{
				ID:                 uuid.Must(uuid.NewV7()),
				JobID:              job.ID,
				TransactionID:      uuid.Nil,
				InternalStatus:     "",
				GatewayStatus:      se.GatewayStatus,
				InternalAmount:     0,
				GatewayAmount:      se.GatewayAmount,
				GatewayFees:        se.GatewayFees,
				FXRateAtSettlement: se.FXRate,
				MismatchType:       reconciliation.MismatchMissingInternal,
				ResolutionStatus:   reconciliation.ResolutionUnresolved,
			})
		}
	}

	// Check for internal transactions not found in gateway report.
	for id, ref := range internalByRef {
		if ref.reconciled {
			continue
		}
		uid, _ := uuid.Parse(id)
		if matchedInternal[uid] {
			continue
		}
		entries = append(entries, reconciliation.Entry{
			ID:               uuid.Must(uuid.NewV7()),
			JobID:            job.ID,
			TransactionID:    uid,
			InternalStatus:   ref.txn.Status,
			GatewayStatus:    "NOT_FOUND_IN_REPORT",
			InternalAmount:   ref.txn.Amount,
			GatewayAmount:    0,
			MismatchType:     reconciliation.MismatchMissingGateway,
			ResolutionStatus: reconciliation.ResolutionUnresolved,
		})
	}

	return entries, nil
}

func (s *Service) compareTransaction(txn *transaction.Txn, se *ports.SettlementEntry, jobID uuid.UUID) *reconciliation.Entry {
	entry := &reconciliation.Entry{
		ID:                 uuid.Must(uuid.NewV7()),
		JobID:              jobID,
		TransactionID:      txn.ID,
		InternalStatus:     string(txn.Status),
		GatewayStatus:      se.GatewayStatus,
		InternalAmount:     txn.Amount,
		GatewayAmount:      se.GatewayAmount,
		InternalFees:       nil,
		GatewayFees:        se.GatewayFees,
		FXRateApplied:      nil,
		FXRateAtSettlement: se.FXRate,
		ResolutionStatus:   reconciliation.ResolutionUnresolved,
	}

	if txn.FeeBreakdown != nil && txn.FeeBreakdown.Fees.Total > 0 {
		fee := txn.FeeBreakdown.Fees.Total
		entry.InternalFees = &fee
	}

	if isInternalSuccess(txn.Status) && se.GatewayStatus != "succeeded" {
		entry.MismatchType = reconciliation.MismatchStatus
	} else if !isInternalSuccess(txn.Status) && se.GatewayStatus == "succeeded" {
		entry.MismatchType = reconciliation.MismatchStatus
	} else if txn.Amount != se.GatewayAmount {
		entry.MismatchType = reconciliation.MismatchAmount
	} else if entry.InternalFees != nil && se.GatewayFees != nil && *entry.InternalFees != *se.GatewayFees {
		entry.MismatchType = reconciliation.MismatchFee
	} else {
		return nil
	}

	return entry
}

// compareTransactionFull is like compareTransaction but works with TransactionSummary.
func (s *Service) compareTransactionFull(txn *transaction.Txn, se *ports.SettlementEntry, jobID uuid.UUID) *reconciliation.Entry {
	return s.compareTransaction(txn, se, jobID)
}

func (s *Service) GetJob(ctx context.Context, id uuid.UUID) (*reconciliation.Job, error) {
	return s.store.GetJob(ctx, id)
}

func (s *Service) ListJobs(ctx context.Context, filter ports.ReconciliationJobFilter) ([]*reconciliation.Job, error) {
	return s.store.ListJobs(ctx, filter)
}

func (s *Service) GetEntries(ctx context.Context, jobID uuid.UUID, filter ports.ReconciliationEntryFilter) ([]*reconciliation.Entry, error) {
	return s.store.GetEntries(ctx, jobID, filter)
}

func (s *Service) ResolveEntry(ctx context.Context, entryID uuid.UUID, resolvedBy, notes string) error {
	now := time.Now().UTC()
	entry := &reconciliation.Entry{
		ID:               entryID,
		ResolutionStatus: reconciliation.ResolutionResolved,
		ResolvedBy:       resolvedBy,
		ResolvedAt:       &now,
		Notes:            notes,
	}
	return s.store.UpdateEntry(ctx, entry)
}

// feeDiscrepancy returns the absolute difference between internal and gateway fees, or 0 if either is nil.
func feeDiscrepancy(internal, gateway *int64) int64 {
	if internal == nil || gateway == nil {
		return 0
	}
	d := *internal - *gateway
	if d < 0 {
		d = -d
	}
	return d
}

// roundUp rounds a float64 up to the nearest integer.
func roundUp(f float64) int64 {
	return int64(math.Ceil(f))
}

func isInternalSuccess(s transaction.Status) bool {
	switch s {
	case transaction.StatusCaptured, transaction.StatusSettled:
		return true
	}
	return false
}
