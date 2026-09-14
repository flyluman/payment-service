package reconciliation

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/domain/reconciliation"
	"github.com/crownroutes/payment-service/internal/domain/transaction"
	"github.com/crownroutes/payment-service/internal/ports"
)

type TransactionReader interface {
	GetByID(ctx context.Context, id uuid.UUID) (*transaction.Txn, error)
	GetByIDs(ctx context.Context, ids []uuid.UUID) ([]*transaction.Txn, error)
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

func (s *Service) CreateJob(ctx context.Context, gatewayID string, tenantID *uuid.UUID, periodStart, periodEnd *time.Time, transactionID *uuid.UUID, triggeredBy string) (*reconciliation.Job, error) {
	job := &reconciliation.Job{
		ID:            uuid.Must(uuid.NewV7()),
		GatewayID:     gatewayID,
		TenantID:      tenantID,
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

func (s *Service) run(ctx context.Context, job *reconciliation.Job) error {
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
	var err error

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
			InternalAmount:   settledAmountFor(txn),
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
	if job.TenantID == nil {
		return nil, fmt.Errorf("batch job %s has no tenant_id; tenant-scoped settlement is required", job.ID)
	}

	report, err := fetcher.FetchSettlementReport(ctx, *job.TenantID, job.GatewayID, *job.PeriodStart, *job.PeriodEnd)
	if err != nil {
		return nil, fmt.Errorf("fetch settlement: %w", err)
	}

	// Load all internal transactions for this tenant+gateway in the period,
	// paging through the cursor until the store's page cap is exhausted.
	filter := ports.TransactionFilter{
		TenantID:  job.TenantID,
		GatewayID: []string{job.GatewayID},
		DateFrom:  job.PeriodStart,
		DateTo:    job.PeriodEnd,
		Limit:     100,
	}

	var summaries []ports.TransactionSummary
	for {
		result, err := s.txnLister.List(ctx, filter)
		if err != nil {
			return nil, fmt.Errorf("list internal transactions: %w", err)
		}
		summaries = append(summaries, result.Transactions...)
		if !result.HasMore || result.NextCursor == nil {
			break
		}
		filter.Cursor = result.NextCursor
	}

	fullTxns, err := s.txnRepo.GetByIDs(ctx, idsFromSummary(summaries))
	if err != nil {
		return nil, fmt.Errorf("load full transactions: %w", err)
	}

	internalByRef := make(map[string]*transaction.Txn, len(fullTxns))
	for _, t := range fullTxns {
		if t.GatewayReferenceID != "" {
			internalByRef[t.GatewayReferenceID] = t
		}
	}

	var entries []reconciliation.Entry
	matchedInternal := make(map[uuid.UUID]bool)

	for _, se := range report.Entries {
		fullTxn, found := internalByRef[se.TransactionRef]
		if found {
			entry := s.compareTransaction(fullTxn, &se, job.ID)
			if entry != nil {
				entries = append(entries, *entry)
			}
			matchedInternal[fullTxn.ID] = true
			continue
		}
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

	// Check for internal transactions not found in gateway report.
	for _, t := range fullTxns {
		if matchedInternal[t.ID] {
			continue
		}
		entries = append(entries, reconciliation.Entry{
			ID:               uuid.Must(uuid.NewV7()),
			JobID:            job.ID,
			TransactionID:    t.ID,
			InternalStatus:   string(t.Status),
			GatewayStatus:    "NOT_FOUND_IN_REPORT",
			InternalAmount:   settledAmountFor(t),
			GatewayAmount:    0,
			MismatchType:     reconciliation.MismatchMissingGateway,
			ResolutionStatus: reconciliation.ResolutionUnresolved,
		})
	}

	return entries, nil
}

// idsFromSummary extracts the transaction ids from a list result.
func idsFromSummary(txns []ports.TransactionSummary) []uuid.UUID {
	ids := make([]uuid.UUID, 0, len(txns))
	for _, t := range txns {
		ids = append(ids, t.ID)
	}
	return ids
}

func (s *Service) compareTransaction(txn *transaction.Txn, se *ports.SettlementEntry, jobID uuid.UUID) *reconciliation.Entry {
	// The gateway settles the fee-inclusive gross amount it was charged, so
	// compare against gateway_amount when present rather than the nominal
	// amount, or every fee-bearing payment shows up as an amount mismatch.
	internalAmount := settledAmountFor(txn)

	entry := &reconciliation.Entry{
		ID:                 uuid.Must(uuid.NewV7()),
		JobID:              jobID,
		TransactionID:      txn.ID,
		InternalStatus:     string(txn.Status),
		GatewayStatus:      se.GatewayStatus,
		InternalAmount:     internalAmount,
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
	} else if internalAmount != se.GatewayAmount {
		entry.MismatchType = reconciliation.MismatchAmount
	} else if entry.InternalFees != nil && se.GatewayFees != nil && *entry.InternalFees != *se.GatewayFees {
		entry.MismatchType = reconciliation.MismatchFee
	} else {
		return nil
	}

	return entry
}

func (s *Service) GetJob(ctx context.Context, actor string, id uuid.UUID) (*reconciliation.Job, error) {
	job, err := s.store.GetJob(ctx, id)
	if err != nil {
		return nil, err
	}
	if !s.canAccessJob(actor, job) {
		return nil, ErrNotFound
	}
	return job, nil
}

func (s *Service) ListJobs(ctx context.Context, filter ports.ReconciliationJobFilter) ([]*reconciliation.Job, error) {
	return s.store.ListJobs(ctx, filter)
}

func (s *Service) RunJob(ctx context.Context, actor string, id uuid.UUID) error {
	job, err := s.store.GetJob(ctx, id)
	if err != nil {
		return err
	}
	if !s.canAccessJob(actor, job) {
		return ErrNotFound
	}
	if job.Status != reconciliation.JobStatusPending {
		return nil
	}
	return s.run(ctx, job)
}

func (s *Service) GetEntries(ctx context.Context, actor string, jobID uuid.UUID, filter ports.ReconciliationEntryFilter) ([]*reconciliation.Entry, error) {
	job, err := s.store.GetJob(ctx, jobID)
	if err != nil {
		return nil, err
	}
	if !s.canAccessJob(actor, job) {
		return nil, ErrNotFound
	}
	return s.store.GetEntries(ctx, jobID, filter)
}

func (s *Service) ResolveEntry(ctx context.Context, actor string, entryID uuid.UUID, resolvedBy, notes string) error {
	entry, err := s.store.GetEntry(ctx, entryID)
	if err != nil {
		return err
	}
	job, err := s.store.GetJob(ctx, entry.JobID)
	if err != nil {
		return err
	}
	if !s.canAccessJob(actor, job) {
		return ErrNotFound
	}
	now := time.Now().UTC()
	entry.ResolutionStatus = reconciliation.ResolutionResolved
	entry.ResolvedBy = resolvedBy
	entry.ResolvedAt = &now
	entry.Notes = notes
	return s.store.UpdateEntry(ctx, entry)
}

// canAccessJob returns true if actor may view/manage the job. Ops ("ops") may
// access any job; service actors may only access jobs they created.
func (s *Service) canAccessJob(actor string, job *reconciliation.Job) bool {
	if actor == "ops" {
		return true
	}
	return job.Actor == actor
}

var ErrNotFound = errors.New("not found")

// settledAmountFor returns the fee-inclusive amount expected in a gateway
// settlement report (gateway_amount when set, else the nominal amount).
func settledAmountFor(txn *transaction.Txn) int64 {
	if txn.GatewayAmount != nil {
		return *txn.GatewayAmount
	}
	return txn.Amount
}

func isInternalSuccess(s transaction.Status) bool {
	switch s {
	case transaction.StatusCaptured, transaction.StatusSettled:
		return true
	}
	return false
}
