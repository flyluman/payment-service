package reconciliation

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/domain/fees"
	"github.com/crownroutes/payment-service/internal/domain/reconciliation"
	"github.com/crownroutes/payment-service/internal/domain/transaction"
	"github.com/crownroutes/payment-service/internal/ports"
)

// --- fakes ---

type fakeStore struct {
	jobs    []*reconciliation.Job
	entries []reconciliation.Entry
}

func (f *fakeStore) CreateJob(_ context.Context, job *reconciliation.Job) error {
	f.jobs = append(f.jobs, job)
	return nil
}
func (f *fakeStore) UpdateJob(_ context.Context, job *reconciliation.Job) error {
	for i, j := range f.jobs {
		if j.ID == job.ID {
			f.jobs[i] = job
		}
	}
	return nil
}
func (f *fakeStore) GetJob(_ context.Context, id uuid.UUID) (*reconciliation.Job, error) {
	for _, j := range f.jobs {
		if j.ID == id {
			return j, nil
		}
	}
	return nil, nil
}
func (f *fakeStore) ListJobs(_ context.Context, _ ports.ReconciliationJobFilter) ([]*reconciliation.Job, error) {
	return f.jobs, nil
}
func (f *fakeStore) InsertEntries(_ context.Context, entries []reconciliation.Entry) error {
	f.entries = append(f.entries, entries...)
	return nil
}
func (f *fakeStore) GetEntries(_ context.Context, _ uuid.UUID, _ ports.ReconciliationEntryFilter) ([]*reconciliation.Entry, error) {
	return nil, nil
}
func (f *fakeStore) UpdateEntry(_ context.Context, _ *reconciliation.Entry) error { return nil }
func (f *fakeStore) GetPendingJobs(_ context.Context, _ int) ([]*reconciliation.Job, error) {
	var pending []*reconciliation.Job
	for _, j := range f.jobs {
		if j.Status == reconciliation.JobStatusPending {
			pending = append(pending, j)
		}
	}
	return pending, nil
}
func (f *fakeStore) GetAutoResolutionConfig(_ context.Context, _, _ string) (*reconciliation.AutoResolutionConfig, error) {
	return &reconciliation.AutoResolutionConfig{ThresholdBPS: 100, AbsoluteCapPaise: 100000}, nil
}

type fakeTxnReader struct {
	txns map[uuid.UUID]*transaction.Txn
}

func (f *fakeTxnReader) GetByID(_ context.Context, id uuid.UUID) (*transaction.Txn, error) {
	return f.txns[id], nil
}

type fakeTxnLister struct {
	result *ports.TransactionListResult
}

func (f *fakeTxnLister) List(_ context.Context, _ ports.TransactionFilter) (*ports.TransactionListResult, error) {
	return f.result, nil
}

type fakeRegistry struct {
	fetchers map[string]ports.SettlementReportFetcher
}

func (f *fakeRegistry) SettlementFetcher(gatewayID string) (ports.SettlementReportFetcher, bool) {
	fetcher, ok := f.fetchers[gatewayID]
	return fetcher, ok
}

type fakeFetcher struct {
	report *ports.SettlementReport
}

func (f *fakeFetcher) FetchSettlementReport(_ context.Context, _ uuid.UUID, _ string, _, _ time.Time) (*ports.SettlementReport, error) {
	return f.report, nil
}

type fakeLogger struct{}

func (f *fakeLogger) Info(string, map[string]any)  {}
func (f *fakeLogger) Warn(string, map[string]any)  {}
func (f *fakeLogger) Error(string, map[string]any, error) {}
func (f *fakeLogger) Debug(string, map[string]any) {}
func (f *fakeLogger) Trace(string, map[string]any) {}
func (f *fakeLogger) With(map[string]any) ports.Logger { return f }

type fakeMetrics struct{}

func (f *fakeMetrics) Increment(string, map[string]string) {}
func (f *fakeMetrics) Histogram(string, float64, map[string]string) {}
func (f *fakeMetrics) Gauge(string, float64, map[string]string) {}

// --- tests ---

func TestCompareTransaction_StatusMatch(t *testing.T) {
	svc := &Service{}
	txn := &transaction.Txn{Status: transaction.StatusSucceeded, Amount: 50000}
	se := &ports.SettlementEntry{GatewayStatus: "succeeded", GatewayAmount: 50000}

	entry := svc.compareTransaction(txn, se, uuid.New())
	if entry != nil {
		t.Error("expected nil (match), got entry")
	}
}

func TestCompareTransaction_StatusMismatch(t *testing.T) {
	svc := &Service{}
	txn := &transaction.Txn{Status: transaction.StatusSucceeded, Amount: 50000}
	se := &ports.SettlementEntry{GatewayStatus: "failed", GatewayAmount: 50000}

	entry := svc.compareTransaction(txn, se, uuid.New())
	if entry == nil {
		t.Fatal("expected entry for status mismatch")
	}
	if entry.MismatchType != reconciliation.MismatchStatus {
		t.Errorf("mismatch type: got %s, want STATUS_MISMATCH", entry.MismatchType)
	}
}

func TestCompareTransaction_AmountMismatch(t *testing.T) {
	svc := &Service{}
	txn := &transaction.Txn{Status: transaction.StatusSucceeded, Amount: 50000}
	se := &ports.SettlementEntry{GatewayStatus: "succeeded", GatewayAmount: 51000}

	entry := svc.compareTransaction(txn, se, uuid.New())
	if entry == nil {
		t.Fatal("expected entry for amount mismatch")
	}
	if entry.MismatchType != reconciliation.MismatchAmount {
		t.Errorf("mismatch type: got %s, want AMOUNT_MISMATCH", entry.MismatchType)
	}
}

func TestCompareTransaction_FeeMismatch(t *testing.T) {
	svc := &Service{}
	fee := int64(1750)
	txn := &transaction.Txn{
		Status: transaction.StatusSucceeded,
		Amount: 50000,
		FeeBreakdown: &fees.Breakdown{
			Fees: fees.FeeDetail{Total: 1750},
		},
	}
	se := &ports.SettlementEntry{
		GatewayStatus: "succeeded",
		GatewayAmount: 50000,
		GatewayFees:   &fee,
	}

	// Same fees → match
	entry := svc.compareTransaction(txn, se, uuid.New())
	if entry != nil {
		t.Error("expected nil (fees match), got entry")
	}

	// Different fees → mismatch
	differentFee := int64(2000)
	se.GatewayFees = &differentFee
	entry = svc.compareTransaction(txn, se, uuid.New())
	if entry == nil {
		t.Fatal("expected entry for fee mismatch")
	}
	if entry.MismatchType != reconciliation.MismatchFee {
		t.Errorf("mismatch type: got %s, want FEE_MISMATCH", entry.MismatchType)
	}
}

func TestCreateJob(t *testing.T) {
	store := &fakeStore{}
	svc := NewService(store, nil, nil, nil, &fakeLogger{}, &fakeMetrics{})

	now := time.Now().UTC()
	job, err := svc.CreateJob(context.Background(), "stripe", &now, nil, nil, "ops")
	if err != nil {
		t.Fatal(err)
	}
	if job.GatewayID != "stripe" {
		t.Errorf("gateway: got %s, want stripe", job.GatewayID)
	}
	if job.Status != reconciliation.JobStatusPending {
		t.Errorf("status: got %s, want PENDING", job.Status)
	}
	if job.TriggeredBy != reconciliation.TriggerOps {
		t.Errorf("triggered_by: got %s, want ops", job.TriggeredBy)
	}
	if len(store.jobs) != 1 {
		t.Errorf("jobs in store: got %d, want 1", len(store.jobs))
	}
}

func TestRunJob_SingleTxn_CompleteMatch(t *testing.T) {
	txnID := uuid.New()
	txn := &transaction.Txn{
		ID:                 txnID,
		GatewayID:          "stripe",
		TenantID:           uuid.New(),
		Status:             transaction.StatusSucceeded,
		Amount:             50000,
		GatewayReferenceID: "pi_123",
	}

	store := &fakeStore{}
	reader := &fakeTxnReader{txns: map[uuid.UUID]*transaction.Txn{txnID: txn}}
	fetcher := &fakeFetcher{report: &ports.SettlementReport{
		Entries: []ports.SettlementEntry{
			{TransactionRef: "pi_123", GatewayStatus: "succeeded", GatewayAmount: 50000},
		},
	}}
	registry := &fakeRegistry{fetchers: map[string]ports.SettlementReportFetcher{"stripe": fetcher}}

	svc := NewService(store, reader, nil, registry, &fakeLogger{}, &fakeMetrics{})

	job := &reconciliation.Job{
		ID:            uuid.New(),
		GatewayID:     "stripe",
		TransactionID: &txnID,
		Status:        reconciliation.JobStatusPending,
	}
	store.jobs = append(store.jobs, job)

	err := svc.RunJob(context.Background(), job.ID)
	if err != nil {
		t.Fatal(err)
	}

	// Complete match → no entries
	if len(store.entries) != 0 {
		t.Errorf("entries: got %d, want 0", len(store.entries))
	}
	if job.Status != reconciliation.JobStatusCompleted {
		t.Errorf("status: got %s, want COMPLETED", job.Status)
	}
}

func TestRunJob_SingleTxn_MissingGateway(t *testing.T) {
	txnID := uuid.New()
	txn := &transaction.Txn{
		ID:        txnID,
		GatewayID: "stripe",
		TenantID:  uuid.New(),
		Status:    transaction.StatusSucceeded,
		Amount:    50000,
	}

	store := &fakeStore{}
	reader := &fakeTxnReader{txns: map[uuid.UUID]*transaction.Txn{txnID: txn}}
	fetcher := &fakeFetcher{report: &ports.SettlementReport{Entries: []ports.SettlementEntry{}}}
	registry := &fakeRegistry{fetchers: map[string]ports.SettlementReportFetcher{"stripe": fetcher}}

	svc := NewService(store, reader, nil, registry, &fakeLogger{}, &fakeMetrics{})

	job := &reconciliation.Job{
		ID:            uuid.New(),
		GatewayID:     "stripe",
		TransactionID: &txnID,
		Status:        reconciliation.JobStatusPending,
	}
	store.jobs = append(store.jobs, job)

	err := svc.RunJob(context.Background(), job.ID)
	if err != nil {
		t.Fatal(err)
	}

	// No gateway entry → MISSING_GATEWAY
	if len(store.entries) != 1 {
		t.Fatalf("entries: got %d, want 1", len(store.entries))
	}
	if store.entries[0].MismatchType != reconciliation.MismatchMissingGateway {
		t.Errorf("mismatch: got %s, want MISSING_GATEWAY", store.entries[0].MismatchType)
	}
}

func TestRunJob_NonPending_Skipped(t *testing.T) {
	store := &fakeStore{}
	svc := NewService(store, nil, nil, nil, &fakeLogger{}, &fakeMetrics{})

	job := &reconciliation.Job{
		ID:     uuid.New(),
		Status: reconciliation.JobStatusCompleted,
	}
	store.jobs = append(store.jobs, job)

	err := svc.RunJob(context.Background(), job.ID)
	if err != nil {
		t.Fatal(err)
	}
	// Should be a no-op, status unchanged
	if job.Status != reconciliation.JobStatusCompleted {
		t.Errorf("status: got %s, want COMPLETED (unchanged)", job.Status)
	}
}

func TestResolveEntry(t *testing.T) {
	store := &fakeStore{}
	svc := NewService(store, nil, nil, nil, &fakeLogger{}, &fakeMetrics{})

	entryID := uuid.New()
	err := svc.ResolveEntry(context.Background(), entryID, "ops", "looks good")
	if err != nil {
		t.Fatal(err)
	}
	// Store.UpdateEntry was called — we just verify no error
}
