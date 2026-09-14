package ports

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/domain/reconciliation"
)

type ReconciliationStore interface {
	CreateJob(ctx context.Context, job *reconciliation.Job) error
	UpdateJob(ctx context.Context, job *reconciliation.Job) error
	GetJob(ctx context.Context, id uuid.UUID) (*reconciliation.Job, error)
	ListJobs(ctx context.Context, filter ReconciliationJobFilter) ([]*reconciliation.Job, error)
	InsertEntries(ctx context.Context, entries []reconciliation.Entry) error
	GetEntries(ctx context.Context, jobID uuid.UUID, filter ReconciliationEntryFilter) ([]*reconciliation.Entry, error)
	GetEntry(ctx context.Context, entryID uuid.UUID) (*reconciliation.Entry, error)
	UpdateEntry(ctx context.Context, entry *reconciliation.Entry) error
	GetPendingJobs(ctx context.Context, limit int) ([]*reconciliation.Job, error)
	GetAutoResolutionConfig(ctx context.Context, gatewayID, paymentMethod string) (*reconciliation.AutoResolutionConfig, error)
}

type SettlementReportFetcher interface {
	FetchSettlementReport(ctx context.Context, tenantID uuid.UUID, gatewayID string, start, end time.Time) (*SettlementReport, error)
}

type SettlementReport struct {
	GatewayID   string
	PeriodStart time.Time
	PeriodEnd   time.Time
	Entries     []SettlementEntry
}

type SettlementEntry struct {
	TransactionRef string
	GatewayStatus  string
	GatewayAmount  int64
	GatewayFees    *int64
	FXRate         *float64
	SettledAt      time.Time
}

type ReconciliationJobFilter struct {
	Actor         *string
	TenantID      *uuid.UUID
	GatewayID     *string
	Status        *string
	TransactionID *uuid.UUID
	DateFrom      *time.Time
	DateTo        *time.Time
	Limit         int
	Offset        int
}

type ReconciliationEntryFilter struct {
	MismatchType     *string
	ResolutionStatus *string
	Limit            int
	Offset           int
}
