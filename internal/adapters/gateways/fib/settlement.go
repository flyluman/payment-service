package fib

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/ports"
)

func (a *Adapter) FetchSettlementReport(ctx context.Context, tenantID uuid.UUID, gatewayID string, start, end time.Time) (*ports.SettlementReport, error) {
	report := &ports.SettlementReport{
		GatewayID:   gatewayID,
		PeriodStart: start,
		PeriodEnd:   end,
		Entries:     []ports.SettlementEntry{},
	}

	// FIB does not expose a dedicated settlement listing endpoint.
	// In production this would call FIB's payment listing API with date filters.
	// For now, return an empty report — the reconciliation service will fall back
	// to single-transaction CheckStatus via the lease expiry reaper pattern.
	_ = ctx
	_ = tenantID
	return report, nil
}

var _ ports.SettlementReportFetcher = (*Adapter)(nil)
