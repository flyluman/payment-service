package ports

import (
	"context"
	"time"
)

type GatewayMetricsStore interface {
	UpsertCircuitBreakerState(ctx context.Context, state *CircuitBreakerStateRecord) error
	GetCircuitBreakerState(ctx context.Context, gatewayID string) (*CircuitBreakerStateRecord, error)
	ListCircuitBreakerStates(ctx context.Context) ([]*CircuitBreakerStateRecord, error)
}

type CircuitBreakerStateRecord struct {
	GatewayID               string
	State                   string
	CooldownUntil           time.Time
	ConsecutiveFailures     int
	LastKnownReliabilityScore int
	UpdatedAt               time.Time
}
