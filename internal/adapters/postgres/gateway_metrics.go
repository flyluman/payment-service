package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/crownroutes/payment-service/internal/ports"
)

type GatewayMetricsStore struct {
	db *DB
}

func NewGatewayMetricsStore(db *DB) *GatewayMetricsStore {
	return &GatewayMetricsStore{db: db}
}

func (s *GatewayMetricsStore) UpsertCircuitBreakerState(ctx context.Context, state *ports.CircuitBreakerStateRecord) error {
	_, err := s.db.Pool().Exec(ctx,
		`INSERT INTO gateway_circuit_breaker_state (gateway_id, state, cooldown_until, consecutive_failures, last_known_reliability_score, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 ON CONFLICT (gateway_id) DO UPDATE SET
			state = EXCLUDED.state,
			cooldown_until = EXCLUDED.cooldown_until,
			consecutive_failures = EXCLUDED.consecutive_failures,
			last_known_reliability_score = EXCLUDED.last_known_reliability_score,
			updated_at = EXCLUDED.updated_at`,
		state.GatewayID,
		state.State,
		state.CooldownUntil,
		state.ConsecutiveFailures,
		state.LastKnownReliabilityScore,
		state.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("gateway_metrics: upsert cb state %s: %w", state.GatewayID, err)
	}
	return nil
}

func (s *GatewayMetricsStore) GetCircuitBreakerState(ctx context.Context, gatewayID string) (*ports.CircuitBreakerStateRecord, error) {
	row := s.db.ReadPool().QueryRow(ctx,
		`SELECT gateway_id, state, cooldown_until, consecutive_failures, last_known_reliability_score, updated_at
		 FROM gateway_circuit_breaker_state
		 WHERE gateway_id = $1`, gatewayID)

	var state ports.CircuitBreakerStateRecord
	err := row.Scan(
		&state.GatewayID, &state.State, &state.CooldownUntil,
		&state.ConsecutiveFailures, &state.LastKnownReliabilityScore, &state.UpdatedAt,
	)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("gateway_metrics: get cb state %s: %w", gatewayID, err)
	}
	return &state, nil
}

func (s *GatewayMetricsStore) ListCircuitBreakerStates(ctx context.Context) ([]*ports.CircuitBreakerStateRecord, error) {
	rows, err := s.db.ReadPool().Query(ctx,
		`SELECT gateway_id, state, cooldown_until, consecutive_failures, last_known_reliability_score, updated_at
		 FROM gateway_circuit_breaker_state`)
	if err != nil {
		return nil, fmt.Errorf("gateway_metrics: list cb states: %w", err)
	}
	defer rows.Close()

	var states []*ports.CircuitBreakerStateRecord
	for rows.Next() {
		var state ports.CircuitBreakerStateRecord
		if err := rows.Scan(
			&state.GatewayID, &state.State, &state.CooldownUntil,
			&state.ConsecutiveFailures, &state.LastKnownReliabilityScore, &state.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("gateway_metrics: scan cb state: %w", err)
		}
		states = append(states, &state)
	}
	return states, rows.Err()
}

var _ ports.GatewayMetricsStore = (*GatewayMetricsStore)(nil)
