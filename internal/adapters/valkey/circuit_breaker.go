package valkey

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/valkey-io/valkey-go"

	"github.com/crownroutes/payment-service/internal/domain/gateway"
)

type CircuitBreakerStore struct {
	client valkey.Client
}

func NewCircuitBreakerStore(c *Client) *CircuitBreakerStore {
	return &CircuitBreakerStore{client: c.Cache}
}

type cbRecord struct {
	State               string `json:"state"`
	CooldownUntil       int64  `json:"cooldown_until"`
	ConsecutiveFailures int    `json:"consecutive_failures"`
	LastKnownScore      int    `json:"last_known_score"`
}

func cbKey(gatewayID string) string    { return "gateway:cb:" + gatewayID }
func probeKey(gatewayID string) string { return "gateway:probe:" + gatewayID }
func critKey(gatewayID string) string  { return "gateway:critical:" + gatewayID }

func (s *CircuitBreakerStore) Get(ctx context.Context, gatewayID string) (*gateway.CircuitBreaker, error) {
	msg, err := s.client.Do(ctx, s.client.B().Get().Key(cbKey(gatewayID)).Build()).ToMessage()
	if err != nil {
		if errors.Is(err, valkey.Nil) {
			return &gateway.CircuitBreaker{GatewayID: gatewayID, State: gateway.StateClosed}, nil
		}
		return nil, fmt.Errorf("circuit_breaker: get %s: %w", gatewayID, err)
	}
	raw, err := msg.AsBytes()
	if err != nil {
		return nil, fmt.Errorf("circuit_breaker: get %s: %w", gatewayID, err)
	}
	var r cbRecord
	if err := json.Unmarshal(raw, &r); err != nil {
		return nil, fmt.Errorf("circuit_breaker: unmarshal %s: %w", gatewayID, err)
	}
	cb := &gateway.CircuitBreaker{
		GatewayID:                 gatewayID,
		State:                     gateway.CircuitState(r.State),
		ConsecutiveFailures:       r.ConsecutiveFailures,
		LastKnownReliabilityScore: r.LastKnownScore,
	}
	if r.CooldownUntil > 0 {
		cb.CooldownUntil = time.Unix(r.CooldownUntil, 0).UTC()
	}
	return cb, nil
}

func (s *CircuitBreakerStore) Transition(ctx context.Context, cb *gateway.CircuitBreaker, to gateway.CircuitState) error {
	newFails := cb.ConsecutiveFailures
	if to == gateway.StateOpen {
		newFails++
	} else if to == gateway.StateClosed {
		newFails = 0
	}

	var cooldownEpoch int64
	if to == gateway.StateOpen {
		cooldownEpoch = time.Now().UTC().Add(gateway.CooldownDuration(newFails)).Unix()
	}

	res := cbTransitionScript.Exec(ctx, s.client,
		[]string{cbKey(cb.GatewayID)},
		[]string{string(to), fmt.Sprintf("%d", cooldownEpoch), fmt.Sprintf("%d", newFails)},
	)
	n, err := res.ToInt64()
	if err != nil {
		return fmt.Errorf("circuit_breaker: transition script %s: %w", cb.GatewayID, err)
	}
	if n == 0 {
		return fmt.Errorf("circuit_breaker: invalid transition %s → %s", cb.State, to)
	}

	cb.State = to
	cb.ConsecutiveFailures = newFails
	if cooldownEpoch > 0 {
		cb.CooldownUntil = time.Unix(cooldownEpoch, 0).UTC()
	} else {
		cb.CooldownUntil = time.Time{}
	}
	return nil
}

func (s *CircuitBreakerStore) RecordFailure(ctx context.Context, gatewayID string, threshold int) (opened bool, failures int, err error) {
	now := fmt.Sprintf("%d", time.Now().UTC().Unix())
	res := cbRecordFailureScript.Exec(ctx, s.client,
		[]string{cbKey(gatewayID)},
		[]string{fmt.Sprintf("%d", threshold), now},
	)
	vals, err := res.AsIntSlice()
	if err != nil {
		return false, 0, fmt.Errorf("circuit_breaker: record failure %s: %w", gatewayID, err)
	}
	if len(vals) != 2 {
		return false, 0, fmt.Errorf("circuit_breaker: record failure %s: unexpected result", gatewayID)
	}
	return vals[0] == 1, int(vals[1]), nil
}

func (s *CircuitBreakerStore) RecordSuccess(ctx context.Context, gatewayID string) error {
	if err := cbRecordSuccessScript.Exec(ctx, s.client,
		[]string{cbKey(gatewayID)}, nil,
	).Error(); err != nil {
		return fmt.Errorf("circuit_breaker: record success %s: %w", gatewayID, err)
	}
	return nil
}

func (s *CircuitBreakerStore) AcquireProbe(ctx context.Context, gatewayID string, ttl time.Duration) (bool, error) {
	res := s.client.Do(ctx, s.client.B().Set().Key(probeKey(gatewayID)).Value("1").Nx().PxMilliseconds(ttl.Milliseconds()).Build())
	msg, err := res.ToMessage()
	if err != nil {
		if errors.Is(err, valkey.Nil) {
			return false, nil
		}
		return false, fmt.Errorf("circuit_breaker: acquire probe %s: %w", gatewayID, err)
	}
	if msg.IsBool() {
		return msg.ToBool()
	}
	return true, nil
}

func (s *CircuitBreakerStore) SetLastKnownScore(ctx context.Context, gatewayID string, score int) error {
	if err := cbSetScoreScript.Exec(ctx, s.client,
		[]string{cbKey(gatewayID)},
		[]string{fmt.Sprintf("%d", score)},
	).Error(); err != nil {
		return fmt.Errorf("circuit_breaker: set last known score %s: %w", gatewayID, err)
	}
	return nil
}

func (s *CircuitBreakerStore) GetCritical(ctx context.Context, gatewayID string) ([]byte, error) {
	msg, err := s.client.Do(ctx, s.client.B().Get().Key(critKey(gatewayID)).Build()).ToMessage()
	if err != nil {
		if errors.Is(err, valkey.Nil) {
			return nil, nil
		}
		return nil, err
	}
	raw, err := msg.AsBytes()
	if err != nil {
		return nil, err
	}
	return raw, nil
}

func (s *CircuitBreakerStore) SetCritical(ctx context.Context, gatewayID string, data []byte) error {
	return s.client.Do(ctx, s.client.B().Set().Key(critKey(gatewayID)).Value(string(data)).Build()).Error()
}

func (s *CircuitBreakerStore) DeleteCritical(ctx context.Context, gatewayID string) error {
	return s.client.Do(ctx, s.client.B().Del().Key(critKey(gatewayID)).Build()).Error()
}
