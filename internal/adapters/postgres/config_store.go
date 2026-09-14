package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/crownroutes/payment-service/internal/domain/fees"
	"github.com/crownroutes/payment-service/internal/ports"
)

var ErrGatewayNotFound = errors.New("gateway not found")

const (
	defaultWebhookReplayWindowSec = 300
	defaultWebhookClockSkewSec    = 30
)

type ConfigCache interface {
	GetGatewayConfig(ctx context.Context, gatewayID string) (*ports.GatewayConfig, error)
	SetGatewayConfig(ctx context.Context, cfg *ports.GatewayConfig) error
	DelGatewayConfig(ctx context.Context, gatewayID string) error
	GetProcessingTimeout(ctx context.Context, gatewayID, paymentMethod string) (time.Duration, error)
	SetProcessingTimeout(ctx context.Context, gatewayID, paymentMethod string, timeout time.Duration) error
	DelProcessingTimeout(ctx context.Context, gatewayID, paymentMethod string) error
}

type ConfigStore struct {
	db    *DB
	cache ConfigCache
}

func NewConfigStore(db *DB) *ConfigStore { return &ConfigStore{db: db} }

func (s *ConfigStore) SetCache(c ConfigCache) { s.cache = c }

var _ ports.ConfigStore = (*ConfigStore)(nil)

func (s *ConfigStore) GetGatewayConfig(ctx context.Context, gatewayID string) (*ports.GatewayConfig, error) {
	if s.cache != nil {
		cfg, err := s.cache.GetGatewayConfig(ctx, gatewayID)
		if err == nil && cfg != nil {
			return cfg, nil
		}
	}

	row := s.db.ReadPool().QueryRow(ctx, `SELECT
    gc.gateway_id, gc.display_name, gc.is_active,
    gc.min_amount, gc.max_amount,
    gc.supported_currencies, gc.supported_methods,
    gc.idempotency_capable, gc.supports_cancel, gc.supports_partial_refund,
    gc.priority, gc.updated_at,
    COALESCE(cb.state, 'CLOSED'),
    COALESCE(cb.cooldown_until, '0001-01-01 00:00:00+00')
FROM gateway_config gc
LEFT JOIN gateway_circuit_breaker_state cb ON cb.gateway_id = gc.gateway_id
WHERE gc.gateway_id = $1`, gatewayID)

	cfg, err := scanGatewayConfig(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrGatewayNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("config_store: get gateway %s: %w", gatewayID, err)
	}

	timeouts, err := s.getTimeouts(ctx, gatewayID)
	if err != nil {
		return nil, err
	}
	cfg.EstimatedTimeouts = timeouts

	if s.cache != nil {
		_ = s.cache.SetGatewayConfig(ctx, cfg)
	}
	return cfg, nil
}

func (s *ConfigStore) ListActiveGateways(ctx context.Context) ([]*ports.GatewayConfig, error) {
	rows, err := s.db.ReadPool().Query(ctx, `SELECT
    gc.gateway_id, gc.display_name, gc.is_active,
    gc.min_amount, gc.max_amount,
    gc.supported_currencies, gc.supported_methods,
    gc.idempotency_capable, gc.supports_cancel, gc.supports_partial_refund,
    gc.priority, gc.updated_at,
    COALESCE(cb.state, 'CLOSED'),
    COALESCE(cb.cooldown_until, '0001-01-01 00:00:00+00')
FROM gateway_config gc
LEFT JOIN gateway_circuit_breaker_state cb ON cb.gateway_id = gc.gateway_id
WHERE gc.is_active = true`)
	if err != nil {
		return nil, fmt.Errorf("config_store: list active gateways: %w", err)
	}
	defer rows.Close()

	var configs []*ports.GatewayConfig
	var ids []string
	for rows.Next() {
		cfg, err := scanGatewayConfig(rows)
		if err != nil {
			return nil, fmt.Errorf("config_store: scan gateway: %w", err)
		}
		configs = append(configs, cfg)
		ids = append(ids, cfg.GatewayID)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	timeouts, err := s.timeoutsForGateways(ctx, ids)
	if err != nil {
		return nil, err
	}
	for _, cfg := range configs {
		cfg.EstimatedTimeouts = timeouts[cfg.GatewayID]
	}
	return configs, nil
}

func (s *ConfigStore) timeoutsForGateways(ctx context.Context, gatewayIDs []string) (map[string]map[string]time.Duration, error) {
	out := make(map[string]map[string]time.Duration, len(gatewayIDs))
	if len(gatewayIDs) == 0 {
		return out, nil
	}

	rows, err := s.db.ReadPool().Query(ctx, `SELECT gateway_id, payment_method, estimated_timeout_sec
FROM gateway_timeouts
WHERE gateway_id = ANY($1)`, gatewayIDs)
	if err != nil {
		return nil, fmt.Errorf("config_store: list timeouts for gateways: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var gatewayID, method string
		var sec int
		if err := rows.Scan(&gatewayID, &method, &sec); err != nil {
			return nil, fmt.Errorf("config_store: scan timeout: %w", err)
		}
		if out[gatewayID] == nil {
			out[gatewayID] = make(map[string]time.Duration)
		}
		out[gatewayID][method] = time.Duration(sec) * time.Second
	}
	return out, rows.Err()
}

func (s *ConfigStore) GetFeeModel(ctx context.Context, gatewayID, paymentMethod string) (*ports.GatewayFeeModel, error) {
	var m ports.GatewayFeeModel
	err := s.db.ReadPool().QueryRow(ctx, `SELECT gateway_id, payment_method, fixed_fee, percentage_bps,
       interchange_cap, discount_volume_threshold, COALESCE(charges_currency, '')
FROM gateway_fee_models
WHERE gateway_id = $1 AND payment_method = $2`, gatewayID, paymentMethod).Scan(
		&m.GatewayID, &m.PaymentMethod, &m.FixedFee, &m.PercentageBPS,
		&m.InterchangeCap, &m.DiscountVolumeThreshold, &m.ChargesCurrency,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("config_store: get fee model %s/%s: %w", gatewayID, paymentMethod, err)
	}
	return &m, nil
}

func (s *ConfigStore) GetCurrencyRates(ctx context.Context, tenantID uuid.UUID) ([]fees.CurrencyRate, error) {
	rows, err := s.db.ReadPool().Query(ctx, `SELECT from_currency, to_currency, rate, markup_pct, markup_fixed
FROM tenant_currency_rates
WHERE tenant_id = $1`, tenantID)
	if err != nil {
		return nil, fmt.Errorf("config_store: get currency rates for tenant %s: %w", tenantID, err)
	}
	defer rows.Close()

	var rates []fees.CurrencyRate
	for rows.Next() {
		var r fees.CurrencyRate
		if err := rows.Scan(&r.FromCurrency, &r.ToCurrency, &r.Rate, &r.MarkupPct, &r.MarkupFixed); err != nil {
			return nil, fmt.Errorf("config_store: scan currency rate: %w", err)
		}
		rates = append(rates, r)
	}
	return rates, rows.Err()
}

func (s *ConfigStore) GetMetadataSchema(ctx context.Context, gatewayID string) (*ports.GatewayMetadataSchema, error) {
	var schema ports.GatewayMetadataSchema
	err := s.db.ReadPool().QueryRow(ctx, `SELECT gateway_id, allowed_keys, required_keys, max_size_bytes
FROM gateway_metadata_schemas
WHERE gateway_id = $1`, gatewayID).Scan(
		&schema.GatewayID, &schema.AllowedKeys,
		&schema.RequiredKeys, &schema.MaxSizeBytes,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("config_store: no metadata schema for %s", gatewayID)
	}
	if err != nil {
		return nil, fmt.Errorf("config_store: get metadata schema %s: %w", gatewayID, err)
	}
	return &schema, nil
}

func (s *ConfigStore) WebhookPolicy(ctx context.Context, gatewayID string) (replayWindowSec, clockSkewSec int, err error) {
	err = s.db.ReadPool().QueryRow(ctx, `SELECT webhook_replay_window_sec, webhook_clock_skew_sec
FROM gateway_config
WHERE gateway_id = $1`, gatewayID).Scan(&replayWindowSec, &clockSkewSec)
	if errors.Is(err, pgx.ErrNoRows) {
		return defaultWebhookReplayWindowSec, defaultWebhookClockSkewSec, nil
	}
	if err != nil {
		return 0, 0, fmt.Errorf("config_store: webhook policy %s: %w", gatewayID, err)
	}
	return replayWindowSec, clockSkewSec, nil
}

func (s *ConfigStore) GetProcessingTimeout(ctx context.Context, gatewayID, paymentMethod string) (time.Duration, error) {
	if s.cache != nil {
		d, err := s.cache.GetProcessingTimeout(ctx, gatewayID, paymentMethod)
		if err == nil && d > 0 {
			return d, nil
		}
	}

	var sec int
	err := s.db.ReadPool().QueryRow(ctx, `SELECT estimated_timeout_sec
FROM gateway_timeouts
WHERE gateway_id = $1 AND payment_method = $2`, gatewayID, paymentMethod).Scan(&sec)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("config_store: no timeout configured for %s/%s", gatewayID, paymentMethod)
	}
	if err != nil {
		return 0, fmt.Errorf("config_store: get processing timeout %s/%s: %w", gatewayID, paymentMethod, err)
	}
	d := time.Duration(sec) * time.Second
	if s.cache != nil {
		_ = s.cache.SetProcessingTimeout(ctx, gatewayID, paymentMethod, d)
	}
	return d, nil
}

func (s *ConfigStore) getTimeouts(ctx context.Context, gatewayID string) (map[string]time.Duration, error) {
	rows, err := s.db.ReadPool().Query(ctx, `SELECT payment_method, estimated_timeout_sec
FROM gateway_timeouts
WHERE gateway_id = $1`, gatewayID)
	if err != nil {
		return nil, fmt.Errorf("config_store: get timeouts for %s: %w", gatewayID, err)
	}
	defer rows.Close()

	timeouts := make(map[string]time.Duration)
	for rows.Next() {
		var method string
		var sec int
		if err := rows.Scan(&method, &sec); err != nil {
			return nil, fmt.Errorf("config_store: scan timeout: %w", err)
		}
		timeouts[method] = time.Duration(sec) * time.Second
	}
	return timeouts, rows.Err()
}

func scanGatewayConfig(row interface {
	Scan(dest ...any) error
}) (*ports.GatewayConfig, error) {
	var cfg ports.GatewayConfig
	var cbState string
	var cbCooldown time.Time

	err := row.Scan(
		&cfg.GatewayID, &cfg.DisplayName, &cfg.IsActive,
		&cfg.MinAmount, &cfg.MaxAmount,
		&cfg.SupportedCurrencies, &cfg.SupportedMethods,
		&cfg.IdempotencyCapable, &cfg.SupportsCancel, &cfg.SupportsPartialRefund,
		&cfg.Priority, &cfg.UpdatedAt,
		&cbState, &cbCooldown,
	)
	if err != nil {
		return nil, err
	}

	cfg.CircuitBreaker = ports.CircuitBreakerState{
		State:         cbState,
		CooldownUntil: cbCooldown,
	}

	return &cfg, nil
}
