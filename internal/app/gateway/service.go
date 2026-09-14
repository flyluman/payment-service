package gateway

import (
	"context"
	"fmt"

	"github.com/crownroutes/payment-service/internal/ports"
)

type ConfigStore interface {
	ListActiveGateways(ctx context.Context) ([]*ports.GatewayConfig, error)
}

type CircuitBreakerChecker interface {
	IsRoutable(ctx context.Context, gatewayID string) (bool, error)
}

type Service struct {
	config  ConfigStore
	breaker CircuitBreakerChecker
	log     ports.Logger
	metrics ports.MetricRecorder
}

func NewService(config ConfigStore, breaker CircuitBreakerChecker, log ports.Logger, metrics ports.MetricRecorder) *Service {
	return &Service{config: config, breaker: breaker, log: log, metrics: metrics}
}

func (s *Service) ListActiveGateways(ctx context.Context) ([]*ports.GatewayConfig, error) {
	configs, err := s.config.ListActiveGateways(ctx)
	if err != nil {
		return nil, fmt.Errorf("gateway: list active gateways: %w", err)
	}

	var out []*ports.GatewayConfig
	for _, cfg := range configs {
		if cfg == nil {
			continue
		}

		routable, err := s.breaker.IsRoutable(ctx, cfg.GatewayID)
		if err != nil {
			s.log.Warn("gateway.circuit_breaker_check_failed", map[string]any{
				"gateway_id": cfg.GatewayID,
				"error":      err.Error(),
			})
		} else if !routable {
			s.log.Info("gateway.skipped_open_circuit", map[string]any{
				"gateway_id": cfg.GatewayID,
			})
			continue
		}

		out = append(out, cfg)
	}

	return out, nil
}
