package gateway

import (
	"context"
	"fmt"
	"strings"

	"github.com/crownroutes/payment-service/internal/ports"
)

type ConfigStore interface {
	ListActiveGateways(ctx context.Context, paymentMethod string) ([]*ports.GatewayConfig, error)
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

func (s *Service) ListActiveGateways(ctx context.Context, paymentMethods []string) ([]*ports.GatewayConfig, error) {
	seen := map[string]struct{}{}
	var out []*ports.GatewayConfig

	for _, method := range paymentMethods {
		method = strings.TrimSpace(method)
		if method == "" {
			continue
		}

		configs, err := s.config.ListActiveGateways(ctx, method)
		if err != nil {
			return nil, fmt.Errorf("gateway: list for method %s: %w", method, err)
		}

		for _, cfg := range configs {
			if cfg == nil {
				continue
			}
			if _, ok := seen[cfg.GatewayID]; ok {
				continue
			}
			seen[cfg.GatewayID] = struct{}{}

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
	}

	return out, nil
}
