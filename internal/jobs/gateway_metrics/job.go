package gateway_metrics

import (
	"context"
	"time"

	"github.com/crownroutes/payment-service/internal/domain/gateway"
	"github.com/crownroutes/payment-service/internal/ports"
)

type CircuitBreakerReader interface {
	Get(ctx context.Context, gatewayID string) (*gateway.CircuitBreaker, error)
}

type GatewayLister interface {
	IDs() []string
}

type Job struct {
	cbStore  CircuitBreakerReader
	metrics  ports.GatewayMetricsStore
	gateways GatewayLister
	log      ports.Logger
}

func NewJob(
	cbStore CircuitBreakerReader,
	metrics ports.GatewayMetricsStore,
	gateways GatewayLister,
	log ports.Logger,
) *Job {
	return &Job{
		cbStore:  cbStore,
		metrics:  metrics,
		gateways: gateways,
		log:      log,
	}
}

func (j *Job) RunOnce(ctx context.Context) error {
	ids := j.gateways.IDs()
	now := time.Now().UTC()
	for _, gwID := range ids {
		cb, err := j.cbStore.Get(ctx, gwID)
		if err != nil {
			j.log.Warn("gateway_metrics.cb_read_failed", map[string]any{
				"gateway_id": gwID,
				"error":      err.Error(),
			})
			continue
		}

		state := &ports.CircuitBreakerStateRecord{
			GatewayID:                 gwID,
			State:                     string(cb.State),
			CooldownUntil:             cb.CooldownUntil,
			ConsecutiveFailures:       cb.ConsecutiveFailures,
			LastKnownReliabilityScore: cb.LastKnownReliabilityScore,
			UpdatedAt:                 now,
		}
		if err := j.metrics.UpsertCircuitBreakerState(ctx, state); err != nil {
			j.log.Warn("gateway_metrics.cb_write_failed", map[string]any{
				"gateway_id": gwID,
				"error":      err.Error(),
			})
		}
	}

	return nil
}
