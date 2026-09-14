package observability

import (
	"context"
	"fmt"
	"log/slog"

	"go.opentelemetry.io/otel"

	"github.com/crownroutes/payment-service/config"
	otelcore "github.com/crownroutes/payment-service/internal/adapters/observability/otel"
	"github.com/crownroutes/payment-service/internal/ports"
)

// Observability bundles the service's metrics recorder and logger along with
// the close functions that flush each telemetry pipeline on shutdown.
type Observability struct {
	Logger  *SlogLogger
	Metrics ports.MetricRecorder

	closes []func(context.Context) error
}

// Close flushes the telemetry providers in reverse initialization order.
func (o *Observability) Close(ctx context.Context) error {
	var firstErr error
	for i := len(o.closes) - 1; i >= 0; i-- {
		if err := o.closes[i](ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// New wires up the metrics recorder and logger according to the configured
// observability backend. With backend "otel"/"otlp" the logger writes to both
// stdout and OpenObserve and a OTel trace provider is registered globally.
func New(ctx context.Context, cfg *config.Config, level slog.Level, hostname string) (*Observability, error) {
	metrics, metricsClose, err := NewMetrics(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("observability: init metrics: %w", err)
	}

	o := &Observability{
		Metrics: metrics,
		closes:  []func(context.Context) error{metricsClose},
	}

	switch cfg.Observability.Backend {
	case "otel", "otlp":
		otelCfg := otelcore.Config{
			Endpoint:       cfg.Observability.OTLPEndpoint,
			Protocol:       cfg.Observability.OTLPProtocol,
			ServiceName:    cfg.App.ServiceName,
			ServiceVersion: cfg.App.ServiceVersion,
			Environment:    cfg.App.Environment,
			Hostname:       hostname,
		}

		logProvider, err := otelcore.NewLogProvider(ctx, otelCfg)
		if err != nil {
			return nil, fmt.Errorf("observability: init otel logs: %w", err)
		}
		o.closes = append(o.closes, logProvider.Shutdown)

		traceProvider, err := otelcore.NewTraceProvider(ctx, otelCfg)
		if err != nil {
			return nil, fmt.Errorf("observability: init otel traces: %w", err)
		}
		o.closes = append(o.closes, traceProvider.Shutdown)

		otel.SetTracerProvider(traceProvider)
		o.Logger = newSlogLoggerOTel(level, cfg.App.ServiceName, cfg.App.ServiceVersion, cfg.App.Environment, hostname, logProvider)
	default:
		o.Logger = NewSlogLogger(level, cfg.App.ServiceName, cfg.App.ServiceVersion, cfg.App.Environment, hostname)
	}

	return o, nil
}
