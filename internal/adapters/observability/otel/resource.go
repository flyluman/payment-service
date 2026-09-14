package otel

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// resourceFor builds the shared resource attributes applied to metrics, logs,
// and traces so telemetry can be correlated across signals.
func resourceFor(ctx context.Context, cfg Config) (*resource.Resource, error) {
	attrs := []attribute.KeyValue{
		semconv.ServiceName(cfg.ServiceName),
		semconv.ServiceVersion(cfg.ServiceVersion),
		semconv.DeploymentEnvironment(cfg.Environment),
	}
	if cfg.Hostname != "" {
		attrs = append(attrs, semconv.HostName(cfg.Hostname))
	}
	return resource.New(ctx, resource.WithAttributes(attrs...))
}
