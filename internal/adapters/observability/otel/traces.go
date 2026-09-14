package otel

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// NewTraceProvider builds an OTLP-backed TracerProvider for the given config.
// The caller must call the returned Shutdown to flush spans on exit.
func NewTraceProvider(ctx context.Context, cfg Config) (*sdktrace.TracerProvider, error) {
	exporter, err := newTraceExporter(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("otel: build trace exporter: %w", err)
	}

	res, err := resourceFor(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("otel: build trace resource: %w", err)
	}

	return sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter, sdktrace.WithBatchTimeout(2*time.Second)),
		sdktrace.WithResource(res),
	), nil
}

func newTraceExporter(ctx context.Context, cfg Config) (sdktrace.SpanExporter, error) {
	switch cfg.Protocol {
	case "grpc":
		return otlptracegrpc.New(ctx, otlptracegrpc.WithEndpoint(stripScheme(cfg.Endpoint)))
	default:
		return otlptracehttp.New(ctx,
			otlptracehttp.WithEndpoint(stripScheme(cfg.Endpoint)),
			otlptracehttp.WithInsecure(),
		)
	}
}
