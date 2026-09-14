package otel

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	sdklog "go.opentelemetry.io/otel/sdk/log"
)

// NewLogProvider builds an OTLP-backed LoggerProvider for the given config.
// The returned provider must be registered via the global logger provider (or
// passed to a slog bridge) and its Shutdown called on exit.
func NewLogProvider(ctx context.Context, cfg Config) (*sdklog.LoggerProvider, error) {
	exporter, err := newLogExporter(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("otel: build log exporter: %w", err)
	}

	res, err := resourceFor(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("otel: build log resource: %w", err)
	}

	return sdklog.NewLoggerProvider(
		sdklog.WithResource(res),
		sdklog.WithProcessor(sdklog.NewBatchProcessor(
			exporter,
			sdklog.WithExportInterval(2*time.Second),
		)),
	), nil
}

func newLogExporter(ctx context.Context, cfg Config) (sdklog.Exporter, error) {
	switch cfg.Protocol {
	case "grpc":
		return otlploggrpc.New(ctx, otlploggrpc.WithEndpoint(stripScheme(cfg.Endpoint)))
	default:
		return otlploghttp.New(ctx,
			otlploghttp.WithEndpoint(stripScheme(cfg.Endpoint)),
			otlploghttp.WithInsecure(),
		)
	}
}

// stripScheme strips http:// or https:// from an endpoint URL, returning the
// bare host:port that OTLP WithEndpoint expects.
func stripScheme(endpoint string) string {
	if after, ok := strings.CutPrefix(endpoint, "https://"); ok {
		endpoint = after
	}
	if after, ok := strings.CutPrefix(endpoint, "http://"); ok {
		endpoint = after
	}
	endpoint = strings.TrimRight(endpoint, "/")
	return endpoint
}
