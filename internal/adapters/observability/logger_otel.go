package observability

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	sdklog "go.opentelemetry.io/otel/sdk/log"
)

// newSlogLoggerOTel builds a logger that writes to stdout AND forwards each
// record to the given OTLP logger provider (so logs land in OpenObserve).
//
// Identity fields (service/version/environment/hostname) are emitted by the
// OTel resource on the OTLP side (service_name, service_version,
// deployment_environment, host.name), so they are attached to the stdout
// handler only to avoid duplicated attributes in log records.
func newSlogLoggerOTel(level slog.Level, svcName, svcVersion, env, hostname string, provider *sdklog.LoggerProvider) *SlogLogger {
	stdoutHandler := stdoutJSONHandler(level).WithAttrs([]slog.Attr{
		slog.String("service", svcName),
		slog.String("version", svcVersion),
		slog.String("environment", env),
		slog.String("hostname", hostname),
	})
	otelHandler := otelslog.NewHandler(svcName, otelslog.WithLoggerProvider(provider))
	h := &multiHandler{
		level:    level,
		handlers: []slog.Handler{stdoutHandler, otelHandler},
	}
	return &SlogLogger{base: slog.New(h)}
}

// multiHandler fans out each log record to every wrapped handler, letting the
// same record reach both stdout and OpenTelemetry.
type multiHandler struct {
	handlers []slog.Handler
	level    slog.Level
}

func (m *multiHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= m.level
}

func (m *multiHandler) Handle(ctx context.Context, r slog.Record) error {
	for _, h := range m.handlers {
		if err := h.Handle(ctx, r); err != nil {
			return err
		}
	}
	return nil
}

func (m *multiHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	next := make([]slog.Handler, len(m.handlers))
	for i, h := range m.handlers {
		next[i] = h.WithAttrs(attrs)
	}
	return &multiHandler{handlers: next, level: m.level}
}

func (m *multiHandler) WithGroup(name string) slog.Handler {
	next := make([]slog.Handler, len(m.handlers))
	for i, h := range m.handlers {
		next[i] = h.WithGroup(name)
	}
	return &multiHandler{handlers: next, level: m.level}
}
