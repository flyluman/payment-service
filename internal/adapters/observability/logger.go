package observability

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/crownroutes/payment-service/internal/ports"
)

const levelTrace = slog.Level(-8)

type SlogLogger struct {
	base *slog.Logger
}

func NewSlogLogger(level slog.Level, svcName, svcVersion, env, hostname string) *SlogLogger {
	h := stdoutJSONHandler(level)
	base := slog.New(h).With(
		"service", svcName,
		"version", svcVersion,
		"environment", env,
		"hostname", hostname,
	)
	return &SlogLogger{base: base}
}

// stdoutJSONHandler returns the canonical stdout JSON handler with the
// timestamp rendered as an RFC3339Nano "timestamp" attribute.
func stdoutJSONHandler(level slog.Level) slog.Handler {
	return slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey {
				a.Key = "timestamp"
				a.Value = slog.StringValue(a.Value.Time().UTC().Format(time.RFC3339Nano))
			}
			return a
		},
	})
}

const redactedValue = "<redacted>"

var sensitiveLogKeys = map[string]struct{}{
	"vpa":             {},
	"card_number":     {},
	"pan":             {},
	"cvv":             {},
	"card_cvv":        {},
	"token":           {},
	"api_key":         {},
	"client_secret":   {},
	"client_id":       {},
	"access_token":    {},
	"key_secret":      {},
	"publishable_key": {},
	"webhook_secret":  {},
	"authorization":   {},
	"bearer":          {},
	"password":        {},
}

func fieldsToAttrs(fields map[string]any) []any {
	attrs := make([]any, 0, len(fields)*2)
	for k, v := range fields {
		if _, sensitive := sensitiveLogKeys[strings.ToLower(k)]; sensitive {
			v = redactedValue
		}
		attrs = append(attrs, k, v)
	}
	return attrs
}

func NewSlogLoggerFromHandler(h slog.Handler) *SlogLogger { return &SlogLogger{base: slog.New(h)} }
func (l *SlogLogger) Info(event string, fields map[string]any) {
	l.base.Info(event, fieldsToAttrs(fields)...)
}
func (l *SlogLogger) Warn(event string, fields map[string]any) {
	l.base.Warn(event, fieldsToAttrs(fields)...)
}
func (l *SlogLogger) Debug(event string, fields map[string]any) {
	l.base.Debug(event, fieldsToAttrs(fields)...)
}
func (l *SlogLogger) Trace(event string, fields map[string]any) {
	l.base.Log(context.Background(), levelTrace, event, fieldsToAttrs(fields)...)
}
func (l *SlogLogger) With(fields map[string]any) ports.Logger {
	return &SlogLogger{base: l.base.With(fieldsToAttrs(fields)...)}
}

func (l *SlogLogger) Error(event string, fields map[string]any, err error) {
	if fields == nil {
		fields = map[string]any{}
	}

	var missing []string
	for _, required := range []string{ports.FieldErrorCode, ports.FieldTraceID, ports.FieldTransactionID} {
		if _, ok := fields[required]; !ok {
			missing = append(missing, required)
		}
	}

	attrs := fieldsToAttrs(fields)
	if err != nil {
		attrs = append(attrs, "error", err.Error())
	}
	if len(missing) > 0 {
		attrs = append(attrs, "log_validation_error", "missing required fields: "+strings.Join(missing, ","))
	}

	l.base.Error(event, attrs...)
}

var _ ports.Logger = (*SlogLogger)(nil)
