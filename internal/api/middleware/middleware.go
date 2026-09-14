package middleware

import (
	"context"
	"net/http"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/api/response"
	"github.com/crownroutes/payment-service/internal/ports"
)

type contextKey string

const requestIDKey contextKey = "request_id"
const traceIDKey contextKey = "trace_id"
const loggerKey contextKey = "logger"

func RequestIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(requestIDKey).(string); ok {
		return v
	}
	return ""
}

func TraceIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(traceIDKey).(string); ok {
		return v
	}
	return ""
}

func WithTraceID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, traceIDKey, id)
}

func LoggerFromContext(ctx context.Context) ports.Logger {
	if v, ok := ctx.Value(loggerKey).(ports.Logger); ok {
		return v
	}
	return nil
}

func WithLogger(ctx context.Context, log ports.Logger) context.Context {
	return context.WithValue(ctx, loggerKey, log)
}

func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		if id == "" {
			id = uuid.NewString()
		}
		w.Header().Set("X-Request-ID", id)
		ctx := context.WithValue(r.Context(), requestIDKey, id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func Recover(log ports.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rec := recover(); rec != nil {
					reqLog := log
					if ctxLog := LoggerFromContext(r.Context()); ctxLog != nil {
						reqLog = ctxLog
					}
					reqLog.Error("http.panic_recovered", map[string]any{
						ports.FieldErrorCode:     "panic",
						ports.FieldTraceID:       TraceIDFromContext(r.Context()),
						ports.FieldRequestID:     RequestIDFromContext(r.Context()),
						ports.FieldTransactionID: "",
						"path":                   r.URL.Path,
					}, nil)
					response.WriteError(w, http.StatusInternalServerError, "internal_error", "internal server error", RequestIDFromContext(r.Context()))
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}

func Chain(h http.Handler, middlewares ...func(http.Handler) http.Handler) http.Handler {
	for i := len(middlewares) - 1; i >= 0; i-- {
		h = middlewares[i](h)
	}
	return h
}
