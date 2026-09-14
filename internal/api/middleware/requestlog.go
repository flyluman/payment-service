package middleware

import (
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/crownroutes/payment-service/internal/ports"
)

type responseWriter struct {
	http.ResponseWriter
	status int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.status = code
	rw.ResponseWriter.WriteHeader(code)
}

func (rw *responseWriter) Flush() {
	if f, ok := rw.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func RequestLog(log ports.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rw := &responseWriter{ResponseWriter: w, status: http.StatusOK}

			reqID := RequestIDFromContext(r.Context())
			trcID := TraceIDFromContext(r.Context())
			ctx := r.Context()
			if reqID != "" {
				reqLog := log.With(map[string]any{
					ports.FieldRequestID: reqID,
					ports.FieldTraceID:   trcID,
				})
				ctx = WithLogger(ctx, reqLog)
				r = r.WithContext(ctx)
			}

			next.ServeHTTP(rw, r)
			log.Info("http.request", map[string]any{
				"method":      r.Method,
				"path":        r.URL.Path,
				"query":       sanitizeQuery(r.URL.RawQuery),
				"status":      rw.status,
				"duration_ms": time.Since(start).Milliseconds(),
				ports.FieldRequestID: reqID,
				ports.FieldTraceID:   trcID,
			})
		})
	}
}

func sanitizeQuery(raw string) string {
	if raw == "" {
		return ""
	}
	params, err := url.ParseQuery(raw)
	if err != nil {
		return ""
	}
	sensitive := map[string]bool{
		"token": true,
	}
	for key := range params {
		if sensitive[strings.ToLower(key)] {
			params.Set(key, "<redacted>")
		}
	}
	return params.Encode()
}
