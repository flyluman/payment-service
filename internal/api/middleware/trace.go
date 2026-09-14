package middleware

import (
	"net/http"
	"strings"

	"github.com/google/uuid"
)

func TraceID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Trace-ID")
		if id == "" {
			id = parseTraceparent(r.Header.Get("traceparent"))
		}
		if id == "" {
			id = uuid.NewString()
		}
		ctx := WithTraceID(r.Context(), id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func parseTraceparent(header string) string {
	// W3C format: 00-<trace_id>-<span_id>-<flags>
	// trace_id is 32 hex chars (16 bytes)
	parts := strings.Split(header, "-")
	if len(parts) != 4 {
		return ""
	}
	traceID := parts[1]
	if len(traceID) != 32 {
		return ""
	}
	for _, c := range traceID {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return ""
		}
	}
	return traceID
}
