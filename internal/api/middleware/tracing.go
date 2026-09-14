package middleware

import (
	"net/http"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const tracerName = "payment-service/internal/api"

// Tracing starts an OTel span for each inbound request and joins an incoming
// W3C traceparent when present. Span/trace IDs are also exposed via response
// headers for cross-referencing with logs.
func Tracing(next http.Handler) http.Handler {
	tracer := otel.Tracer(tracerName)
	prop := otel.GetTextMapPropagator()

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := prop.Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		ctx, span := tracer.Start(ctx, spanName(r), trace.WithAttributes(
			attribute.String("http.method", r.Method),
			attribute.String("http.target", r.URL.Path),
		))
		defer span.End()

		r = r.WithContext(ctx)
		if sc := span.SpanContext(); sc.IsValid() {
			w.Header().Set("X-Trace-ID", sc.TraceID().String())
			w.Header().Set("X-Span-ID", sc.SpanID().String())
		}

		sw := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(sw, r)
		span.SetAttributes(attribute.Int("http.status_code", sw.status))
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

func spanName(r *http.Request) string {
	if route := r.Pattern; route != "" {
		return route
	}
	return r.Method + " " + r.URL.Path
}
