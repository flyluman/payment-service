package middleware

import (
	"context"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/crownroutes/payment-service/internal/api/response"
	"github.com/crownroutes/payment-service/internal/ports"
)

type RateDecision struct {
	Allowed    bool
	RetryAfter time.Duration
}

type Limiter interface {
	Allow(ctx context.Context, userID, tenantID, ip string, capacity int64, ratePerSec float64) RateDecision
}

type RateLimitConfig struct {
	Capacity     int64
	RefillPerSec float64
}

func RateLimit(limiter Limiter, cfg RateLimitConfig, log ports.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			userID := UserIDFromContext(r.Context())
			if userID == "" {
				userID = r.Header.Get("X-User-ID")
			}
			tenantID := tenantBucketID(r.Context())
			ip := clientIP(r)

			decision := limiter.Allow(r.Context(), userID, tenantID, ip, cfg.Capacity, cfg.RefillPerSec)
			if !decision.Allowed {
				retrySec := int(decision.RetryAfter.Seconds())
				if retrySec < 1 {
					retrySec = 1
				}
				w.Header().Set("Retry-After", strconv.Itoa(retrySec))
				log.Warn(ports.LogEventRateLimitRejected, map[string]any{
					"ip":   ip,
					"path": r.URL.Path,
				})
				response.WriteError(w, http.StatusTooManyRequests, "rate_limited", "too many requests", RequestIDFromContext(r.Context()))
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

func tenantBucketID(ctx context.Context) string {
	if t := TenantIDFromContext(ctx); t != "" {
		return t
	}
	if p, ok := PrincipalFromContext(ctx); ok {
		return "principal:" + string(p.Role)
	}
	return ""
}

func clientIP(r *http.Request) string {
	if fwd := r.Header.Get("X-Forwarded-For"); fwd != "" {
		return strings.TrimSpace(strings.SplitN(fwd, ",", 2)[0])
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
