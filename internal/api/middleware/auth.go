package middleware

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strings"

	"github.com/crownroutes/payment-service/internal/api/response"
	"github.com/crownroutes/payment-service/internal/ports"
)

type Role string

const (
	RoleService Role = "service"
	RoleOps     Role = "ops"
)

type Principal struct {
	Role     Role
	TenantID string
	UserID   string
}
type TokenProvider interface {
	Resolve(ctx context.Context, role Role, token string) (Principal, bool, error)
}

const principalKey contextKey = "principal"

const tenantIDKey contextKey = "tenant_id"
const userIDKey contextKey = "user_id"

func PrincipalFromContext(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(principalKey).(Principal)
	return p, ok
}

// ContextWithPrincipal is primarily used for testing or internal routing.
func ContextWithPrincipal(ctx context.Context, p Principal) context.Context {
	ctx = context.WithValue(ctx, principalKey, p)
	if p.TenantID != "" {
		ctx = context.WithValue(ctx, tenantIDKey, p.TenantID)
	}
	if p.UserID != "" {
		ctx = context.WithValue(ctx, userIDKey, p.UserID)
	}
	return ctx
}


func TenantIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(tenantIDKey).(string); ok {
		return v
	}
	return ""
}

func UserIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(userIDKey).(string); ok {
		return v
	}
	return ""
}

func Authenticate(provider TokenProvider, log ports.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			path := r.URL.Path

			// Hard-exempt: no service token possible
			if path == "/health" || path == "/favicon.ico" || strings.HasPrefix(path, "/webhooks/") || strings.HasPrefix(path, "/pay/") {
				next.ServeHTTP(w, r)
				return
			}

			// Soft-exempt: try service token, pass through on failure (browser uses checkout token)
			if r.Method == "GET" && strings.HasPrefix(path, "/api/v1/payments/") {
				rest := path[len("/api/v1/payments/"):]
				if !strings.Contains(rest, "/") || (strings.Count(rest, "/") == 1 && strings.HasSuffix(rest, "/events")) {
					principal, ok := authenticate(r, provider)
					if ok {
						next.ServeHTTP(w, r.WithContext(setPrincipalContext(r.Context(), principal)))
					} else {
						next.ServeHTTP(w, r)
					}
					return
				}
			}

			// All other paths: require valid service token
			principal, ok := authenticate(r, provider)
			if !ok {
				log.Warn("auth.rejected", map[string]any{"path": path})
				response.WriteError(w, http.StatusUnauthorized, "unauthorized", "missing or invalid service token", RequestIDFromContext(r.Context()))
				return
			}

			next.ServeHTTP(w, r.WithContext(setPrincipalContext(r.Context(), principal)))
		})
	}
}

func setPrincipalContext(ctx context.Context, p Principal) context.Context {
	ctx = context.WithValue(ctx, principalKey, p)
	if p.TenantID != "" {
		ctx = context.WithValue(ctx, tenantIDKey, p.TenantID)
	}
	if p.UserID != "" {
		ctx = context.WithValue(ctx, userIDKey, p.UserID)
	}
	return ctx
}

func authenticate(r *http.Request, provider TokenProvider) (Principal, bool) {
	if tok := r.Header.Get("X-Ops-Token"); tok != "" {
		return resolveToken(r.Context(), provider, RoleOps, tok)
	}
	if tok := r.Header.Get("X-Service-Token"); tok != "" {
		return resolveToken(r.Context(), provider, RoleService, tok)
	}
	return Principal{}, false
}

func resolveToken(ctx context.Context, provider TokenProvider, role Role, token string) (Principal, bool) {
	principal, ok, err := provider.Resolve(ctx, role, token)
	if err != nil || !ok {
		return Principal{}, false
	}
	return principal, true
}

type StaticTokenProvider struct {
	service map[string]Principal
	ops     map[string]Principal
}

func NewStaticTokenProvider(serviceTokens map[string]string, opsTokens []string) *StaticTokenProvider {
	return &StaticTokenProvider{
		service: servicePrincipals(serviceTokens),
		ops:     opsPrincipals(opsTokens),
	}
}

func (p *StaticTokenProvider) Resolve(_ context.Context, role Role, token string) (Principal, bool, error) {
	var set map[string]Principal
	switch role {
	case RoleOps:
		set = p.ops
	case RoleService:
		set = p.service
	default:
		return Principal{}, false, nil
	}
	sum := sha256.Sum256([]byte(token))
	principal, ok := set[hex.EncodeToString(sum[:])]
	return principal, ok, nil
}

func (p *StaticTokenProvider) HasAny() bool { return len(p.service) > 0 || len(p.ops) > 0 }

func servicePrincipals(tokens map[string]string) map[string]Principal {
	set := make(map[string]Principal, len(tokens))
	for token, raw := range tokens {
		if token == "" {
			continue
		}
		sum := sha256.Sum256([]byte(token))
		tenantID, userID := splitPrincipal(raw)
		set[hex.EncodeToString(sum[:])] = Principal{Role: RoleService, TenantID: tenantID, UserID: userID}
	}
	return set
}

func opsPrincipals(tokens []string) map[string]Principal {
	set := make(map[string]Principal, len(tokens))
	for _, token := range tokens {
		if token == "" {
			continue
		}
		sum := sha256.Sum256([]byte(token))
		set[hex.EncodeToString(sum[:])] = Principal{Role: RoleOps}
	}
	return set
}

func splitPrincipal(raw string) (tenantID, userID string) {
	parts := strings.SplitN(raw, ":", 2)
	switch len(parts) {
	case 0:
		return "", ""
	case 1:
		return strings.TrimSpace(parts[0]), ""
	default:
		return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
	}
}
