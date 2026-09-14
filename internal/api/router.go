package api

import (
	"html/template"
	"io"
	"net/http"

	"github.com/crownroutes/payment-service/internal/api/handlers"
	"github.com/crownroutes/payment-service/internal/api/middleware"
	"github.com/crownroutes/payment-service/internal/ports"
)

type Deps struct {
	Payment        *handlers.PaymentHandler
	Pay            *handlers.PayHandler
	Gateway        *handlers.GatewayHandler
	TenantGateway  *handlers.TenantGatewayHandler
	Refund         *handlers.RefundHandler
	Cancel         *handlers.CancelHandler
	Webhook        *handlers.WebhookHandler
	Dispute        *handlers.DisputeHandler
	DeadLetter     *handlers.DeadLetterHandler
	Reconciliation *handlers.ReconciliationHandler
	Health         *handlers.HealthHandler
	SSE            *handlers.SSEHandler
	UI             http.Handler
	Logger         ports.Logger
	Auth           middleware.TokenProvider
	Limiter        middleware.Limiter
	RateLimit      middleware.RateLimitConfig
	ResponseCache  middleware.ResponseCacheStore
}

func NewRouter(deps Deps) http.Handler {
	mux := http.NewServeMux()

	// Health + favicon — no auth
	mux.HandleFunc("GET /health", deps.Health.Health)
	mux.HandleFunc("GET /favicon.ico", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	// ── /pay/ routes (browser-facing, checkout token auth) ──
	if deps.Pay != nil {
		mux.HandleFunc("GET /pay/{transaction_id}", deps.Pay.ServeCheckout)
		mux.HandleFunc("GET /pay/success", deps.Pay.Success)
		mux.HandleFunc("GET /pay/failure", deps.Pay.Failure)
	}

	// ── /api/v1/ routes ──
	if deps.Payment != nil {
		mux.HandleFunc("GET /api/v1/payments", deps.Payment.List)
		mux.HandleFunc("POST /api/v1/payments", deps.Payment.Create)
		mux.HandleFunc("GET /api/v1/payments/{id}", deps.Payment.Get)
	}
	if deps.Gateway != nil {
		mux.HandleFunc("GET /api/v1/gateways", deps.Gateway.List)
	}
	if deps.TenantGateway != nil {
		mux.HandleFunc("GET /api/v1/tenants/{tenant_id}/gateways", deps.TenantGateway.List)
		mux.HandleFunc("GET /api/v1/tenants/{tenant_id}/gateways/{gateway_id}", deps.TenantGateway.Get)
		mux.HandleFunc("POST /api/v1/tenants/{tenant_id}/gateways", deps.TenantGateway.Upsert)
		mux.HandleFunc("DELETE /api/v1/tenants/{tenant_id}/gateways/{gateway_id}", deps.TenantGateway.Delete)
	}
	if deps.Refund != nil {
		mux.HandleFunc("POST /api/v1/payments/{id}/refunds", deps.Refund.Initiate)
	}
	if deps.Cancel != nil {
		mux.HandleFunc("POST /api/v1/payments/{id}/cancel", deps.Cancel.Cancel)
	}
	if deps.SSE != nil {
		mux.HandleFunc("GET /api/v1/payments/{id}/events", deps.SSE.Events)
	}
	if deps.Webhook != nil {
		mux.HandleFunc("POST /webhooks/gateway/{gateway_id}", deps.Webhook.Handle)
	}
	if deps.Dispute != nil {
		mux.HandleFunc("GET /api/v1/disputes", deps.Dispute.List)
		mux.HandleFunc("GET /api/v1/disputes/{id}", deps.Dispute.Get)
		mux.HandleFunc("POST /api/v1/disputes/{id}/evidence", deps.Dispute.SubmitEvidence)
		mux.HandleFunc("GET /api/v1/disputes/{id}/evidence", deps.Dispute.ListEvidence)
	}
	if deps.DeadLetter != nil {
		mux.HandleFunc("GET /api/v1/dead-letters", deps.DeadLetter.List)
		mux.HandleFunc("POST /api/v1/dead-letters/{id}/replay", deps.DeadLetter.Replay)
	}
	if deps.Reconciliation != nil {
		mux.HandleFunc("POST /api/v1/reconciliation/jobs", deps.Reconciliation.CreateJob)
		mux.HandleFunc("GET /api/v1/reconciliation/jobs", deps.Reconciliation.ListJobs)
		mux.HandleFunc("GET /api/v1/reconciliation/jobs/{id}", deps.Reconciliation.GetJob)
		mux.HandleFunc("POST /api/v1/reconciliation/jobs/{id}/run", deps.Reconciliation.RunJob)
		mux.HandleFunc("GET /api/v1/reconciliation/jobs/{id}/entries", deps.Reconciliation.GetEntries)
		mux.HandleFunc("POST /api/v1/reconciliation/jobs/{id}/entries/{entry_id}/resolve", deps.Reconciliation.ResolveEntry)
	}

	// Static UI (legacy, optional)
	if deps.UI != nil {
		mux.Handle("GET /", deps.UI)
	}

	// Build middleware chain
	chain := []func(http.Handler) http.Handler{
		middleware.RequestID,
		middleware.TraceID,
		middleware.RequestLog(deps.Logger),
		middleware.Recover(deps.Logger),
	}
	if deps.Auth != nil {
		chain = append(chain, middleware.Authenticate(deps.Auth, deps.Logger))
	}
	if deps.Limiter != nil {
		chain = append(chain, middleware.RateLimit(deps.Limiter, deps.RateLimit, deps.Logger))
	}
	if deps.ResponseCache != nil {
		chain = append(chain, middleware.ResponseCache(deps.ResponseCache, deps.Logger))
	}

	return middleware.Chain(mux, chain...)
}

// LoadCheckoutTemplates loads HTML templates for the checkout pages from the embedded filesystem.
func LoadCheckoutTemplates(fs http.FileSystem) (*template.Template, error) {
	tmpl := template.New("")

	files := []string{"checkout.html", "success.html", "failure.html"}
	for _, name := range files {
		f, err := fs.Open("checkout/" + name)
		if err != nil {
			return nil, err
		}
		data, err := io.ReadAll(f)
		_ = f.Close()
		if err != nil {
			return nil, err
		}
		if _, err := tmpl.New(name).Parse(string(data)); err != nil {
			return nil, err
		}
	}

	return tmpl, nil
}
