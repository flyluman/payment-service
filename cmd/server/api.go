package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"time"

	"github.com/crownroutes/payment-service/config"
	"github.com/crownroutes/payment-service/internal/adapters/postgres"
	"github.com/crownroutes/payment-service/internal/adapters/security"
	"github.com/crownroutes/payment-service/internal/adapters/valkey"
	"github.com/crownroutes/payment-service/internal/api"
	"github.com/crownroutes/payment-service/internal/api/handlers"
	"github.com/crownroutes/payment-service/internal/api/middleware"
	"github.com/crownroutes/payment-service/internal/domain/gateway"
	"github.com/crownroutes/payment-service/web"
)

func startAPI(ctx context.Context, d *deps) error {
	cfg := d.cfg
	logger := d.logger

	var authProvider middleware.TokenProvider
	tokenProvider := middleware.NewStaticTokenProvider(
		config.ParseKeyValues(cfg.Security.ServiceTokens),
		config.SplitEnv(cfg.Security.OpsTokens),
	)
	if tokenProvider.HasAny() {
		authProvider = tokenProvider
		logger.Info("auth.enabled", nil)
	} else {
		return fmt.Errorf("no auth tokens configured: set SERVICE_TOKENS or OPS_TOKENS")
	}

	tenantGatewayHandler := handlers.NewTenantGatewayHandler(d.tenantConfigStore)
	twhConfigHandler := handlers.NewTenantWebhookConfigHandler(postgres.NewTenantWebhookConfigStore(d.db))

	uiFS, _ := fs.Sub(web.StaticFiles, "static")
	uiHandler := http.FileServer(http.FS(uiFS))

	tmplFS, _ := fs.Sub(web.StaticFiles, "static")
	templates, err := api.LoadCheckoutTemplates(http.FS(tmplFS))
	if err != nil {
		return fmt.Errorf("load checkout templates: %w", err)
	}

	payHandler := handlers.NewPayHandler(d.txnRepo, templates, logger, d.paymentSvc)
	sseHandler := handlers.NewSSEHandler(d.eventBus, d.txnRepo, logger)

	handler := api.NewRouter(api.Deps{
		Payment:           handlers.NewPaymentHandler(d.paymentSvc),
		Pay:               payHandler,
		Gateway:           handlers.NewGatewayHandler(d.gatewaySvc, d.configStore),
		TenantGateway:     tenantGatewayHandler,
		Refund:            handlers.NewRefundHandler(d.refundSvc, d.txnRepo),
		Cancel:            handlers.NewCancelHandler(d.cancelSvc),
		Webhook:           handlers.NewWebhookHandler(d.webhookSvc, d.registry, d.txnRepo, d.tenantConfigStore, d.disputeSvc, logger),
		Dispute:           handlers.NewDisputeHandler(d.disputeSvc),
		DeadLetter:        handlers.NewDeadLetterHandler(d.outboxWriter),
		Reconciliation:    handlers.NewReconciliationHandler(d.reconSvc),
		SSE:               sseHandler,
		TenantWebhookConf: twhConfigHandler,
		UI:                uiHandler,
		Health: handlers.NewHealthHandler(
			handlers.Check{Name: "database", Pinger: d.db},
			handlers.Check{Name: "valkey", Pinger: d.valkeyClient},
		),
		Logger:        logger,
		Auth:          authProvider,
		Limiter:       limiterAdapter{rl: d.rateLimiter},
		ResponseCache: d.responseCache,
		RateLimit: middleware.RateLimitConfig{
			Capacity:     cfg.RateLimit.Capacity,
			RefillPerSec: cfg.RateLimit.RefillPerSec,
		},
	})

	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.App.Port),
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}

	var tlsMgr *security.Manager
	if cfg.Security.TLSCertFile != "" {
		var err error
		if !cfg.App.MtlsStrictMode {
			logger.Warn("mtls.optional_mode", map[string]any{
				"warning": "MTLS_STRICT_MODE=false: client certificates are requested but not enforced",
			})
		}
		tlsMgr, err = security.NewManager(security.Config{
			CertFile:        cfg.Security.TLSCertFile,
			KeyFile:         cfg.Security.TLSKeyFile,
			CAFile:          cfg.Security.TLSCAFile,
			OptionalMTLS:    !cfg.App.MtlsStrictMode,
			RefreshInterval: time.Duration(cfg.Security.CertRefreshIntervalSec) * time.Second,
		}, logger, d.metrics)
		if err != nil {
			return fmt.Errorf("configure mTLS: %w", err)
		}
		srv.TLSConfig = tlsMgr.TLSConfig()

		refreshCtx, cancelRefresh := context.WithCancel(context.Background())
		defer cancelRefresh()
		tlsMgr.StartRefresh(refreshCtx)
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	logger.Info("http.server_starting", map[string]any{
		"port": cfg.App.Port,
		"tls":  tlsMgr != nil,
		"mtls": tlsMgr != nil && cfg.App.MtlsStrictMode,
	})

	var serveErr error
	if tlsMgr != nil {
		serveErr = srv.ListenAndServeTLS("", "")
	} else {
		serveErr = srv.ListenAndServe()
	}
	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		return fmt.Errorf("http server: %w", serveErr)
	}
	logger.Info("http.server_stopped", nil)
	return nil
}

type circuitBreakerAdapter struct {
	store     *valkey.CircuitBreakerStore
	threshold int
}

func (a circuitBreakerAdapter) RecordFailure(ctx context.Context, gatewayID string) error {
	_, _, err := a.store.RecordFailure(ctx, gatewayID, a.threshold)
	return err
}
func (a circuitBreakerAdapter) RecordSuccess(ctx context.Context, gatewayID string) error {
	return a.store.RecordSuccess(ctx, gatewayID)
}

type gatewaysBreakerAdapter struct {
	store *valkey.CircuitBreakerStore
}

func (a gatewaysBreakerAdapter) IsRoutable(ctx context.Context, gatewayID string) (bool, error) {
	cb, err := a.store.Get(ctx, gatewayID)
	if err != nil {
		return true, err
	}
	if cb.IsRoutable() {
		return true, nil
	}
	if !cb.ShouldTransitionToHalfOpen() {
		return false, nil
	}
	// Lazy recovery: the cooldown has expired but nothing has re-opened the
	// circuit, so atomically move OPEN → HALF_OPEN and let the next real call
	// be the probe. The Lua transition is the single-writer: concurrent
	// callers lose and report not routable.
	if err := a.store.Transition(ctx, cb, gateway.StateHalfOpen); err != nil {
		return false, nil
	}
	return true, nil
}

type limiterAdapter struct{ rl *valkey.RateLimiter }

func (a limiterAdapter) Allow(ctx context.Context, userID, tenantID, ip string, capacity int64, ratePerSec float64) middleware.RateDecision {
	res := a.rl.Allow(ctx, userID, tenantID, ip, capacity, ratePerSec)
	return middleware.RateDecision{Allowed: res.Allowed, RetryAfter: res.RetryAfter}
}
