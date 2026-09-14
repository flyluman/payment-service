package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/crownroutes/payment-service/config"
	"github.com/crownroutes/payment-service/internal/adapters/broadcast"
	"github.com/crownroutes/payment-service/internal/adapters/encryption"
	"github.com/crownroutes/payment-service/internal/adapters/gateways"
	notifadapter "github.com/crownroutes/payment-service/internal/adapters/notification"
	"github.com/crownroutes/payment-service/internal/adapters/observability"
	"github.com/crownroutes/payment-service/internal/adapters/postgres"
	"github.com/crownroutes/payment-service/internal/adapters/valkey"
	"github.com/crownroutes/payment-service/internal/app/cancel"
	"github.com/crownroutes/payment-service/internal/app/dispute"
	"github.com/crownroutes/payment-service/internal/app/gateway"
	"github.com/crownroutes/payment-service/internal/app/idempotency"
	notifapp "github.com/crownroutes/payment-service/internal/app/notification"
	"github.com/crownroutes/payment-service/internal/app/payment"
	reconapp "github.com/crownroutes/payment-service/internal/app/reconciliation"
	"github.com/crownroutes/payment-service/internal/app/refund"
	twhapp "github.com/crownroutes/payment-service/internal/app/tenantwebhook"
	"github.com/crownroutes/payment-service/internal/app/webhook"
	"github.com/crownroutes/payment-service/internal/bootstrap"
	"github.com/crownroutes/payment-service/internal/ports"
)

// deps holds every shared resources initialised once in main and passed to each
// subsystem goroutine. Nothing here is goroutine-safe on its own; the goroutines
// receive a read-only view and use their own synchronisation internally.
type deps struct {
	cfg     *config.Config
	logger  *observability.SlogLogger
	metrics ports.MetricRecorder

	db *postgres.DB

	txnRepo           *postgres.TransactionRepository
	refundRepo        *postgres.RefundRepository
	outboxWriter      *postgres.OutboxWriter
	leaseRepo         *postgres.LeaseRepository
	transactor        *postgres.Transactor
	configStore       *postgres.ConfigStore
	idempotencyRepo   *postgres.IdempotencyRepository
	webhookRepo       *postgres.WebhookRepository
	tenantConfigStore *postgres.TenantConfigStore

	registry   *gateways.Registry
	gatewaySvc *gateway.Service
	paymentSvc *payment.Service
	refundSvc  *refund.Service
	cancelSvc  *cancel.Service
	webhookSvc *webhook.Service
	disputeSvc *dispute.Service
	notifSvc   *notifapp.Service
	reconSvc   *reconapp.Service

	valkeyClient  *valkey.Client
	rateLimiter   *valkey.RateLimiter
	cbStore       *valkey.CircuitBreakerStore
	intentStore   *valkey.IntentStore
	responseCache *valkey.ResponseCacheStore

	eventBus *broadcast.InMemoryBus
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

func hostname() string {
	h, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return h
}

func run() error {
	// ── Config ───────────────────────────────────────────────────────────
	cfg, err := config.LoadConfig()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	// Root context: cancelled on SIGINT / SIGTERM so every goroutine drains.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// ── Observability (logger + metrics + traces) ────────────────────────
	obs, err := observability.New(ctx, cfg, bootstrap.ParseLogLevel(cfg.Observability.LogLevel), hostname())
	if err != nil {
		return fmt.Errorf("init observability: %w", err)
	}
	defer func() {
		flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = obs.Close(flushCtx)
	}()
	logger := obs.Logger
	metrics := obs.Metrics

	// ── Postgres ─────────────────────────────────────────────────────────
	connectPolicy := bootstrap.RetryPolicy{
		Attempts:       cfg.Startup.ConnectMaxAttempts,
		AttemptTimeout: cfg.Startup.ConnectAttemptTimeout,
		Backoff:        cfg.Startup.ConnectBackoff,
	}

	var db *postgres.DB
	if err := bootstrap.Connect(ctx, logger, "postgres", connectPolicy, func(c context.Context) error {
		db, err = postgres.New(c, cfg.Database)
		return err
	}); err != nil {
		return err
	}
	defer db.Close()

	// ── Valkey ───────────────────────────────────────────────────────────
	valkeyClient, err := valkey.New(cfg.Valkey)
	if err != nil {
		return fmt.Errorf("connect valkey: %w", err)
	}
	defer valkeyClient.Close()
	if err := bootstrap.Connect(ctx, logger, "valkey", connectPolicy, valkeyClient.Ping); err != nil {
		return err
	}
	rateLimiter := valkey.NewRateLimiter(valkeyClient, cfg.RateLimit, logger, metrics)
	defer rateLimiter.Close()

	// ── Repositories ─────────────────────────────────────────────────────
	txnRepo := postgres.NewTransactionRepository(db)
	txnRepo.SetCache(valkeyClient)
	refundRepo := postgres.NewRefundRepository(db)
	outboxWriter := postgres.NewOutboxWriter(db)
	leaseRepo := postgres.NewLeaseRepository(db)
	transactor := postgres.NewTransactor(db)
	configStore := postgres.NewConfigStore(db)
	configStore.SetCache(valkeyClient)
	idempotencyRepo := postgres.NewIdempotencyRepository(db)
	webhookRepo := postgres.NewWebhookRepository(db)
	var encryptor postgres.Encryptor
	if encKeyHex := cfg.Security.EncryptionKey; encKeyHex != "" {
		encKey, err := hex.DecodeString(encKeyHex)
		if err != nil {
			return fmt.Errorf("ENCRYPTION_KEY: hex decode: %w", err)
		}
		km, err := encryption.NewLocalKeyManager("master", encKey)
		if err != nil {
			return fmt.Errorf("init encryption key manager: %w", err)
		}
		encryptor = encryption.NewEnvelope(km, encryption.Config{})
		logger.Info("encryption.enabled", nil)
	} else if cfg.App.Environment == "production" {
		return fmt.Errorf("ENCRYPTION_KEY is required in production — gateway credentials must not be stored as plaintext")
	} else {
		logger.Warn("encryption.disabled", map[string]any{
			"reason": "ENCRYPTION_KEY not set — gateway credentials stored as plaintext",
		})
	}
	tenantConfigStore := postgres.NewTenantConfigStore(db, encryptor)

	// ── Services ─────────────────────────────────────────────────────────
	registry := bootstrap.GatewayRegistry(cfg, tenantConfigStore)

	paymentSvc := payment.NewService(
		txnRepo, outboxWriter, configStore, transactor, leaseRepo,
		registry, logger, metrics,
	)
	refundSvc := refund.NewService(txnRepo, refundRepo, outboxWriter, transactor, registry, logger, metrics)
	cancelSvc := cancel.NewService(txnRepo, logger, metrics)
	paymentSvc.SetCancelResolver(refundSvc)
	paymentSvc.SetGatewayMetadataStore(webhookRepo)

	webhookSvc := webhook.NewService(txnRepo, webhookRepo, outboxWriter, transactor, logger, metrics)
	webhookSvc.SetCancelResolver(refundSvc)

	disputeStore := postgres.NewDisputeStore(db)
	disputeEvidenceStore := postgres.NewDisputeEvidenceStore(db)
	disputeSvc := dispute.NewService(disputeStore, disputeEvidenceStore, txnRepo, transactor)
	disputeSvc.SetOutboxWriter(outboxWriter)

	notifStore := postgres.NewNotificationStore(db)
	notifTemplateStore := postgres.NewNotificationTemplateStore(db)
	notifPrefStore := postgres.NewNotificationPreferenceStore(db)
	notifEmailSender := notifadapter.NewSMTPEmailSender(notifadapter.SMTPConfig{
		Host:     cfg.Notification.SMTP.Host,
		Port:     cfg.Notification.SMTP.Port,
		Username: cfg.Notification.SMTP.Username,
		Password: cfg.Notification.SMTP.Password,
		From:     cfg.Notification.SMTP.From,
	})
	notifSMSSender := notifadapter.NewStubSMSSender(notifadapter.SMSConfig{
		Provider: cfg.Notification.SMS.Provider,
		APIKey:   cfg.Notification.SMS.APIKey,
		From:     cfg.Notification.SMS.From,
	})
	notifSvc := notifapp.NewService(notifStore, notifTemplateStore, notifPrefStore, notifEmailSender, notifSMSSender)

	// Wire notification service to domain services
	paymentSvc.SetNotificationService(notifSvc)
	webhookSvc.SetNotificationService(notifSvc)
	refundSvc.SetNotificationService(notifSvc)
	disputeSvc.SetNotificationService(notifSvc)

	// Wire audit log store
	auditStore := postgres.NewAuditLogStore(db)
	paymentSvc.SetAuditLogStore(auditStore)
	webhookSvc.SetAuditLogStore(auditStore)
	refundSvc.SetAuditLogStore(auditStore)
	disputeSvc.SetAuditLogStore(auditStore)
	cancelSvc.SetAuditLogStore(auditStore)

	// Wire tenant webhook dispatcher
	twhWriter := postgres.NewTenantWebhookWriter(db)
	twhConfigStore := postgres.NewTenantWebhookConfigStore(db)
	twhDispatcher := twhapp.NewDispatcher(twhWriter, twhConfigStore)
	paymentSvc.SetTenantWebhookDispatcher(twhDispatcher)
	webhookSvc.SetTenantWebhookDispatcher(twhDispatcher)
	refundSvc.SetTenantWebhookDispatcher(twhDispatcher)
	disputeSvc.SetTenantWebhookDispatcher(twhDispatcher)

	reconStore := postgres.NewReconciliationStore(db)
	reconSvc := reconapp.NewService(reconStore, txnRepo, txnRepo, registry, logger, metrics)

	// ── Event bus (SSE) ──────────────────────────────────────────────────
	eventBus := broadcast.NewInMemoryBus()
	paymentSvc.SetEventBus(eventBus)
	webhookSvc.SetEventBus(eventBus)
	refundSvc.SetEventBus(eventBus)
	disputeSvc.SetEventBus(eventBus)

	// ── Idempotency ──────────────────────────────────────────────────────
	idemGuard := idempotency.NewGuard(idempotencyRepo, transactor)
	paymentSvc.SetIdempotency(idemGuard)
	refundSvc.SetIdempotency(idemGuard)
	cancelSvc.SetIdempotency(idemGuard)

	// ── Circuit breaker + intent tracking ────────────────────────────────
	cbStore := valkey.NewCircuitBreakerStore(valkeyClient)
	paymentSvc.SetCircuitBreaker(circuitBreakerAdapter{
		store:     cbStore,
		threshold: cfg.Gateway.CircuitBreakerThreshold,
	})
	intentStore := valkey.NewIntentStore(valkeyClient)
	paymentSvc.SetIntentTracker(intentStore)

	responseCache := valkey.NewResponseCacheStore(valkeyClient)

	gatewaySvc := gateway.NewService(
		configStore,
		gatewaysBreakerAdapter{store: cbStore},
		logger,
		metrics,
	)

	d := &deps{
		cfg:     cfg,
		logger:  logger,
		metrics: metrics,

		db: db,

		txnRepo:           txnRepo,
		refundRepo:        refundRepo,
		outboxWriter:      outboxWriter,
		leaseRepo:         leaseRepo,
		transactor:        transactor,
		configStore:       configStore,
		idempotencyRepo:   idempotencyRepo,
		webhookRepo:       webhookRepo,
		tenantConfigStore: tenantConfigStore,

		registry:   registry,
		gatewaySvc: gatewaySvc,
		paymentSvc: paymentSvc,
		refundSvc:  refundSvc,
		cancelSvc:  cancelSvc,
		webhookSvc: webhookSvc,
		disputeSvc: disputeSvc,
		notifSvc:   notifSvc,
		reconSvc:   reconSvc,

		valkeyClient:  valkeyClient,
		rateLimiter:   rateLimiter,
		cbStore:       cbStore,
		intentStore:   intentStore,
		responseCache: responseCache,
		eventBus:      eventBus,
	}

	// ── Launch subsystems ────────────────────────────────────────────────
	logger.Info("service.starting", map[string]any{
		"port": cfg.App.Port,
	})

	var wg sync.WaitGroup
	errc := make(chan error, 3)

	wg.Add(3)
	go func() { defer wg.Done(); errc <- startAPI(ctx, d) }()
	go func() { defer wg.Done(); errc <- startRelay(ctx, d) }()
	go func() { defer wg.Done(); errc <- startJobs(ctx, d) }()

	// Wait for the first subsystem to return. A cancelled context (from signal)
	// makes all three return; we only care about the first real error.
	firstErr := <-errc
	stop() // propagate shutdown to the other two

	// Give the remaining goroutines a moment to drain, but don't block forever.
	go func() { wg.Wait(); close(errc) }()

	select {
	case <-errc:
	case <-time.After(15 * time.Second):
		logger.Warn("service.shutdown_timeout", nil)
	}

	logger.Info("service.stopped", nil)
	return firstErr
}
