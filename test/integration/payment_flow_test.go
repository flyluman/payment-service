//go:build integration

package integration

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/adapters/gateways"
	"github.com/crownroutes/payment-service/internal/adapters/gateways/stripe"
	"github.com/crownroutes/payment-service/internal/adapters/observability"
	"github.com/crownroutes/payment-service/internal/adapters/postgres"
	apppayment "github.com/crownroutes/payment-service/internal/app/payment"
	"github.com/crownroutes/payment-service/internal/domain/transaction"
	"github.com/crownroutes/payment-service/internal/ports"
	"github.com/crownroutes/payment-service/internal/testsupport"
)

func stripeAdapter(baseURL string) *stripe.Adapter {
	return stripe.New(stripe.Config{
		ResolveConfig: func(ctx context.Context, tenantID uuid.UUID) (*stripe.TenantConfig, error) {
			return &stripe.TenantConfig{APIKey: "sk_test", BaseURL: baseURL}, nil
		},
	})
}

// warmAdapter caches the tenant for a seeded transaction on the stripe adapter
// instance, mirroring the txnTenants map that InitiatePayment populates in
// production so later CheckStatus/Refund calls can resolve per-tenant config.
func warmAdapter(t *testing.T, adapter *stripe.Adapter, txn *transaction.Txn) {
	t.Helper()
	ctx := context.Background()
	if _, err := adapter.InitiatePayment(ctx, ports.GatewayPaymentRequest{
		TransactionID: txn.ID,
		TenantID:      txn.TenantID,
		Amount:        txn.Amount,
		Currency:      txn.Currency,
		PaymentMethod: txn.PaymentMethod,
	}); err != nil {
		t.Fatalf("warm adapter cache: %v", err)
	}
}

func buildService(pg *testsupport.PG, registry *gateways.Registry) *apppayment.Service {
	cfg := postgres.NewConfigStore(pg.DB)
	logger := observability.NewSlogLoggerFromHandler(slog.NewJSONHandler(io.Discard, nil))

	return apppayment.NewService(
		postgres.NewTransactionRepository(pg.DB),
		postgres.NewOutboxWriter(pg.DB),
		cfg,
		postgres.NewTransactor(pg.DB),
		postgres.NewLeaseRepository(pg.DB),
		registry,
		logger,
		observability.NewNoopMetrics(),
	)
}

func cardInput() apppayment.CreateInput {
	return apppayment.CreateInput{
		TenantID:      uuid.New(),
		UserID:        uuid.New(),
		GatewayID:     "stripe",
		Amount:        150000,
		Currency:      "BDT",
		PaymentMethod: transaction.PaymentMethodCard,
		CustomerEmail: "buyer@example.com",
		Description:   "integration order",
		CallbackURL:   "https://example.com/callback",
		RedirectURL:   "https://example.com/redirect",
	}
}

func outboxEventTypes(t *testing.T, pg *testsupport.PG) []string {
	t.Helper()
	events, err := postgres.NewOutboxWriter(pg.DB).PollPending(context.Background(), testsupport.AllShards(), 10)
	if err != nil {
		t.Fatalf("poll outbox: %v", err)
	}
	types := make([]string, 0, len(events))
	for _, e := range events {
		types = append(types, e.EventType)
	}
	return types
}

func contains(types []string, want string) bool {
	for _, ty := range types {
		if ty == want {
			return true
		}
	}
	return false
}

func TestPaymentFlow_CreateSyncInitiate(t *testing.T) {
	pg := testsupport.RequirePostgres(t)
	pg.Truncate(t, "transactions", "processing_lease", "outbox_events")
	testsupport.SeedStripeCardGateway(t, pg)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"pi_flow_ok","status":"succeeded","amount":150000,"currency":"bdt"}`))
	}))
	defer srv.Close()

	registry := gateways.NewRegistry()
	registry.Register("stripe", stripeAdapter(srv.URL))
	svc := buildService(pg, registry)
	ctx := context.Background()

	createdRes, err := svc.Create(ctx, cardInput())
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	created := createdRes.Transaction
	if created.Status != transaction.StatusProcessing {
		t.Errorf("expected PROCESSING after sync initiate, got %s", created.Status)
	}
	if created.GatewayReferenceID != "pi_flow_ok" {
		t.Errorf("expected gateway ref pi_flow_ok, got %q", created.GatewayReferenceID)
	}
	if created.GatewayID != "stripe" {
		t.Errorf("expected routing to select stripe, got %q", created.GatewayID)
	}
	if created.EstimatedTimeoutSeconds != 30 {
		t.Errorf("expected processing timeout 30s from config, got %d", created.EstimatedTimeoutSeconds)
	}

	types := outboxEventTypes(t, pg)
	if !contains(types, ports.EventTypeTransactionCreated) || !contains(types, ports.EventTypeGatewayInitiate) {
		t.Errorf("expected TRANSACTION_CREATED + GATEWAY_INITIATE in outbox, got %v", types)
	}
}

func TestPaymentFlow_SuccessEndToEnd(t *testing.T) {
	pg := testsupport.RequirePostgres(t)
	pg.Truncate(t, "transactions", "processing_lease", "outbox_events")
	testsupport.SeedStripeCardGateway(t, pg)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"pi_flow_ok","status":"succeeded","amount":150000,"currency":"bdt"}`))
	}))
	defer srv.Close()

	registry := gateways.NewRegistry()
	registry.Register("stripe", stripeAdapter(srv.URL))
	svc := buildService(pg, registry)
	ctx := context.Background()

	created := seedTransaction(t, pg, transaction.StatusPending, 150000, "stripe", "")

	processed, err := svc.ProcessPayment(ctx, created.ID)
	if err != nil {
		t.Fatalf("ProcessPayment: %v", err)
	}
	if processed.Status != transaction.StatusCaptured {
		t.Errorf("expected CAPTURED, got %s", processed.Status)
	}
	if processed.GatewayReferenceID != "pi_flow_ok" || processed.ActualGateway != "stripe" {
		t.Errorf("expected gateway ref pi_flow_ok / actual stripe, got %q / %q", processed.GatewayReferenceID, processed.ActualGateway)
	}
	persisted, err := postgres.NewTransactionRepository(pg.DB).GetByID(ctx, created.ID)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if persisted.Status != transaction.StatusCaptured {
		t.Errorf("persisted status should be CAPTURED, got %s", persisted.Status)
	}

	types := outboxEventTypes(t, pg)
	if len(types) != 1 || !contains(types, ports.EventTypeTransactionCaptured) {
		t.Errorf("expected TRANSACTION_CAPTURED in outbox, got %v", types)
	}
}

func TestPaymentFlow_GatewayDecline(t *testing.T) {
	pg := testsupport.RequirePostgres(t)
	pg.Truncate(t, "transactions", "processing_lease", "outbox_events")
	testsupport.SeedStripeCardGateway(t, pg)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusPaymentRequired)
		_, _ = w.Write([]byte(`{"error":{"type":"card_error","code":"card_declined","decline_code":"generic_decline","message":"declined"}}`))
	}))
	defer srv.Close()

	registry := gateways.NewRegistry()
	registry.Register("stripe", stripeAdapter(srv.URL))
	svc := buildService(pg, registry)
	ctx := context.Background()

	created := seedTransaction(t, pg, transaction.StatusPending, 150000, "stripe", "")

	processed, err := svc.ProcessPayment(ctx, created.ID)
	if err != nil {
		t.Fatalf("ProcessPayment: %v", err)
	}
	if processed.Status != transaction.StatusFailed {
		t.Errorf("expected FAILED on decline, got %s", processed.Status)
	}
	if processed.FailureReason == nil {
		t.Error("expected failure reason recorded")
	}

	types := outboxEventTypes(t, pg)
	if !contains(types, ports.EventTypeTransactionFailed) {
		t.Errorf("expected TRANSACTION_FAILED in outbox, got %v", types)
	}
}

func TestPaymentFlow_NoEligibleGateway(t *testing.T) {
	pg := testsupport.RequirePostgres(t)
	pg.Truncate(t, "transactions", "processing_lease", "outbox_events")
	testsupport.SeedStripeCardGateway(t, pg)

	registry := gateways.NewRegistry()
	registry.Register("stripe", stripeAdapter("http://unused"))
	svc := buildService(pg, registry)

	in := cardInput()
	in.GatewayID = ""

	_, err := svc.Create(context.Background(), in)
	if !errors.Is(err, apppayment.ErrNoGateway) {
		t.Fatalf("expected ErrNoGateway for missing gateway, got %v", err)
	}
}
