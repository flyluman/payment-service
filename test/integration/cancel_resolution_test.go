//go:build integration

package integration

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/crownroutes/payment-service/internal/adapters/gateways"
	"github.com/crownroutes/payment-service/internal/adapters/gateways/stripe"
	"github.com/crownroutes/payment-service/internal/adapters/observability"
	"github.com/crownroutes/payment-service/internal/adapters/postgres"
	apppayment "github.com/crownroutes/payment-service/internal/app/payment"
	"github.com/crownroutes/payment-service/internal/domain/payment"
	"github.com/crownroutes/payment-service/internal/testsupport"
)

func TestCancelResolution_SucceededWithIntentAutoRefunds(t *testing.T) {
	pg := testsupport.RequirePostgres(t)
	pg.Truncate(t, "transactions", "refunds", "processing_lease", "outbox_events")
	testsupport.SeedStripeCardGateway(t, pg)
	ctx := context.Background()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/refunds") {
			_, _ = w.Write([]byte(`{"id":"re_cancel","status":"succeeded","amount":150000,"currency":"bdt"}`))
			return
		}
		_, _ = w.Write([]byte(`{"id":"pi_cancel","status":"succeeded","amount":150000,"currency":"bdt"}`))
	}))
	defer srv.Close()

	registry := gateways.NewRegistry()
	registry.Register("stripe", stripe.New(stripe.Config{APIKey: "sk_test", BaseURL: srv.URL}))

	cfg := postgres.NewConfigStore(pg.DB, pg.Q)
	refundSvc := refundService(pg, registry)
	paymentSvc := apppayment.NewService(
		postgres.NewPaymentRepository(pg.DB, pg.Q),
		postgres.NewOutboxWriter(pg.DB, pg.Q),
		cfg,
		postgres.NewTransactor(pg.DB),
		postgres.NewLeaseRepository(pg.DB, pg.Q),
		registry,
		discardLogger(),
		observability.NewNoopMetrics(),
	)
	paymentSvc.SetCancelResolver(refundSvc)

	createdRes, err := paymentSvc.Checkout(ctx, cardInput())
	if err != nil {
		t.Fatalf("Checkout: %v", err)
	}
	created := createdRes.Transaction

	repo := postgres.NewPaymentRepository(pg.DB, pg.Q)
	ok, err := repo.SetCancelIntent(ctx, created.ID, payment.ActorTenant, payment.CancelViaAPI)
	if err != nil || !ok {
		t.Fatalf("set cancel intent: ok=%v err=%v", ok, err)
	}

	processed, err := paymentSvc.ProcessPayment(ctx, created.ID)
	if err != nil {
		t.Fatalf("ProcessPayment: %v", err)
	}
	if processed.Status != payment.StatusSucceeded {
		t.Fatalf("expected SUCCEEDED, got %s", processed.Status)
	}

	var count int
	var status string
	err = pg.DB.Pool().QueryRow(ctx,
		"SELECT count(*), COALESCE(max(status),'') FROM refunds WHERE transaction_id = $1 AND reason = 'cancel_resolution'", created.ID,
	).Scan(&count, &status)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("expected exactly 1 cancel_resolution refund, got %d", count)
	}
	if status != string(payment.StatusRefunded) {
		t.Errorf("expected the auto-refund to be REFUNDED, got %q", status)
	}

	// Re-processing is a no-op and must not create a second resolution refund.
	if _, err := paymentSvc.ProcessPayment(ctx, created.ID); err != nil {
		t.Fatal(err)
	}
	_ = pg.DB.Pool().QueryRow(ctx,
		"SELECT count(*) FROM refunds WHERE transaction_id = $1 AND reason = 'cancel_resolution'", created.ID,
	).Scan(&count)
	if count != 1 {
		t.Errorf("cancel resolution must be idempotent, got %d refunds", count)
	}
}
