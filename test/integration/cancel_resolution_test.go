//go:build integration

package integration

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/crownroutes/payment-service/internal/adapters/gateways"
	"github.com/crownroutes/payment-service/internal/adapters/observability"
	"github.com/crownroutes/payment-service/internal/adapters/postgres"
	apppayment "github.com/crownroutes/payment-service/internal/app/payment"
	"github.com/crownroutes/payment-service/internal/domain/transaction"
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

	adapter := stripeAdapter(srv.URL)
	registry := gateways.NewRegistry()
	registry.Register("stripe", adapter)

	cfg := postgres.NewConfigStore(pg.DB)
	refundSvc := refundService(pg, registry)
	paymentSvc := apppayment.NewService(
		postgres.NewTransactionRepository(pg.DB),
		postgres.NewOutboxWriter(pg.DB),
		cfg,
		postgres.NewTransactor(pg.DB),
		postgres.NewLeaseRepository(pg.DB),
		registry,
		discardLogger(),
		observability.NewNoopMetrics(),
	)
	paymentSvc.SetCancelResolver(refundSvc)

	created := seedTransaction(t, pg, transaction.StatusPending, 150000, "stripe", "")

	repo := postgres.NewTransactionRepository(pg.DB)
	ok, err := repo.SetCancelIntent(ctx, created.ID, transaction.ActorTenant, transaction.CancelViaAPI)
	if err != nil || !ok {
		t.Fatalf("set cancel intent: ok=%v err=%v", ok, err)
	}

	processed, err := paymentSvc.ProcessPayment(ctx, created.ID)
	if err != nil {
		t.Fatalf("ProcessPayment: %v", err)
	}
	if processed.Status != transaction.StatusCaptured {
		t.Fatalf("expected CAPTURED, got %s", processed.Status)
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
	if status != string(transaction.StatusRefunded) {
		t.Errorf("expected the auto-refund to be REFUNDED, got %q", status)
	}

	persisted, err := postgres.NewTransactionRepository(pg.DB).GetByID(ctx, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.Status != transaction.StatusRefunded {
		t.Errorf("expected parent to end REFUNDED after full cancel-resolution refund, got %s", persisted.Status)
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
