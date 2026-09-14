//go:build integration

package integration

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/adapters/gateways"
	"github.com/crownroutes/payment-service/internal/adapters/observability"
	"github.com/crownroutes/payment-service/internal/adapters/postgres"
	appcancel "github.com/crownroutes/payment-service/internal/app/cancel"
	apprefund "github.com/crownroutes/payment-service/internal/app/refund"
	domainrefund "github.com/crownroutes/payment-service/internal/domain/refund"
	"github.com/crownroutes/payment-service/internal/domain/transaction"
	"github.com/crownroutes/payment-service/internal/ports"
	"github.com/crownroutes/payment-service/internal/testsupport"
)

func discardLogger() *observability.SlogLogger {
	return observability.NewSlogLoggerFromHandler(slog.NewJSONHandler(io.Discard, nil))
}

func refundService(pg *testsupport.PG, registry apprefund.GatewayRegistry) *apprefund.Service {
	return apprefund.NewService(
		postgres.NewTransactionRepository(pg.DB),
		postgres.NewRefundRepository(pg.DB),
		postgres.NewOutboxWriter(pg.DB),
		postgres.NewTransactor(pg.DB),
		registry,
		discardLogger(),
		observability.NewNoopMetrics(),
	)
}

func seedTransaction(t *testing.T, pg *testsupport.PG, status transaction.Status, amount int64, gatewayID, reference string) *transaction.Txn {
	t.Helper()
	repo := postgres.NewTransactionRepository(pg.DB)
	tr := postgres.NewTransactor(pg.DB)

	txn, err := transaction.New(uuid.New(), uuid.New(), amount, "BDT", transaction.PaymentMethodCard, gatewayID, uuid.New(), "b@e.com", "o", nil, 30, transaction.CaptureModeAuto)
	if err != nil {
		t.Fatal(err)
	}
	txn.Status = status
	txn.AttemptedGateway = gatewayID
	txn.GatewayReferenceID = reference
	if err := tr.WithinTx(context.Background(), func(ctx context.Context) error { return repo.Insert(ctx, txn) }); err != nil {
		t.Fatalf("seed transaction: %v", err)
	}
	return txn
}

func TestRefund_ConcurrentNoOverRefund(t *testing.T) {
	pg := testsupport.RequirePostgres(t)
	pg.Truncate(t, "transactions", "refunds", "outbox_events")
	ctx := context.Background()

	parent := seedTransaction(t, pg, transaction.StatusCaptured, 100000, "stripe", "")

	svc := refundService(pg, gateways.NewRegistry())

	const goroutines = 8
	var success, blocked int64
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := svc.InitiateRefund(ctx, apprefund.InitiateInput{
				TransactionID: parent.ID, Amount: 60000, Reason: "customer_request", InitiatedBy: "ops:1",
			})
			var over domainrefund.ErrOverRefund
			switch {
			case err == nil:
				atomic.AddInt64(&success, 1)
			case errors.As(err, &over):
				atomic.AddInt64(&blocked, 1)
			case errors.Is(err, postgres.ErrVersionConflict):
				atomic.AddInt64(&blocked, 1)
			default:
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	wg.Wait()

	// 60000 each against a 100000 original: only one can succeed.
	if success != 1 {
		t.Errorf("expected exactly 1 successful refund, got %d (blocked=%d)", success, blocked)
	}

	var total int64
	if err := pg.DB.Pool().QueryRow(ctx,
		"SELECT COALESCE(SUM(amount),0) FROM refunds WHERE transaction_id = $1 AND status <> 'REFUND_FAILED'", parent.ID,
	).Scan(&total); err != nil {
		t.Fatal(err)
	}
	if total > parent.Amount {
		t.Errorf("total active refunds %d exceeds original %d", total, parent.Amount)
	}
}

func TestRefund_FullFlowEndToEnd(t *testing.T) {
	pg := testsupport.RequirePostgres(t)
	pg.Truncate(t, "transactions", "refunds", "outbox_events")
	ctx := context.Background()

	parent := seedTransaction(t, pg, transaction.StatusCaptured, 100000, "stripe", "pi_ref")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/refunds") {
			_, _ = w.Write([]byte(`{"id":"re_1","status":"succeeded","amount":40000,"currency":"bdt"}`))
			return
		}
		_, _ = w.Write([]byte(`{"id":"pi_ref","status":"succeeded","amount":100000,"currency":"bdt"}`))
	}))
	defer srv.Close()

	adapter := stripeAdapter(srv.URL)
	warmAdapter(t, adapter, parent)
	registry := gateways.NewRegistry()
	registry.Register("stripe", adapter)
	svc := refundService(pg, registry)

	initiatedRes, err := svc.InitiateRefund(ctx, apprefund.InitiateInput{
		TransactionID: parent.ID, Amount: 40000, Reason: "customer_request", InitiatedBy: "ops:1",
	})
	if err != nil {
		t.Fatalf("InitiateRefund: %v", err)
	}
	initiated := initiatedRes.Refund

	processed, err := svc.ProcessRefund(ctx, initiated.ID)
	if err != nil {
		t.Fatalf("ProcessRefund: %v", err)
	}
	if processed.Status != domainrefund.StatusRefunded {
		t.Errorf("expected REFUNDED, got %s", processed.Status)
	}
	if processed.GatewayRefundID != "re_1" {
		t.Errorf("expected gateway refund id re_1, got %q", processed.GatewayRefundID)
	}

	reloaded, err := postgres.NewRefundRepository(pg.DB).GetByID(ctx, initiated.ID)
	if err != nil {
		t.Fatal(err)
	}
	if reloaded.Status != domainrefund.StatusRefunded || reloaded.ResolvedAt == nil {
		t.Errorf("persisted refund should be REFUNDED with ResolvedAt, got %s / %v", reloaded.Status, reloaded.ResolvedAt)
	}

	parentAfter, err := postgres.NewTransactionRepository(pg.DB).GetByID(ctx, parent.ID)
	if err != nil {
		t.Fatal(err)
	}
	if parentAfter.Status != transaction.StatusPartiallyRefunded {
		t.Errorf("parent should be PARTIALLY_REFUNDED after a partial refund, got %s", parentAfter.Status)
	}

	var refundEvents int
	events, perr := postgres.NewOutboxWriter(pg.DB).PollPending(ctx, testsupport.AllShards(), 10)
	if perr != nil {
		t.Fatalf("poll outbox: %v", perr)
	}
	for _, e := range events {
		if e.EventType == ports.EventTypeRefundInitiated || e.EventType == ports.EventTypeRefundSucceeded {
			refundEvents++
		}
	}
	if refundEvents != 2 {
		t.Errorf("expected REFUND_INITIATED + REFUND_SUCCEEDED in outbox, got %d refund events", refundEvents)
	}
}

func TestCancel_ConcurrentSingleIntent(t *testing.T) {
	pg := testsupport.RequirePostgres(t)
	pg.Truncate(t, "transactions")
	ctx := context.Background()

	txn := seedTransaction(t, pg, transaction.StatusProcessing, 150000, "stripe", "")
	repo := postgres.NewTransactionRepository(pg.DB)

	const goroutines = 16
	var winners int64
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, err := repo.SetCancelIntent(ctx, txn.ID, transaction.ActorTenant, transaction.CancelViaAPI)
			if err != nil {
				t.Errorf("set cancel intent: %v", err)
				return
			}
			if ok {
				atomic.AddInt64(&winners, 1)
			}
		}()
	}
	wg.Wait()

	if winners != 1 {
		t.Errorf("expected exactly 1 cancel-intent winner, got %d", winners)
	}
}

func TestCancelService_Outcomes(t *testing.T) {
	pg := testsupport.RequirePostgres(t)
	pg.Truncate(t, "transactions")
	ctx := context.Background()

	svc := appcancel.NewService(postgres.NewTransactionRepository(pg.DB), discardLogger(), observability.NewNoopMetrics())

	processing := seedTransaction(t, pg, transaction.StatusProcessing, 1000, "stripe", "")
	res, err := svc.Cancel(ctx, appcancel.CancelInput{TransactionID: processing.ID, By: transaction.ActorOps, Via: transaction.CancelViaOpsTool})
	if err != nil {
		t.Fatal(err)
	}
	if res.Outcome != appcancel.OutcomeRequested {
		t.Errorf("PROCESSING cancel should be CANCEL_REQUESTED, got %s", res.Outcome)
	}

	terminal := seedTransaction(t, pg, transaction.StatusCaptured, 1000, "stripe", "")
	res, err = svc.Cancel(ctx, appcancel.CancelInput{TransactionID: terminal.ID, By: transaction.ActorOps, Via: transaction.CancelViaOpsTool})
	if err != nil {
		t.Fatal(err)
	}
	if res.Outcome != appcancel.OutcomeAlreadyTerminal {
		t.Errorf("CAPTURED cancel should be ALREADY_TERMINAL, got %s", res.Outcome)
	}
}
