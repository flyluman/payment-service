//go:build integration

package integration

import (
	"context"
	"testing"

	"github.com/crownroutes/payment-service/internal/adapters/observability"
	"github.com/crownroutes/payment-service/internal/adapters/postgres"
	appwebhook "github.com/crownroutes/payment-service/internal/app/webhook"
	"github.com/crownroutes/payment-service/internal/domain/transaction"
	"github.com/crownroutes/payment-service/internal/testsupport"
)

func TestWebhook_ResolvesProcessingTransaction(t *testing.T) {
	pg := testsupport.RequirePostgres(t)
	pg.Truncate(t, "transactions", "webhook_events", "transaction_gateway_metadata", "outbox_events")
	ctx := context.Background()

	txn := seedTransaction(t, pg, transaction.StatusProcessing, 150000, "razorpay", "order_int")

	svc := appwebhook.NewService(
		postgres.NewTransactionRepository(pg.DB),
		postgres.NewWebhookRepository(pg.DB),
		postgres.NewOutboxWriter(pg.DB),
		postgres.NewTransactor(pg.DB),
		discardLogger(),
		observability.NewNoopMetrics(),
	)

	out, err := svc.Process(ctx, "razorpay", appwebhook.Event{
		EventID: "evt_int_1", GatewayReferenceID: "order_int", Status: "success",
	}, []byte(`{"raw":"payload"}`))
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	if !out.Resolved || out.Status != transaction.StatusCaptured {
		t.Fatalf("expected resolved to CAPTURED, got %+v", out)
	}

	persisted, _ := postgres.NewTransactionRepository(pg.DB).GetByID(ctx, txn.ID)
	if persisted.Status != transaction.StatusCaptured {
		t.Errorf("transaction should be CAPTURED in DB, got %s", persisted.Status)
	}

	var rawCount, eventCount int
	_ = pg.DB.Pool().QueryRow(ctx, "SELECT count(*) FROM transaction_gateway_metadata WHERE transaction_id=$1", txn.ID).Scan(&rawCount)
	_ = pg.DB.Pool().QueryRow(ctx, "SELECT count(*) FROM outbox_events WHERE aggregate_id=$1", txn.ID).Scan(&eventCount)
	if rawCount != 1 {
		t.Errorf("expected 1 raw metadata row, got %d", rawCount)
	}
	if eventCount != 1 {
		t.Errorf("expected 1 outbox event, got %d", eventCount)
	}

	// Replaying the same event must be an idempotent no-op.
	again, err := svc.Process(ctx, "razorpay", appwebhook.Event{
		EventID: "evt_int_1", GatewayReferenceID: "order_int", Status: "success",
	}, []byte(`{"raw":"payload"}`))
	if err != nil {
		t.Fatal(err)
	}
	if !again.Duplicate {
		t.Errorf("replayed webhook should be a duplicate no-op, got %+v", again)
	}
	_ = pg.DB.Pool().QueryRow(ctx, "SELECT count(*) FROM transaction_gateway_metadata WHERE transaction_id=$1", txn.ID).Scan(&rawCount)
	if rawCount != 1 {
		t.Errorf("duplicate webhook must not insert a second raw metadata row, got %d", rawCount)
	}
}
