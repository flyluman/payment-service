package payment

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/domain/transaction"
	"github.com/crownroutes/payment-service/internal/ports"
)

func manualTxn() *transaction.Txn {
	t, _ := transaction.New(uuid.New(), uuid.New(), 150000, "BDT", transaction.PaymentMethodCard, "stripe", uuid.New(), "b@example.com", "order", nil, 30, transaction.CaptureModeManual)
	return t
}

func TestAuthorize_EmitsTerminalEventAndAuthorizes(t *testing.T) {
	txn := manualTxn()
	reg := &fakeRegistry{adapter: &fakeAdapter{resp: &ports.GatewayPaymentResponse{
		Status:             ports.GatewayPaymentStatusSucceeded,
		GatewayReferenceID: "pi_auth",
	}}}
	outbox := &fakeOutbox{}
	svc := NewService(seedRepo(txn), outbox, &fakeConfig{}, &fakeTransactor{}, &fakeLease{acquired: true}, reg, noopLogger{}, noopMetrics{})

	got, err := svc.Authorize(context.Background(), txn.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != transaction.StatusAuthorized {
		t.Fatalf("expected AUTHORIZED, got %s", got.Status)
	}
	if got.GatewayReferenceID != "pi_auth" {
		t.Errorf("expected gateway reference pi_auth, got %q", got.GatewayReferenceID)
	}
	if len(outbox.events) != 1 || outbox.events[0].EventType != ports.EventTypeTransactionAuthorized {
		t.Fatalf("expected single TRANSACTION_AUTHORIZED event, got %+v", outbox.events)
	}
}

func TestAuthorize_LeaseNotAcquiredReturnsCurrentState(t *testing.T) {
	txn := manualTxn()
	adapter := &fakeAdapter{resp: &ports.GatewayPaymentResponse{Status: ports.GatewayPaymentStatusSucceeded}}
	svc := NewService(seedRepo(txn), &fakeOutbox{}, &fakeConfig{}, &fakeTransactor{}, &fakeLease{acquired: false}, &fakeRegistry{adapter: adapter}, noopLogger{}, noopMetrics{})

	got, err := svc.Authorize(context.Background(), txn.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != transaction.StatusProcessing {
		t.Fatalf("expected PROCESSING (lease loser reloads), got %s", got.Status)
	}
}

func TestAuthorize_NonTerminalStaysProcessing(t *testing.T) {
	txn := manualTxn()
	reg := &fakeRegistry{adapter: &fakeAdapter{err: &ports.GatewayError{Category: ports.ErrorCategoryAmbiguous}}}
	svc := NewService(seedRepo(txn), &fakeOutbox{}, &fakeConfig{}, &fakeTransactor{}, &fakeLease{acquired: true}, reg, noopLogger{}, noopMetrics{})

	got, err := svc.Authorize(context.Background(), txn.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != transaction.StatusProcessing {
		t.Fatalf("expected PROCESSING on ambiguous response, got %s", got.Status)
	}
}

func TestCapture_EmitsTerminalEventAndCaptures(t *testing.T) {
	txn := manualTxn()
	txn.Status = transaction.StatusAuthorized
	reg := &fakeRegistry{adapter: &fakeAdapter{capture: &ports.GatewayCaptureResponse{
		Status:             ports.GatewayPaymentStatusSucceeded,
		GatewayReferenceID: "pi_cap",
	}}}
	outbox := &fakeOutbox{}
	svc := NewService(seedRepo(txn), outbox, &fakeConfig{}, &fakeTransactor{}, &fakeLease{acquired: true}, reg, noopLogger{}, noopMetrics{})

	got, err := svc.Capture(context.Background(), txn.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != transaction.StatusCaptured {
		t.Fatalf("expected CAPTURED, got %s", got.Status)
	}
	if len(outbox.events) != 1 || outbox.events[0].EventType != ports.EventTypeTransactionCaptured {
		t.Fatalf("expected single TRANSACTION_CAPTURED event, got %+v", outbox.events)
	}
}

func TestCapture_LeaseNotAcquiredReturnsCurrentState(t *testing.T) {
	txn := manualTxn()
	txn.Status = transaction.StatusAuthorized
	svc := NewService(seedRepo(txn), &fakeOutbox{}, &fakeConfig{}, &fakeTransactor{}, &fakeLease{acquired: false}, &fakeRegistry{adapter: &fakeAdapter{capture: &ports.GatewayCaptureResponse{Status: ports.GatewayPaymentStatusSucceeded}}}, noopLogger{}, noopMetrics{})

	got, err := svc.Capture(context.Background(), txn.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != transaction.StatusProcessing {
		t.Fatalf("expected PROCESSING (lease loser reloads), got %s", got.Status)
	}
}

func TestCapture_NonTerminalStaysProcessing(t *testing.T) {
	txn := manualTxn()
	txn.Status = transaction.StatusAuthorized
	reg := &fakeRegistry{adapter: &fakeAdapter{captureErr: &ports.GatewayError{Category: ports.ErrorCategoryNetworkTimeout}}}
	svc := NewService(seedRepo(txn), &fakeOutbox{}, &fakeConfig{}, &fakeTransactor{}, &fakeLease{acquired: true}, reg, noopLogger{}, noopMetrics{})

	got, err := svc.Capture(context.Background(), txn.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != transaction.StatusProcessing {
		t.Fatalf("expected PROCESSING on timeout, got %s", got.Status)
	}
}

func TestSettle_EmitsSettledEvent(t *testing.T) {
	txn := manualTxn()
	txn.Status = transaction.StatusCaptured
	outbox := &fakeOutbox{}
	svc := NewService(seedRepo(txn), outbox, &fakeConfig{}, &fakeTransactor{}, &fakeLease{acquired: true}, &fakeRegistry{}, noopLogger{}, noopMetrics{})

	got, err := svc.Settle(context.Background(), txn.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != transaction.StatusSettled {
		t.Fatalf("expected SETTLED, got %s", got.Status)
	}
	if len(outbox.events) != 1 || outbox.events[0].EventType != ports.EventTypeTransactionSettled {
		t.Fatalf("expected single TRANSACTION_SETTLED event, got %+v", outbox.events)
	}
}
