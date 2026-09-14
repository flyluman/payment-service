package webhook

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/domain/transaction"
	"github.com/crownroutes/payment-service/internal/ports"
)

type fakeTxns struct {
	txn     *transaction.Txn
	updates int
}

func (f *fakeTxns) GetByGatewayReference(context.Context, string, string) (*transaction.Txn, error) {
	return f.txn, nil
}
func (f *fakeTxns) UpdateStatus(_ context.Context, t *transaction.Txn) error {
	f.updates++
	return nil
}

type fakeWebhooks struct {
	recorded bool
	rawCount int
}

func (f *fakeWebhooks) RecordEvent(context.Context, string, string) (bool, error) {
	return f.recorded, nil
}
func (f *fakeWebhooks) InsertGatewayMetadata(context.Context, uuid.UUID, string, []byte) error {
	f.rawCount++
	return nil
}

type fakeOutbox struct{ events []ports.OutboxEvent }

func (f *fakeOutbox) Write(_ context.Context, e ports.OutboxEvent) error {
	f.events = append(f.events, e)
	return nil
}

type fakeTransactor struct{}

func (fakeTransactor) WithinTx(ctx context.Context, fn func(context.Context) error) error {
	return fn(ctx)
}

type noopLogger struct{}

func (noopLogger) Info(string, map[string]any)         {}
func (noopLogger) Warn(string, map[string]any)         {}
func (noopLogger) Error(string, map[string]any, error) {}
func (noopLogger) Debug(string, map[string]any)        {}
func (noopLogger) Trace(string, map[string]any)        {}
func (l noopLogger) With(map[string]any) ports.Logger  { return l }

type noopMetrics struct{}

func (noopMetrics) Increment(string, map[string]string)          {}
func (noopMetrics) Histogram(string, float64, map[string]string) {}
func (noopMetrics) Gauge(string, float64, map[string]string)     {}

func processingTxn() *transaction.Txn {
	t, _ := transaction.New(uuid.New(), uuid.New(), 150000, "BDT", transaction.PaymentMethodCard, "razorpay", uuid.New(), "b@e.com", "o", nil, 30, transaction.CaptureModeAuto)
	t.Status = transaction.StatusProcessing
	t.GatewayReferenceID = "order_1"
	return t
}

func TestProcess_ResolvesProcessingToSucceeded(t *testing.T) {
	txn := processingTxn()
	txns := &fakeTxns{txn: txn}
	webhooks := &fakeWebhooks{recorded: true}
	outbox := &fakeOutbox{}
	s := NewService(txns, webhooks, outbox, fakeTransactor{}, noopLogger{}, noopMetrics{})

	out, err := s.Process(context.Background(), "razorpay", Event{EventID: "evt", GatewayReferenceID: "order_1", Status: "success"}, []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if !out.Resolved || out.Status != transaction.StatusCaptured {
		t.Errorf("expected resolved to CAPTURED, got %+v", out)
	}
	if txns.updates != 1 || len(outbox.events) != 1 || webhooks.rawCount != 1 {
		t.Errorf("expected update+event+raw metadata, got updates=%d events=%d raw=%d", txns.updates, len(outbox.events), webhooks.rawCount)
	}
	if outbox.events[0].EventType != ports.EventTypeTransactionCaptured {
		t.Errorf("expected PAYMENT_SUCCEEDED, got %s", outbox.events[0].EventType)
	}
}

func TestProcess_DuplicateIsNoop(t *testing.T) {
	txns := &fakeTxns{txn: processingTxn()}
	webhooks := &fakeWebhooks{recorded: false} // already recorded -> duplicate
	outbox := &fakeOutbox{}
	s := NewService(txns, webhooks, outbox, fakeTransactor{}, noopLogger{}, noopMetrics{})

	out, err := s.Process(context.Background(), "razorpay", Event{EventID: "evt", GatewayReferenceID: "order_1", Status: "success"}, []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if !out.Duplicate || txns.updates != 0 || len(outbox.events) != 0 {
		t.Errorf("duplicate webhook must be a no-op, got %+v updates=%d events=%d", out, txns.updates, len(outbox.events))
	}
}

func TestProcess_UnknownTransaction(t *testing.T) {
	txns := &fakeTxns{txn: nil} // not found
	s := NewService(txns, &fakeWebhooks{recorded: true}, &fakeOutbox{}, fakeTransactor{}, noopLogger{}, noopMetrics{})

	out, err := s.Process(context.Background(), "razorpay", Event{EventID: "evt", GatewayReferenceID: "nope", Status: "success"}, []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if !out.UnknownTxn {
		t.Errorf("expected UnknownTxn, got %+v", out)
	}
}

func TestProcess_NonTerminalStatusNoTransition(t *testing.T) {
	txn := processingTxn()
	txns := &fakeTxns{txn: txn}
	s := NewService(txns, &fakeWebhooks{recorded: true}, &fakeOutbox{}, fakeTransactor{}, noopLogger{}, noopMetrics{})

	out, err := s.Process(context.Background(), "razorpay", Event{EventID: "evt", GatewayReferenceID: "order_1", Status: "pending"}, []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if out.Resolved || txns.updates != 0 {
		t.Errorf("a non-terminal webhook should not transition, got %+v updates=%d", out, txns.updates)
	}
}
