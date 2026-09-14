package tenantwebhook

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/ports"
)

type fakeWebhookWriter struct {
	deliveries []ports.TenantWebhookDelivery
	err        error
}

func (f *fakeWebhookWriter) WriteDelivery(_ context.Context, d ports.TenantWebhookDelivery) error {
	if f.err != nil {
		return f.err
	}
	f.deliveries = append(f.deliveries, d)
	return nil
}

type fakeWebhookConfigStore struct {
	cfg *ports.TenantWebhookConfig
	err error
}

func (f *fakeWebhookConfigStore) Get(_ context.Context, tenantID uuid.UUID) (*ports.TenantWebhookConfig, error) {
	if f.err != nil {
		return nil, f.err
	}
	if f.cfg != nil && f.cfg.TenantID == uuid.Nil {
		f.cfg.TenantID = tenantID
	}
	return f.cfg, nil
}

func TestDispatch_ActiveConfigWritesDelivery(t *testing.T) {
	writer := &fakeWebhookWriter{}
	d := NewDispatcher(writer, &fakeWebhookConfigStore{cfg: &ports.TenantWebhookConfig{
		EndpointURL: "https://example.com/hook",
		IsActive:    true,
	}})

	tenantID := uuid.New()
	txnID := uuid.New()
	payload := []byte(`{"ok":true}`)

	err := d.Dispatch(context.Background(), tenantID, txnID, "PAYMENT_SUCCESS", payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(writer.deliveries) != 1 {
		t.Fatalf("expected 1 delivery, got %d", len(writer.deliveries))
	}
	got := writer.deliveries[0]
	if got.TenantID != tenantID || got.TransactionID != txnID {
		t.Errorf("delivery ids wrong: tenant=%s txn=%s", got.TenantID, got.TransactionID)
	}
	if got.EventType != "PAYMENT_SUCCESS" {
		t.Errorf("unexpected event type: %s", got.EventType)
	}
	if got.EndpointURL != "https://example.com/hook" {
		t.Errorf("unexpected endpoint: %s", got.EndpointURL)
	}
	if string(got.Payload) != string(payload) {
		t.Error("payload not passed through")
	}
}

func TestDispatch_InactiveConfigSkipsDelivery(t *testing.T) {
	writer := &fakeWebhookWriter{}
	d := NewDispatcher(writer, &fakeWebhookConfigStore{cfg: &ports.TenantWebhookConfig{
		EndpointURL: "https://example.com/hook",
		IsActive:    false,
	}})

	err := d.Dispatch(context.Background(), uuid.New(), uuid.New(), "PAYMENT_SUCCESS", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(writer.deliveries) != 0 {
		t.Fatalf("inactive config must not enqueue deliveries, got %d", len(writer.deliveries))
	}
}

func TestDispatch_NoEndpointSkipsDelivery(t *testing.T) {
	writer := &fakeWebhookWriter{}
	d := NewDispatcher(writer, &fakeWebhookConfigStore{cfg: &ports.TenantWebhookConfig{
		EndpointURL: "",
		IsActive:    true,
	}})

	err := d.Dispatch(context.Background(), uuid.New(), uuid.New(), "PAYMENT_SUCCESS", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(writer.deliveries) != 0 {
		t.Fatalf("missing endpoint must not enqueue deliveries, got %d", len(writer.deliveries))
	}
}

func TestDispatch_MissingConfigSkipsDelivery(t *testing.T) {
	writer := &fakeWebhookWriter{}
	d := NewDispatcher(writer, &fakeWebhookConfigStore{cfg: nil})

	err := d.Dispatch(context.Background(), uuid.New(), uuid.New(), "PAYMENT_SUCCESS", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(writer.deliveries) != 0 {
		t.Fatalf("missing config must not enqueue deliveries, got %d", len(writer.deliveries))
	}
}

func TestDispatch_ConfigStoreErrorPropagates(t *testing.T) {
	writer := &fakeWebhookWriter{}
	d := NewDispatcher(writer, &fakeWebhookConfigStore{err: errors.New("db down")})

	err := d.Dispatch(context.Background(), uuid.New(), uuid.New(), "PAYMENT_SUCCESS", nil)
	if err == nil {
		t.Fatal("expected config store error to propagate")
	}
	if len(writer.deliveries) != 0 {
		t.Fatal("no delivery should be written on config store error")
	}
}

func TestDispatch_UninitializedDispatcherIsNoOp(t *testing.T) {
	d := &Dispatcher{}
	if err := d.Dispatch(context.Background(), uuid.New(), uuid.New(), "PAYMENT_SUCCESS", nil); err != nil {
		t.Fatalf("dispatcher without dependencies should be a no-op, got %v", err)
	}
}

func TestDispatch_WriterErrorPropagates(t *testing.T) {
	d := NewDispatcher(&fakeWebhookWriter{err: errors.New("write failed")}, &fakeWebhookConfigStore{cfg: &ports.TenantWebhookConfig{
		EndpointURL: "https://example.com/hook",
		IsActive:    true,
	}})

	err := d.Dispatch(context.Background(), uuid.New(), uuid.New(), "PAYMENT_SUCCESS", nil)
	if err == nil || err.Error() != "write failed" {
		t.Fatalf("expected 'write failed', got %v", err)
	}
}
