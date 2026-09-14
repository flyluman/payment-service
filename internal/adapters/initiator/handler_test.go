package initiator

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/ports"
)

type fakeInitiateService struct {
	calledWith uuid.UUID
	err        error
}

func (f *fakeInitiateService) ProcessGatewayInitiate(_ context.Context, txnID uuid.UUID) error {
	f.calledWith = txnID
	return f.err
}

func TestHandle_ValidPayload(t *testing.T) {
	svc := &fakeInitiateService{}
	h := NewHandler(svc)
	txnID := uuid.New()

	payload, _ := json.Marshal(gatewayInitiatePayload{
		TransactionID: txnID.String(),
		TenantID:      uuid.NewString(),
		GatewayID:     "stripe",
	})

	err := h.Handle(context.Background(), ports.PendingEvent{Payload: payload})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if svc.calledWith != txnID {
		t.Fatalf("expected service called with %s, got %s", txnID, svc.calledWith)
	}
}

func TestHandle_InvalidJSON(t *testing.T) {
	h := NewHandler(&fakeInitiateService{})
	err := h.Handle(context.Background(), ports.PendingEvent{Payload: []byte("not json")})
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestHandle_InvalidUUID(t *testing.T) {
	h := NewHandler(&fakeInitiateService{})
	payload, _ := json.Marshal(gatewayInitiatePayload{
		TransactionID: "not-a-uuid",
	})
	err := h.Handle(context.Background(), ports.PendingEvent{Payload: payload})
	if err == nil {
		t.Fatal("expected error for invalid UUID")
	}
}

func TestHandle_ServiceError(t *testing.T) {
	svc := &fakeInitiateService{err: errors.New("service failed")}
	h := NewHandler(svc)

	payload, _ := json.Marshal(gatewayInitiatePayload{
		TransactionID: uuid.NewString(),
	})
	err := h.Handle(context.Background(), ports.PendingEvent{Payload: payload})
	if err == nil || err.Error() != "service failed" {
		t.Fatalf("expected 'service failed', got %v", err)
	}
}
