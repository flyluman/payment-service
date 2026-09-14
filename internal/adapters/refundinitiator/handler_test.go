package refundinitiator

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/domain/refund"
	"github.com/crownroutes/payment-service/internal/ports"
)

type fakeRefundProcessService struct {
	calledWith uuid.UUID
	err        error
}

func (f *fakeRefundProcessService) ProcessRefund(_ context.Context, refundID uuid.UUID) (*refund.Refund, error) {
	f.calledWith = refundID
	return nil, f.err
}

func TestHandle_ValidPayload(t *testing.T) {
	svc := &fakeRefundProcessService{}
	h := NewHandler(svc)
	refundID := uuid.New()

	payload, _ := json.Marshal(refundInitiatedPayload{
		RefundID:      refundID.String(),
		TransactionID: uuid.NewString(),
	})

	err := h.Handle(context.Background(), ports.PendingEvent{Payload: payload})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if svc.calledWith != refundID {
		t.Fatalf("expected service called with %s, got %s", refundID, svc.calledWith)
	}
}

func TestHandle_InvalidJSON(t *testing.T) {
	h := NewHandler(&fakeRefundProcessService{})
	err := h.Handle(context.Background(), ports.PendingEvent{Payload: []byte("not json")})
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestHandle_InvalidRefundID(t *testing.T) {
	svc := &fakeRefundProcessService{}
	h := NewHandler(svc)

	payload, _ := json.Marshal(refundInitiatedPayload{RefundID: "not-a-uuid"})
	err := h.Handle(context.Background(), ports.PendingEvent{Payload: payload})
	if err == nil {
		t.Fatal("expected error for invalid refund ID")
	}
	if svc.calledWith != uuid.Nil {
		t.Fatal("service must not be called with an unparsable refund ID")
	}
}

func TestHandle_ServiceError(t *testing.T) {
	svc := &fakeRefundProcessService{err: errors.New("service failed")}
	h := NewHandler(svc)

	payload, _ := json.Marshal(refundInitiatedPayload{RefundID: uuid.NewString()})
	err := h.Handle(context.Background(), ports.PendingEvent{Payload: payload})
	if err == nil || err.Error() != "service failed" {
		t.Fatalf("expected 'service failed', got %v", err)
	}
}
