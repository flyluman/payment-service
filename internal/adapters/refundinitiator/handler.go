package refundinitiator

import (
	"context"
	"encoding/json"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/domain/refund"
	"github.com/crownroutes/payment-service/internal/ports"
)

type refundInitiatedPayload struct {
	RefundID      string `json:"refund_id"`
	TransactionID string `json:"transaction_id"`
}

type RefundProcessService interface {
	ProcessRefund(ctx context.Context, refundID uuid.UUID) (*refund.Refund, error)
}

type Handler struct {
	svc RefundProcessService
}

func NewHandler(svc RefundProcessService) *Handler {
	return &Handler{svc: svc}
}

func (h *Handler) Handle(ctx context.Context, event ports.PendingEvent) error {
	var payload refundInitiatedPayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return err
	}
	refundID, err := uuid.Parse(payload.RefundID)
	if err != nil {
		return err
	}
	_, err = h.svc.ProcessRefund(ctx, refundID)
	return err
}
