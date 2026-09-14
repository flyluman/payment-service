package initiator

import (
	"context"
	"encoding/json"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/ports"
)

type gatewayInitiatePayload struct {
	TransactionID string `json:"transaction_id"`
	TenantID      string `json:"tenant_id"`
	GatewayID     string `json:"gateway_id"`
}

type GatewayInitiateService interface {
	ProcessGatewayInitiate(ctx context.Context, transactionID uuid.UUID) error
}

type Handler struct {
	svc GatewayInitiateService
}

func NewHandler(svc GatewayInitiateService) *Handler {
	return &Handler{svc: svc}
}

func (h *Handler) Handle(ctx context.Context, event ports.PendingEvent) error {
	var payload gatewayInitiatePayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return err
	}
	txnID, err := uuid.Parse(payload.TransactionID)
	if err != nil {
		return err
	}
	return h.svc.ProcessGatewayInitiate(ctx, txnID)
}
