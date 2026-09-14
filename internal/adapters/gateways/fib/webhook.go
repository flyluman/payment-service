package fib

import (
	"encoding/json"

	"github.com/crownroutes/payment-service/internal/ports"
)

func (a *Adapter) ParseWebhook(body []byte, headers map[string]string, secret string) (*ports.GatewayWebhookEvent, error) {
	var payload fibWebhookPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, ports.ErrWebhookParse
	}

	paymentID := payload.ID
	if paymentID == "" {
		paymentID = payload.PaymentID
	}
	if paymentID == "" {
		return nil, ports.ErrWebhookParse
	}
	if payload.Status == "" {
		return nil, ports.ErrWebhookParse
	}

	return &ports.GatewayWebhookEvent{
		EventID:            paymentID,
		GatewayReferenceID: paymentID,
		Status:             mapPaymentStatus(payload.Status),
	}, nil
}

var _ ports.GatewayWebhookParser = (*Adapter)(nil)
