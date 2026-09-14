package fib

import (
	"crypto/sha256"
	"encoding/hex"
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

	// FIB callbacks are unsigned triggers that can fire multiple times for the
	// same payment with different statuses. The payment ID is stable, so using
	// it as the event id would make every later callback a duplicate and drop
	// real status updates. Hash the raw body instead: identical retransmissions
	// still dedup, but distinct payloads (e.g. UNPAID then PAID) do not collide.
	sum := sha256.Sum256(body)

	return &ports.GatewayWebhookEvent{
		EventID:            hex.EncodeToString(sum[:]),
		GatewayReferenceID: paymentID,
		Status:             mapPaymentStatus(payload.Status),
	}, nil
}

// UnsignedWebhooks reports that FIB does not sign its webhook payloads; the
// handler must re-fetch the authoritative status via CheckStatus.
func (a *Adapter) UnsignedWebhooks() bool { return true }

var _ ports.GatewayWebhookParser = (*Adapter)(nil)
