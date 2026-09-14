package stripe

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/crownroutes/payment-service/internal/ports"
)

const webhookToleranceSeconds = 300

type stripeWebhookEvent struct {
	ID   string `json:"id"`
	Type string `json:"type"`
	Data struct {
		Object struct {
			ID               string       `json:"id"`
			PaymentIntent    string       `json:"payment_intent"`
			Status           string       `json:"status"`
			Refunded         bool         `json:"refunded"`
			LastPaymentError *stripeError `json:"last_payment_error"`
			NextAction       *struct {
				Type          string `json:"type"`
				RedirectToURL *struct {
					URL string `json:"url"`
				} `json:"redirect_to_url,omitempty"`
			} `json:"next_action"`
		} `json:"object"`
	} `json:"data"`
}

type stripeDisputeEvent struct {
	ID   string `json:"id"`
	Type string `json:"type"`
	Data struct {
		Object struct {
			ID              string `json:"id"`
			PaymentIntent   string `json:"payment_intent"`
			Status          string `json:"status"`
			Reason          string `json:"reason"`
			Amount          int64  `json:"amount"`
			Currency        string `json:"currency"`
			EvidenceDetails struct {
				DueBy *int64 `json:"due_by"`
			} `json:"evidence_details"`
		} `json:"object"`
	} `json:"data"`
}

func (a *Adapter) ParseWebhook(body []byte, headers map[string]string, secret string) (*ports.GatewayWebhookEvent, error) {
	ts, v1, ok := parseStripeSignature(headers["Stripe-Signature"])
	if !ok {
		return nil, ports.ErrWebhookSignature
	}

	providedMAC, err := hex.DecodeString(v1)
	if err != nil {
		return nil, ports.ErrWebhookSignature
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(strconv.FormatInt(ts, 10)))
	mac.Write([]byte("."))
	mac.Write(body)
	if !hmac.Equal(mac.Sum(nil), providedMAC) {
		return nil, ports.ErrWebhookSignature
	}

	if diff := time.Now().Unix() - ts; diff > webhookToleranceSeconds || diff < -webhookToleranceSeconds {
		return nil, ports.ErrWebhookSignature
	}

	var raw struct {
		ID   string `json:"id"`
		Type string `json:"type"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, ports.ErrWebhookParse
	}

	if strings.HasPrefix(raw.Type, "charge.dispute.") {
		var dev stripeDisputeEvent
		if err := json.Unmarshal(body, &dev); err != nil {
			return nil, ports.ErrWebhookParse
		}

		disputeEv := &ports.GatewayDisputeEvent{
			GatewayDisputeID:   dev.Data.Object.ID,
			GatewayReferenceID: dev.Data.Object.PaymentIntent,
			Status:             mapDisputeStatus(dev.Data.Object.Status),
			Reason:             dev.Data.Object.Reason,
			Amount:             dev.Data.Object.Amount,
			Currency:           strings.ToUpper(dev.Data.Object.Currency),
		}
		if dev.Data.Object.EvidenceDetails.DueBy != nil {
			t := time.Unix(*dev.Data.Object.EvidenceDetails.DueBy, 0).UTC()
			disputeEv.EvidenceDueBy = &t
		}

		return &ports.GatewayWebhookEvent{
			EventID:            dev.ID,
			GatewayReferenceID: dev.Data.Object.PaymentIntent,
			EventType:          dev.Type,
			GatewayMetadata:    map[string]any{"gateway": "stripe", "type": dev.Type},
			Dispute:            disputeEv,
		}, nil
	}

	var ev stripeWebhookEvent
	if err := json.Unmarshal(body, &ev); err != nil {
		return nil, ports.ErrWebhookParse
	}

	// charge.* webhooks carry the charge object, whose id is a charge_..., not
	// the payment_intent we persist as gateway_reference_id. Resolve the PI
	// from the charge's payment_intent field, falling back to the charge id.
	reference := ev.Data.Object.PaymentIntent
	if reference == "" {
		reference = ev.Data.Object.ID
	}

	return &ports.GatewayWebhookEvent{
		EventID:            ev.ID,
		GatewayReferenceID: reference,
		Status:             mapStatusString(ev.Data.Object.Status, ev.Data.Object.LastPaymentError != nil, ev.Data.Object.Refunded),
		EventType:          ev.Type,
		GatewayMetadata:    extractStripeWebhookMetadata(ev),
	}, nil
}

func parseStripeSignature(header string) (ts int64, v1 string, ok bool) {
	for _, part := range strings.Split(header, ",") {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(kv) != 2 {
			continue
		}
		switch kv[0] {
		case "t":
			ts, _ = strconv.ParseInt(kv[1], 10, 64)
		case "v1":
			v1 = kv[1]
		}
	}
	if ts == 0 || v1 == "" {
		return 0, "", false
	}
	return ts, v1, true
}

func mapStatusString(s string, hasError, refunded bool) ports.GatewayPaymentStatus {
	if hasError {
		return ports.GatewayPaymentStatusFailed
	}
	if refunded {
		return ports.GatewayPaymentStatusRefunded
	}
	switch s {
	case "succeeded":
		return ports.GatewayPaymentStatusSucceeded
	case "processing":
		return ports.GatewayPaymentStatusProcessing
	case "canceled":
		return ports.GatewayPaymentStatusCancelled
	default:
		return ports.GatewayPaymentStatusPending
	}
}

func extractStripeWebhookMetadata(ev stripeWebhookEvent) map[string]any {
	meta := map[string]any{}
	if ev.Data.Object.NextAction != nil {
		na := map[string]any{"type": ev.Data.Object.NextAction.Type}
		if ev.Data.Object.NextAction.RedirectToURL != nil {
			na["redirect_url"] = ev.Data.Object.NextAction.RedirectToURL.URL
		}
		meta["next_action"] = na
	}
	return meta
}

func mapDisputeStatus(s string) string {
	switch s {
	case "needs_response", "warning_needs_response":
		return "NEEDS_RESPONSE"
	case "under_review":
		return "UNDER_REVIEW"
	case "won":
		return "WON"
	case "lost":
		return "LOST"
	case "accepted":
		return "ACCEPTED"
	case "expired":
		return "EXPIRED"
	default:
		return "NEEDS_RESPONSE"
	}
}
