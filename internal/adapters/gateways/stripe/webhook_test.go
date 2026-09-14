package stripe

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/crownroutes/payment-service/internal/ports"
)

func stripeSig(secret string, ts int64, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(strconv.FormatInt(ts, 10)))
	mac.Write([]byte("."))
	mac.Write(body)
	return "t=" + strconv.FormatInt(ts, 10) + ",v1=" + hex.EncodeToString(mac.Sum(nil))
}

func TestParseWebhook_Valid(t *testing.T) {
	a := New(Config{})
	secret := "whsec_test"
	body := []byte(`{"id":"evt_1","type":"payment_intent.succeeded","data":{"object":{"id":"pi_1","status":"succeeded"}}}`)
	headers := map[string]string{"Stripe-Signature": stripeSig(secret, time.Now().Unix(), body)}

	ev, err := a.ParseWebhook(body, headers, secret)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ev.EventID != "evt_1" || ev.GatewayReferenceID != "pi_1" || ev.Status != ports.GatewayPaymentStatusSucceeded {
		t.Errorf("unexpected event: %+v", ev)
	}
}

func TestParseWebhook_BadSignature(t *testing.T) {
	a := New(Config{})
	body := []byte(`{"id":"evt_1","data":{"object":{"id":"pi_1","status":"succeeded"}}}`)
	headers := map[string]string{"Stripe-Signature": stripeSig("the-wrong-secret", time.Now().Unix(), body)}

	_, err := a.ParseWebhook(body, headers, "real-secret")
	if !errors.Is(err, ports.ErrWebhookSignature) {
		t.Fatalf("expected ErrWebhookSignature, got %v", err)
	}
}

func TestParseWebhook_StaleTimestamp(t *testing.T) {
	a := New(Config{})
	secret := "whsec_test"
	body := []byte(`{"id":"evt_1","data":{"object":{"id":"pi_1","status":"succeeded"}}}`)
	old := time.Now().Add(-time.Hour).Unix()
	headers := map[string]string{"Stripe-Signature": stripeSig(secret, old, body)}

	_, err := a.ParseWebhook(body, headers, secret)
	if !errors.Is(err, ports.ErrWebhookSignature) {
		t.Fatalf("stale timestamp should fail signature verification, got %v", err)
	}
}

func TestParseWebhook_MissingHeader(t *testing.T) {
	a := New(Config{})
	_, err := a.ParseWebhook([]byte(`{}`), map[string]string{}, "secret")
	if !errors.Is(err, ports.ErrWebhookSignature) {
		t.Fatalf("missing signature header should fail, got %v", err)
	}
}

func TestParseWebhook_DisputeCreated(t *testing.T) {
	a := New(Config{})
	secret := "whsec_test"
	body := []byte(`{
		"id": "evt_disc_1",
		"type": "charge.dispute.created",
		"data": {
			"object": {
				"id": "du_abc123",
				"payment_intent": "pi_xyz789",
				"status": "needs_response",
				"reason": "fraudulent",
				"amount": 5000,
				"currency": "usd",
				"evidence_details": {
					"due_by": 1735689600
				}
			}
		}
	}`)
	headers := map[string]string{"Stripe-Signature": stripeSig(secret, time.Now().Unix(), body)}

	ev, err := a.ParseWebhook(body, headers, secret)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ev.Dispute == nil {
		t.Fatal("expected Dispute to be set")
	}
	if ev.Dispute.GatewayDisputeID != "du_abc123" {
		t.Errorf("GatewayDisputeID = %q, want %q", ev.Dispute.GatewayDisputeID, "du_abc123")
	}
	if ev.Dispute.GatewayReferenceID != "pi_xyz789" {
		t.Errorf("GatewayReferenceID = %q, want %q", ev.Dispute.GatewayReferenceID, "pi_xyz789")
	}
	if ev.Dispute.Status != "NEEDS_RESPONSE" {
		t.Errorf("Status = %q, want %q", ev.Dispute.Status, "NEEDS_RESPONSE")
	}
	if ev.Dispute.Reason != "fraudulent" {
		t.Errorf("Reason = %q, want %q", ev.Dispute.Reason, "fraudulent")
	}
	if ev.Dispute.Amount != 5000 {
		t.Errorf("Amount = %d, want %d", ev.Dispute.Amount, 5000)
	}
	if ev.Dispute.Currency != "USD" {
		t.Errorf("Currency = %q, want %q", ev.Dispute.Currency, "USD")
	}
	if ev.Dispute.EvidenceDueBy == nil {
		t.Fatal("EvidenceDueBy should be set")
	}
	expectedDueBy := time.Unix(1735689600, 0).UTC()
	if !ev.Dispute.EvidenceDueBy.Equal(expectedDueBy) {
		t.Errorf("EvidenceDueBy = %v, want %v", ev.Dispute.EvidenceDueBy, expectedDueBy)
	}
	if ev.EventID != "evt_disc_1" {
		t.Errorf("EventID = %q, want %q", ev.EventID, "evt_disc_1")
	}
	if ev.EventType != "charge.dispute.created" {
		t.Errorf("EventType = %q, want %q", ev.EventType, "charge.dispute.created")
	}
	if ev.GatewayReferenceID != "pi_xyz789" {
		t.Errorf("GatewayReferenceID = %q, want payment_intent ID", ev.GatewayReferenceID)
	}
	if ev.Status != "" {
		t.Errorf("Status should be empty for dispute events, got %q", ev.Status)
	}
}

func TestParseWebhook_DisputeUpdated(t *testing.T) {
	a := New(Config{})
	secret := "whsec_test"
	body := []byte(`{
		"id": "evt_disc_2",
		"type": "charge.dispute.updated",
		"data": {
			"object": {
				"id": "du_abc123",
				"payment_intent": "pi_xyz789",
				"status": "under_review",
				"reason": "fraudulent",
				"amount": 5000,
				"currency": "usd",
				"evidence_details": {}
			}
		}
	}`)
	headers := map[string]string{"Stripe-Signature": stripeSig(secret, time.Now().Unix(), body)}

	ev, err := a.ParseWebhook(body, headers, secret)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ev.Dispute == nil {
		t.Fatal("expected Dispute to be set")
	}
	if ev.Dispute.Status != "UNDER_REVIEW" {
		t.Errorf("Status = %q, want %q", ev.Dispute.Status, "UNDER_REVIEW")
	}
	if ev.EventType != "charge.dispute.updated" {
		t.Errorf("EventType = %q, want %q", ev.EventType, "charge.dispute.updated")
	}
}

func TestParseWebhook_DisputeClosed_Won(t *testing.T) {
	a := New(Config{})
	secret := "whsec_test"
	body := []byte(`{
		"id": "evt_disc_3",
		"type": "charge.dispute.closed",
		"data": {
			"object": {
				"id": "du_abc123",
				"payment_intent": "pi_xyz789",
				"status": "won",
				"reason": "fraudulent",
				"amount": 5000,
				"currency": "usd",
				"evidence_details": {}
			}
		}
	}`)
	headers := map[string]string{"Stripe-Signature": stripeSig(secret, time.Now().Unix(), body)}

	ev, err := a.ParseWebhook(body, headers, secret)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ev.Dispute == nil {
		t.Fatal("expected Dispute to be set")
	}
	if ev.Dispute.Status != "WON" {
		t.Errorf("Status = %q, want %q", ev.Dispute.Status, "WON")
	}
}

func TestParseWebhook_DisputeClosed_Lost(t *testing.T) {
	a := New(Config{})
	secret := "whsec_test"
	body := []byte(`{
		"id": "evt_disc_4",
		"type": "charge.dispute.closed",
		"data": {
			"object": {
				"id": "du_abc123",
				"payment_intent": "pi_xyz789",
				"status": "lost",
				"reason": "fraudulent",
				"amount": 5000,
				"currency": "usd",
				"evidence_details": {}
			}
		}
	}`)
	headers := map[string]string{"Stripe-Signature": stripeSig(secret, time.Now().Unix(), body)}

	ev, err := a.ParseWebhook(body, headers, secret)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ev.Dispute == nil {
		t.Fatal("expected Dispute to be set")
	}
	if ev.Dispute.Status != "LOST" {
		t.Errorf("Status = %q, want %q", ev.Dispute.Status, "LOST")
	}
}

func TestParseWebhook_DisputeWarningNeedsResponse(t *testing.T) {
	a := New(Config{})
	secret := "whsec_test"
	body := []byte(`{
		"id": "evt_disc_5",
		"type": "charge.dispute.updated",
		"data": {
			"object": {
				"id": "du_abc123",
				"payment_intent": "pi_xyz789",
				"status": "warning_needs_response",
				"reason": "general",
				"amount": 1000,
				"currency": "eur",
				"evidence_details": {}
			}
		}
	}`)
	headers := map[string]string{"Stripe-Signature": stripeSig(secret, time.Now().Unix(), body)}

	ev, err := a.ParseWebhook(body, headers, secret)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ev.Dispute == nil {
		t.Fatal("expected Dispute to be set")
	}
	if ev.Dispute.Status != "NEEDS_RESPONSE" {
		t.Errorf("warning_needs_response should map to NEEDS_RESPONSE, got %q", ev.Dispute.Status)
	}
	if ev.Dispute.Currency != "EUR" {
		t.Errorf("Currency = %q, want %q", ev.Dispute.Currency, "EUR")
	}
}
