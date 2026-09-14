package fib

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/crownroutes/payment-service/internal/ports"
)

func TestParseWebhook_Valid(t *testing.T) {
	a := New(Config{})
	body := []byte(`{"id":"fib_pay_123","paymentId":"fib_pay_123","status":"PAID"}`)

	ev, err := a.ParseWebhook(body, nil, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	sum := sha256.Sum256(body)
	if ev.EventID != hex.EncodeToString(sum[:]) {
		t.Errorf("expected body-hash event id, got %s", ev.EventID)
	}
	if ev.GatewayReferenceID != "fib_pay_123" {
		t.Errorf("expected reference fib_pay_123, got %s", ev.GatewayReferenceID)
	}
	if ev.Status != ports.GatewayPaymentStatusSucceeded {
		t.Errorf("expected SUCCEEDED, got %s", ev.Status)
	}
}

func TestParseWebhook_Paid(t *testing.T) {
	a := New(Config{})
	tests := []struct {
		status   string
		expected ports.GatewayPaymentStatus
	}{
		{"UNPAID", ports.GatewayPaymentStatusPending},
		{"PAID", ports.GatewayPaymentStatusSucceeded},
		{"DECLINED", ports.GatewayPaymentStatusFailed},
		{"CANCELLED", ports.GatewayPaymentStatusCancelled},
		{"REFUNDED", ports.GatewayPaymentStatusRefunded},
		{"UNKNOWN", ports.GatewayPaymentStatusProcessing},
	}
	for _, tt := range tests {
		body := []byte(`{"id":"fib_pay_1","paymentId":"fib_pay_1","status":"` + tt.status + `"}`)
		ev, err := a.ParseWebhook(body, nil, "")
		if err != nil {
			t.Fatalf("status %q: %v", tt.status, err)
		}
		if ev.Status != tt.expected {
			t.Errorf("status %q: expected %s, got %s", tt.status, tt.expected, ev.Status)
		}
	}
}

func TestParseWebhook_InvalidBody(t *testing.T) {
	a := New(Config{})
	_, err := a.ParseWebhook([]byte(`not json`), nil, "")
	if !errors.Is(err, ports.ErrWebhookParse) {
		t.Fatalf("expected ErrWebhookParse, got %v", err)
	}
}

func TestParseWebhook_MissingID(t *testing.T) {
	a := New(Config{})
	_, err := a.ParseWebhook([]byte(`{"status":"PAID"}`), nil, "")
	if !errors.Is(err, ports.ErrWebhookParse) {
		t.Fatalf("expected ErrWebhookParse for missing id, got %v", err)
	}
}

func TestParseWebhook_MissingStatus(t *testing.T) {
	a := New(Config{})
	_, err := a.ParseWebhook([]byte(`{"id":"fib_pay_1"}`), nil, "")
	if !errors.Is(err, ports.ErrWebhookParse) {
		t.Fatalf("expected ErrWebhookParse, got %v", err)
	}
}

func TestParseWebhook_FallbackPaymentID(t *testing.T) {
	a := New(Config{})
	body := []byte(`{"paymentId":"fib_pay_456","status":"PAID"}`)

	ev, err := a.ParseWebhook(body, nil, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	sum := sha256.Sum256(body)
	if ev.EventID != hex.EncodeToString(sum[:]) {
		t.Errorf("expected body-hash event id, got %s", ev.EventID)
	}
	if ev.GatewayReferenceID != "fib_pay_456" {
		t.Errorf("expected reference fib_pay_456, got %s", ev.GatewayReferenceID)
	}
}

// Distinct bodies must yield distinct event ids so later FIB callbacks for the
// same payment aren't dropped by the webhook dedup.
func TestParseWebhook_EventIDVariesWithStatus(t *testing.T) {
	a := New(Config{})
	paid := []byte(`{"id":"fib_pay_9","paymentId":"fib_pay_9","status":"PAID"}`)
	refunded := []byte(`{"id":"fib_pay_9","paymentId":"fib_pay_9","status":"REFUNDED"}`)

	paidEv, err := a.ParseWebhook(paid, nil, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	refundedEv, err := a.ParseWebhook(refunded, nil, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if paidEv.EventID == refundedEv.EventID {
		t.Errorf("expected distinct event ids for different payloads, got %s", paidEv.EventID)
	}
	if paidEv.GatewayReferenceID != refundedEv.GatewayReferenceID {
		t.Errorf("reference should be stable across payloads, got %s vs %s", paidEv.GatewayReferenceID, refundedEv.GatewayReferenceID)
	}
}
