package gateway

import (
	"encoding/json"
	"testing"
)

func TestValidProvider(t *testing.T) {
	cases := []struct {
		p    Provider
		want bool
	}{
		{ProviderStripe, true},
		{ProviderRazorpay, true},
		{"unknown", false},
		{"", false},
	}
	for _, tc := range cases {
		if got := ValidProvider(tc.p); got != tc.want {
			t.Errorf("ValidProvider(%q) = %v, want %v", tc.p, got, tc.want)
		}
	}
}

func TestStripeConfig_Validate(t *testing.T) {
	if err := (StripeConfig{}).Validate(); err == nil {
		t.Error("expected error for empty api_key")
	}
	if err := (StripeConfig{APIKey: "short"}).Validate(); err == nil {
		t.Error("expected error for short api_key")
	}
	if err := (StripeConfig{APIKey: "sk_test_12345"}).Validate(); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestRazorpayConfig_Validate(t *testing.T) {
	if err := (RazorpayConfig{}).Validate(); err == nil {
		t.Error("expected error for empty key_id")
	}
	if err := (RazorpayConfig{KeyID: "rzp_test"}).Validate(); err == nil {
		t.Error("expected error for empty key_secret")
	}
	if err := (RazorpayConfig{KeyID: "rzp_test", KeySecret: "secret"}).Validate(); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestParseProviderConfig_Stripe(t *testing.T) {
	raw := json.RawMessage(`{"api_key":"sk_test_12345","webhook_secret":"whsec_abc"}`)
	cfg, err := ParseProviderConfig(ProviderStripe, raw)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Provider() != ProviderStripe {
		t.Errorf("expected stripe, got %s", cfg.Provider())
	}
	stripe := cfg.(StripeConfig)
	if stripe.APIKey != "sk_test_12345" {
		t.Errorf("expected api_key sk_test_12345, got %s", stripe.APIKey)
	}
}

func TestParseProviderConfig_Razorpay(t *testing.T) {
	raw := json.RawMessage(`{"key_id":"rzp_test","key_secret":"secret"}`)
	cfg, err := ParseProviderConfig(ProviderRazorpay, raw)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Provider() != ProviderRazorpay {
		t.Errorf("expected razorpay, got %s", cfg.Provider())
	}
}

func TestParseProviderConfig_InvalidJSON(t *testing.T) {
	_, err := ParseProviderConfig(ProviderStripe, json.RawMessage(`{invalid`))
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestParseProviderConfig_ValidationFails(t *testing.T) {
	raw := json.RawMessage(`{"api_key":""}`)
	_, err := ParseProviderConfig(ProviderStripe, raw)
	if err == nil {
		t.Error("expected validation error for empty api_key")
	}
}

func TestParseProviderConfig_EmptyPayload(t *testing.T) {
	_, err := ParseProviderConfig(ProviderStripe, json.RawMessage(``))
	if err == nil {
		t.Error("expected error for empty payload")
	}
}

func TestParseProviderConfig_UnsupportedProvider(t *testing.T) {
	raw := json.RawMessage(`{"key":"value"}`)
	_, err := ParseProviderConfig("paypal", raw)
	if err == nil {
		t.Error("expected error for unsupported provider")
	}
}
