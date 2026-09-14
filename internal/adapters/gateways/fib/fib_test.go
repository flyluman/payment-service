package fib

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/ports"
)

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func newTestAdapter(handler http.HandlerFunc, fn func(context.Context, uuid.UUID) (*TenantConfig, error)) *Adapter {
	client := &http.Client{Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, r)
		return rec.Result(), nil
	})}
	return New(Config{
		Timeout:       time.Second,
		HTTPClient:    client,
		ResolveConfig: fn,
	})
}

func fixedTenant() *TenantConfig {
	return &TenantConfig{
		ClientID:     "fib-client",
		ClientSecret: "fib-secret",
		BaseURL:      "https://fib.example.com",
		CallbackURL:  "https://myapp.com/webhook/fib",
	}
}

func resolveFixed(context.Context, uuid.UUID) (*TenantConfig, error) {
	return fixedTenant(), nil
}

func TestInitiatePayment_Success(t *testing.T) {
	calls := 0
	var authCall bool
	a := newTestAdapter(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if strings.Contains(r.URL.Path, "/auth/realms") {
			authCall = true
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"access_token":"tok_abc","expires_in":3600,"token_type":"Bearer"}`))
			return
		}
		if strings.Contains(r.URL.Path, "/protected/v1/payments") {
			var body createPaymentRequest
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			if body.MonetaryValue.Amount != "500" || body.MonetaryValue.Currency != "IQD" {
				t.Errorf("unexpected monetary: %+v", body.MonetaryValue)
			}
			if body.Category != "ECOMMERCE" {
				t.Errorf("expected category ECOMMERCE, got %s", body.Category)
			}
			expectedCB := "https://myapp.com/webhook/fib?transaction_id=00000000-0000-0000-0000-000000000001"
			if body.StatusCallbackURL != expectedCB {
				t.Errorf("unexpected callback: %s (want %s)", body.StatusCallbackURL, expectedCB)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"id":"fib_pay_1","paymentId":"fib_pay_1","readableCode":"FIB-123","qrCode":"data:image/png;base64,..."}`))
			return
		}
		http.Error(w, "unexpected path", 404)
	}, resolveFixed)

	resp, err := a.InitiatePayment(context.Background(), ports.GatewayPaymentRequest{
		TransactionID: uuid.MustParse("00000000-0000-0000-0000-000000000001"),
		TenantID:      uuid.MustParse("00000000-0000-0000-0000-000000000002"),
		Amount:        500,
		Currency:      "IQD",
		Description:   "test payment",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.GatewayReferenceID != "fib_pay_1" {
		t.Errorf("expected fib_pay_1, got %s", resp.GatewayReferenceID)
	}
	if resp.Status != ports.GatewayPaymentStatusPending {
		t.Errorf("expected PENDING, got %s", resp.Status)
	}
	if resp.GatewayMetadata["readable_code"] != "FIB-123" {
		t.Errorf("expected FIB-123 readable code, got %v", resp.GatewayMetadata["readable_code"])
	}
	if !authCall {
		t.Error("expected auth endpoint to be called")
	}
}

func TestInitiatePayment_MissingTenant(t *testing.T) {
	a := newTestAdapter(func(w http.ResponseWriter, r *http.Request) {
		t.Error("should not reach the server")
	}, resolveFixed)

	_, err := a.InitiatePayment(context.Background(), ports.GatewayPaymentRequest{
		TransactionID: uuid.New(),
		TenantID:      uuid.Nil,
	})
	if err == nil {
		t.Fatal("expected error for nil tenant")
	}
	var gwErr *ports.GatewayError
	if !asGatewayError(err, &gwErr) || gwErr.Code != "missing_tenant" {
		t.Errorf("expected missing_tenant error, got %v", err)
	}
}

func TestInitiatePayment_ConfigResolveError(t *testing.T) {
	a := newTestAdapter(func(w http.ResponseWriter, r *http.Request) {
		t.Error("should not reach the server")
	}, func(ctx context.Context, tid uuid.UUID) (*TenantConfig, error) {
		return nil, errConfig
	})

	_, err := a.InitiatePayment(context.Background(), ports.GatewayPaymentRequest{
		TransactionID: uuid.New(),
		TenantID:      uuid.New(),
		Amount:        500,
		Currency:      "IQD",
	})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestInitiatePayment_AuthFailure(t *testing.T) {
	a := newTestAdapter(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"invalid_client"}`))
	}, resolveFixed)

	_, err := a.InitiatePayment(context.Background(), ports.GatewayPaymentRequest{
		TransactionID: uuid.New(),
		TenantID:      uuid.New(),
		Amount:        500,
		Currency:      "IQD",
	})
	if err == nil {
		t.Fatal("expected auth error")
	}
}

func TestInitiatePayment_ApiError(t *testing.T) {
	callCount := 0
	a := newTestAdapter(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/auth/realms") {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"access_token":"tok_abc","expires_in":3600,"token_type":"Bearer"}`))
			return
		}
		callCount++
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = w.Write([]byte(`{"error":"amount_below_minimum"}`))
	}, resolveFixed)

	_, err := a.InitiatePayment(context.Background(), ports.GatewayPaymentRequest{
		TransactionID: uuid.New(),
		TenantID:      uuid.New(),
		Amount:        10,
		Currency:      "IQD",
	})
	if err == nil {
		t.Fatal("expected error")
	}
	var gwErr *ports.GatewayError
	if asGatewayError(err, &gwErr) && gwErr.Category != ports.ErrorCategoryHardDecline {
		t.Errorf("expected hard_decline, got %s", gwErr.Category)
	}
}

func TestCheckStatus_Success(t *testing.T) {
	a := newTestAdapter(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/auth/realms") {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"access_token":"tok_abc","expires_in":3600,"token_type":"Bearer"}`))
			return
		}
		if strings.HasSuffix(r.URL.Path, "/status") {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"paymentId":"fib_pay_1","status":"PAID","amount":{"amount":"500","currency":"IQD"}}`))
			return
		}
		http.Error(w, "unexpected", 404)
	}, resolveFixed)

	txnID := uuid.New()
	a.txnTenants.Store(txnID, uuid.New())

	resp, err := a.CheckStatus(context.Background(), ports.GatewayStatusRequest{
		TransactionID:      txnID,
		GatewayReferenceID: "fib_pay_1",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Status != ports.GatewayPaymentStatusSucceeded {
		t.Errorf("expected SUCCEEDED, got %s", resp.Status)
	}
	if resp.GatewayReferenceID != "fib_pay_1" {
		t.Errorf("expected fib_pay_1, got %s", resp.GatewayReferenceID)
	}
}

func TestCheckStatus_UnknownCache(t *testing.T) {
	a := New(Config{ResolveConfig: resolveFixed})
	_, err := a.CheckStatus(context.Background(), ports.GatewayStatusRequest{
		TransactionID:      uuid.New(),
		GatewayReferenceID: "fib_pay_1",
	})
	if err == nil {
		t.Fatal("expected error for uncached transaction")
	}
	var gwErr *ports.GatewayError
	if asGatewayError(err, &gwErr) && gwErr.Code != "tenant_not_cached" {
		t.Errorf("expected tenant_not_cached, got %s", gwErr.Code)
	}
}

func TestRefund_Success(t *testing.T) {
	a := newTestAdapter(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/auth/realms") {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"access_token":"tok_abc","expires_in":3600,"token_type":"Bearer"}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"refundId":"fib_ref_1","status":"REFUNDED"}`))
	}, resolveFixed)

	txnID := uuid.New()
	a.txnTenants.Store(txnID, uuid.New())

	resp, err := a.Refund(context.Background(), ports.GatewayRefundRequest{
		TransactionID:      txnID,
		GatewayReferenceID: "fib_pay_1",
		Amount:             500,
		Currency:           "IQD",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.GatewayRefundID != "fib_ref_1" {
		t.Errorf("expected fib_ref_1, got %s", resp.GatewayRefundID)
	}
	if resp.Status != ports.GatewayRefundStatusCompleted {
		t.Errorf("expected COMPLETED, got %s", resp.Status)
	}
}

func TestCancel_Success(t *testing.T) {
	a := newTestAdapter(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/auth/realms") {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"access_token":"tok_abc","expires_in":3600,"token_type":"Bearer"}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"CANCELLED"}`))
	}, resolveFixed)

	txnID := uuid.New()
	a.txnTenants.Store(txnID, uuid.New())

	resp, err := a.Cancel(context.Background(), ports.GatewayCancelRequest{
		TransactionID:      txnID,
		GatewayReferenceID: "fib_pay_1",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Status != ports.GatewayCancelStatusCancelled {
		t.Errorf("expected CANCELLED, got %s", resp.Status)
	}
}

func TestTokenCaching(t *testing.T) {
	authCalls := 0
	a := newTestAdapter(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/auth/realms") {
			authCalls++
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"access_token":"tok_abc","expires_in":3600,"token_type":"Bearer"}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"paymentId":"fib_pay_1","readableCode":"FIB-123","qrCode":"..."}`))
	}, resolveFixed)

	tenantID := uuid.New()
	for i := 0; i < 5; i++ {
		_, err := a.InitiatePayment(context.Background(), ports.GatewayPaymentRequest{
			TransactionID: uuid.New(),
			TenantID:      tenantID,
			Amount:        500,
			Currency:      "IQD",
		})
		if err != nil {
			t.Fatalf("attempt %d: %v", i, err)
		}
	}
	if authCalls != 1 {
		t.Errorf("expected 1 auth call (cached), got %d", authCalls)
	}
}

func TestCapabilities(t *testing.T) {
	a := New(Config{ResolveConfig: resolveFixed})
	caps := a.Capabilities()
	if !caps.SupportsCancel {
		t.Error("expected SupportsCancel true")
	}
	if caps.SupportsPartialRefund {
		t.Error("expected SupportsPartialRefund false")
	}
	if caps.IdempotencyCapable {
		t.Error("expected IdempotencyCapable false")
	}
	if len(caps.SupportedCurrencies) != 1 || caps.SupportedCurrencies[0] != "IQD" {
		t.Errorf("expected IQD only, got %v", caps.SupportedCurrencies)
	}
}

func asGatewayError(err error, out **ports.GatewayError) bool {
	e, ok := err.(*ports.GatewayError)
	if !ok {
		return false
	}
	*out = e
	return true
}

type errConfigType struct{}

func (e errConfigType) Error() string { return "config not found" }

var errConfig error = errConfigType{}
