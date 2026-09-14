package razorpay

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/ports"
)

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func newTestAdapter(handler http.HandlerFunc) *Adapter {
	client := &http.Client{Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, r)
		return rec.Result(), nil
	})}
	return New(Config{
		HTTPClient: client,
		ResolveConfig: func(_ context.Context, _ uuid.UUID) (*TenantConfig, error) {
			return &TenantConfig{KeyID: "rzp_test", KeySecret: "secret", BaseURL: "https://example.invalid"}, nil
		},
	})
}

func TestInitiatePayment_CreatesOrder(t *testing.T) {
	var gotPath, gotAuth string
	a := newTestAdapter(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		_, _ = w.Write([]byte(`{"id":"order_1","status":"created","amount":150000,"currency":"BDT"}`))
	})

	resp, err := a.InitiatePayment(context.Background(), ports.GatewayPaymentRequest{
		TransactionID: uuid.New(), TenantID: uuid.New(), Amount: 150000, Currency: "BDT", AttemptNumber: 1,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.GatewayReferenceID != "order_1" || resp.Status != ports.GatewayPaymentStatusPending {
		t.Errorf("unexpected response: %+v", resp)
	}
	if gotPath != "/v1/orders" {
		t.Errorf("expected POST /v1/orders, got %s", gotPath)
	}
	if gotAuth == "" {
		t.Error("expected basic auth header")
	}
}

func TestCheckStatus_PaidIsSucceeded(t *testing.T) {
	txnID := uuid.New()
	a := newTestAdapter(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"id":"order_2","status":"paid","amount":150000,"currency":"BDT"}`))
	})
	a.txnTenants.Store(txnID, uuid.New())

	resp, err := a.CheckStatus(context.Background(), ports.GatewayStatusRequest{TransactionID: txnID, GatewayReferenceID: "order_2"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != ports.GatewayPaymentStatusSucceeded {
		t.Errorf("expected SUCCEEDED, got %s", resp.Status)
	}
}

func TestRefund_Processed(t *testing.T) {
	txnID := uuid.New()
	a := newTestAdapter(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"id":"rfnd_1","status":"processed","amount":150000,"currency":"BDT"}`))
	})
	a.txnTenants.Store(txnID, uuid.New())

	resp, err := a.Refund(context.Background(), ports.GatewayRefundRequest{TransactionID: txnID, GatewayReferenceID: "pay_1", Amount: 150000})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != ports.GatewayRefundStatusCompleted {
		t.Errorf("expected COMPLETED, got %s", resp.Status)
	}
}

func TestCancel_NotSupported(t *testing.T) {
	a := newTestAdapter(func(w http.ResponseWriter, r *http.Request) {
		t.Error("should not be called")
	})
	resp, _ := a.Cancel(context.Background(), ports.GatewayCancelRequest{})
	if resp.Status != ports.GatewayCancelStatusNotSupported {
		t.Errorf("razorpay should report cancel NOT_SUPPORTED, got %s", resp.Status)
	}
}

func TestError_Classification(t *testing.T) {
	a := newTestAdapter(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":{"code":"BAD_REQUEST_ERROR","reason":"payment_failed","description":"declined"}}`))
	})

	_, err := a.InitiatePayment(context.Background(), ports.GatewayPaymentRequest{TransactionID: uuid.New(), TenantID: uuid.New(), Amount: 1, Currency: "BDT"})
	var gwErr *ports.GatewayError
	if !errors.As(err, &gwErr) {
		t.Fatalf("expected *ports.GatewayError, got %v", err)
	}
	if gwErr.Category != ports.ErrorCategoryHardDecline {
		t.Errorf("expected HardDecline for payment_failed, got %s", gwErr.Category)
	}
}

func TestClassifyError_Table(t *testing.T) {
	cases := []struct {
		code, reason string
		want         ports.ErrorCategory
	}{
		{"GATEWAY_ERROR", "", ports.ErrorCategoryGatewayError},
		{"SERVER_ERROR", "", ports.ErrorCategoryNetworkTimeout},
		{"BAD_REQUEST_ERROR", "payment_failed", ports.ErrorCategoryHardDecline},
		{"BAD_REQUEST_ERROR", "insufficient_funds", ports.ErrorCategorySoftDecline},
		{"BAD_REQUEST_ERROR", "validation", ports.ErrorCategoryGatewayError},
		{"WHATEVER", "", ports.ErrorCategoryGatewayError},
	}
	for _, c := range cases {
		if got := classifyError(c.code, c.reason); got != c.want {
			t.Errorf("classifyError(%q,%q)=%s want %s", c.code, c.reason, got, c.want)
		}
	}
}

func TestDeriveReceipt_StableAnd40Chars(t *testing.T) {
	id := uuid.New()
	r1 := deriveReceipt(id, 1)
	r2 := deriveReceipt(id, 1)
	r3 := deriveReceipt(id, 2)
	if r1 != r2 {
		t.Error("receipt must be stable for same inputs")
	}
	if r1 == r3 {
		t.Error("receipt must differ by attempt")
	}
	if len(r1) != 40 {
		t.Errorf("receipt must be exactly 40 chars (Razorpay limit), got %d", len(r1))
	}
}
