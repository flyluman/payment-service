package handlers

import (
	"context"
	"errors"
	"html/template"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/app/payment"
	"github.com/crownroutes/payment-service/internal/domain/transaction"
)

type fakeTxnGetter struct {
	txn *transaction.Txn
	err error
}

func (f *fakeTxnGetter) GetByID(_ context.Context, _ uuid.UUID) (*transaction.Txn, error) {
	return f.txn, f.err
}

type fakeGatewayMetaReader struct {
	meta map[string]any
	err  error
}

func (f *fakeGatewayMetaReader) GetGatewayMetadata(_ context.Context, _ uuid.UUID) (map[string]any, error) {
	return f.meta, f.err
}

func allTemplates() *template.Template {
	tmpl := template.New("root")
	tmpl = template.Must(tmpl.New("checkout.html").Parse(`checkout: {{.PageData}}`))
	tmpl = template.Must(tmpl.New("success.html").Parse(`success: {{.TransactionID}} {{.Amount}} {{.Currency}}`))
	tmpl = template.Must(tmpl.New("failure.html").Parse(`failure: {{.Message}}`))
	return tmpl
}

func sampleTxnWithToken() (*transaction.Txn, string) {
	raw, hash, _ := payment.GenerateToken()
	txn, _ := transaction.New(uuid.New(), uuid.New(), 150000, "BDT", transaction.PaymentMethodCard, "stripe", uuid.New(), "b@e.com", "order", nil, 30)
	txn.TokenHash = hash
	return txn, raw
}

func TestServeCheckout_Auth_MissingTransactionID(t *testing.T) {
	h := NewPayHandler(&fakeTxnGetter{}, allTemplates(), noopLog{}, nil)
	req := httptest.NewRequest(http.MethodGet, "/web/v1/stripe?token=abc", nil)
	rec := httptest.NewRecorder()

	h.ServeCheckout(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

func TestServeCheckout_Auth_MissingToken(t *testing.T) {
	txn, _ := sampleTxnWithToken()
	h := NewPayHandler(&fakeTxnGetter{txn: txn}, allTemplates(), noopLog{}, nil)

	req := httptest.NewRequest(http.MethodGet, "/web/v1/stripe", nil)
	req.SetPathValue("transaction_id", txn.ID.String())
	rec := httptest.NewRecorder()

	h.ServeCheckout(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

func TestServeCheckout_Auth_InvalidToken(t *testing.T) {
	txn, _ := sampleTxnWithToken()
	h := NewPayHandler(&fakeTxnGetter{txn: txn}, allTemplates(), noopLog{}, nil)

	req := httptest.NewRequest(http.MethodGet, "/web/v1/stripe?token=wrong-token", nil)
	req.SetPathValue("transaction_id", txn.ID.String())
	rec := httptest.NewRecorder()

	h.ServeCheckout(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

func TestServeCheckout_Auth_TransactionNotFound(t *testing.T) {
	h := NewPayHandler(&fakeTxnGetter{err: errors.New("not found")}, allTemplates(), noopLog{}, nil)

	req := httptest.NewRequest(http.MethodGet, "/web/v1/stripe?token=abc", nil)
	req.SetPathValue("transaction_id", uuid.NewString())
	rec := httptest.NewRecorder()

	h.ServeCheckout(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

func TestServeCheckout_Success(t *testing.T) {
	txn, raw := sampleTxnWithToken()
	h := NewPayHandler(&fakeTxnGetter{txn: txn}, allTemplates(), noopLog{}, nil)

	req := httptest.NewRequest(http.MethodGet, "/web/v1/stripe?token="+raw, nil)
	req.SetPathValue("transaction_id", txn.ID.String())
	rec := httptest.NewRecorder()

	h.ServeCheckout(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "checkout:") {
		t.Fatalf("expected checkout template output, got: %s", rec.Body.String())
	}
}

func TestServeCheckout_WithGatewayMetadata(t *testing.T) {
	txn, raw := sampleTxnWithToken()
	meta := map[string]any{"client_secret": "secret_123"}
	h := NewPayHandler(&fakeTxnGetter{txn: txn}, allTemplates(), noopLog{}, &fakeGatewayMetaReader{meta: meta})

	req := httptest.NewRequest(http.MethodGet, "/web/v1/stripe?token="+raw, nil)
	req.SetPathValue("transaction_id", txn.ID.String())
	rec := httptest.NewRecorder()

	h.ServeCheckout(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "client_secret") {
		t.Fatalf("expected rawMetadata in output, got: %s", rec.Body.String())
	}
}

func TestSuccess_Success(t *testing.T) {
	txn, raw := sampleTxnWithToken()
	h := NewPayHandler(&fakeTxnGetter{txn: txn}, allTemplates(), noopLog{}, nil)

	req := httptest.NewRequest(http.MethodGet, "/web/v1/success?token="+raw, nil)
	req.SetPathValue("transaction_id", txn.ID.String())
	rec := httptest.NewRecorder()

	h.Success(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "success:") {
		t.Fatalf("expected success template output, got: %s", rec.Body.String())
	}
}

func TestFailure_DefaultMessage(t *testing.T) {
	txn, raw := sampleTxnWithToken()
	h := NewPayHandler(&fakeTxnGetter{txn: txn}, allTemplates(), noopLog{}, nil)

	req := httptest.NewRequest(http.MethodGet, "/web/v1/failure?token="+raw, nil)
	req.SetPathValue("transaction_id", txn.ID.String())
	rec := httptest.NewRecorder()

	h.Failure(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "Your payment could not be processed.") {
		t.Fatalf("expected default message, got: %s", rec.Body.String())
	}
}

func TestFailure_GatewayMessage(t *testing.T) {
	txn, raw := sampleTxnWithToken()
	reason := transaction.FailureReason{GatewayMessage: "card declined"}
	txn.FailureReason = &reason
	h := NewPayHandler(&fakeTxnGetter{txn: txn}, allTemplates(), noopLog{}, nil)

	req := httptest.NewRequest(http.MethodGet, "/web/v1/failure?token="+raw, nil)
	req.SetPathValue("transaction_id", txn.ID.String())
	rec := httptest.NewRecorder()

	h.Failure(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "card declined") {
		t.Fatalf("expected gateway message, got: %s", rec.Body.String())
	}
}
