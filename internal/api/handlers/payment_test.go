package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/api/middleware"
	"github.com/crownroutes/payment-service/internal/app/idempotency"
	"github.com/crownroutes/payment-service/internal/app/payment"
	"github.com/crownroutes/payment-service/internal/domain/transaction"
	"github.com/crownroutes/payment-service/internal/ports"
)

type fakeService struct {
	created      *transaction.Txn
	verdict      idempotency.Verdict
	processed    *transaction.Txn
	fetched      *transaction.Txn
	createErr    error
	procErr      error
	getErr       error
	token        string
}

func (f *fakeService) Create(ctx context.Context, in payment.CreateInput) (payment.CreateResult, error) {
	if f.createErr != nil {
		return payment.CreateResult{}, f.createErr
	}
	return payment.CreateResult{Verdict: f.verdict, Transaction: f.created, Token: f.token}, nil
}
func (f *fakeService) ProcessPayment(ctx context.Context, id uuid.UUID) (*transaction.Txn, error) {
	return f.processed, f.procErr
}
func (f *fakeService) GetPayment(ctx context.Context, id uuid.UUID) (*transaction.Txn, error) {
	return f.fetched, f.getErr
}
func (f *fakeService) GetGatewayMetadata(ctx context.Context, id uuid.UUID) (map[string]any, error) {
	return nil, nil
}
func (f *fakeService) ListTransactions(ctx context.Context, filter ports.TransactionFilter) (*ports.TransactionListResult, error) {
	return &ports.TransactionListResult{}, nil
}
func (f *fakeService) Authorize(ctx context.Context, transactionID uuid.UUID) (*transaction.Txn, error) {
	return nil, nil
}
func (f *fakeService) Capture(ctx context.Context, transactionID uuid.UUID) (*transaction.Txn, error) {
	return nil, nil
}
func (f *fakeService) Settle(ctx context.Context, transactionID uuid.UUID) (*transaction.Txn, error) {
	return nil, nil
}

func sampleTxn(status transaction.Status) *transaction.Txn {
	t, _ := transaction.New(uuid.New(), uuid.New(), 150000, "BDT", transaction.PaymentMethodCard, "stripe", uuid.New(), "b@e.com", "order", nil, 30, transaction.CaptureModeAuto)
	t.Status = status
	return t
}

func validCreateBody() string {
	return `{
		"gateway_id": "stripe",
		"amount": 1000,
		"currency": "BDT",
		"payment_method": "card",
		"callback_url": "https://example.com/callback",
		"redirect_url": "https://example.com/redirect"
	}`
}

func TestCreate_Success(t *testing.T) {
	txn := sampleTxn(transaction.StatusPending)
	h := NewPaymentHandler(&fakeService{created: txn, verdict: idempotency.Created, token: "test-token"})

	body := validCreateBody()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/payments", strings.NewReader(body))
	req = req.WithContext(middleware.ContextWithPrincipal(req.Context(), middleware.Principal{TenantID: txn.TenantID.String(), UserID: uuid.NewString()}))
	req.Header.Set("Idempotency-Key", "test-idem-key")
	rec := httptest.NewRecorder()
	h.Create(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d (%s)", rec.Code, rec.Body.String())
	}
	var env struct {
		Data createPaymentResponse `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatal(err)
	}
	if env.Data.TransactionID == "" {
		t.Fatal("expected transaction_id")
	}
	if env.Data.Token == "" {
		t.Fatal("expected token")
	}
}

func TestCreate_MissingFields(t *testing.T) {
	h := NewPaymentHandler(&fakeService{})
	tests := []struct {
		name string
		body string
	}{
		{"empty body", `{}`},
		{"missing gateway_id", `{"amount":1000,"currency":"BDT","payment_method":"card"}`},
		{"zero amount", `{"gateway_id":"stripe","amount":0,"currency":"BDT","payment_method":"card"}`},
		{"missing currency", `{"gateway_id":"stripe","amount":1000,"payment_method":"card"}`},
		{"missing callback_url", `{"gateway_id":"stripe","amount":1000,"currency":"BDT","payment_method":"card","redirect_url":"https://e.com/r"}`},
		{"missing redirect_url", `{"gateway_id":"stripe","amount":1000,"currency":"BDT","payment_method":"card","callback_url":"https://e.com/c"}`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/api/v1/payments", strings.NewReader(tc.body))
			req = req.WithContext(middleware.ContextWithPrincipal(req.Context(), middleware.Principal{TenantID: uuid.NewString()}))
			req.Header.Set("Idempotency-Key", "test-idem-key")
			rec := httptest.NewRecorder()
			h.Create(rec, req)
			if rec.Code != http.StatusBadRequest {
				t.Errorf("expected 400, got %d: %s", rec.Code, rec.Body.String())
			}
		})
	}
}

func TestCreate_MissingTenant(t *testing.T) {
	h := NewPaymentHandler(&fakeService{})
	body := validCreateBody()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/payments", strings.NewReader(body))
	req.Header.Set("Idempotency-Key", "test-idem-key")
	rec := httptest.NewRecorder()
	h.Create(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

func TestCreate_MissingIdempotencyKey(t *testing.T) {
	h := NewPaymentHandler(&fakeService{})
	body := validCreateBody()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/payments", strings.NewReader(body))
	req = req.WithContext(middleware.ContextWithPrincipal(req.Context(), middleware.Principal{TenantID: uuid.NewString()}))
	rec := httptest.NewRecorder()
	h.Create(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestCreate_NoGateway(t *testing.T) {
	h := NewPaymentHandler(&fakeService{createErr: payment.ErrNoGateway})
	body := validCreateBody()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/payments", strings.NewReader(body))
	req = req.WithContext(middleware.ContextWithPrincipal(req.Context(), middleware.Principal{TenantID: uuid.NewString()}))
	req.Header.Set("Idempotency-Key", "test-idem-key")
	rec := httptest.NewRecorder()
	h.Create(rec, req)

	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422, got %d", rec.Code)
	}
}

func TestCreate_IdempotencyConflict(t *testing.T) {
	h := NewPaymentHandler(&fakeService{verdict: idempotency.KeyReused})
	body := validCreateBody()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/payments", strings.NewReader(body))
	req = req.WithContext(middleware.ContextWithPrincipal(req.Context(), middleware.Principal{TenantID: uuid.NewString()}))
	req.Header.Set("Idempotency-Key", "test-idem-key")
	rec := httptest.NewRecorder()
	h.Create(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d", rec.Code)
	}
}

func TestGet_Success(t *testing.T) {
	txn := sampleTxn(transaction.StatusCaptured)
	h := NewPaymentHandler(&fakeService{fetched: txn})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/payments/"+txn.ID.String(), nil)
	req.SetPathValue("id", txn.ID.String())
	req = req.WithContext(middleware.ContextWithPrincipal(req.Context(), middleware.Principal{TenantID: txn.TenantID.String()}))
	rec := httptest.NewRecorder()
	h.Get(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

func TestGet_InvalidID(t *testing.T) {
	h := NewPaymentHandler(&fakeService{})
	req := httptest.NewRequest(http.MethodGet, "/api/v1/payments/bad", nil)
	req.SetPathValue("id", "bad")
	rec := httptest.NewRecorder()
	h.Get(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestGet_NotFound(t *testing.T) {
	h := NewPaymentHandler(&fakeService{getErr: errors.New("not found")})
	id := uuid.NewString()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/payments/"+id, nil)
	req.SetPathValue("id", id)
	rec := httptest.NewRecorder()
	h.Get(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}
