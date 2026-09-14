package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"

	"io"
	"log/slog"
	"github.com/crownroutes/payment-service/internal/api/middleware"
	"github.com/crownroutes/payment-service/internal/adapters/observability"
	"github.com/crownroutes/payment-service/internal/api/handlers"
	"github.com/crownroutes/payment-service/internal/app/idempotency"
	"github.com/crownroutes/payment-service/internal/app/payment"
	"github.com/crownroutes/payment-service/internal/domain/transaction"
	"github.com/crownroutes/payment-service/internal/ports"
)

type stubService struct{ txn *transaction.Txn }

func (s stubService) Create(ctx context.Context, in payment.CreateInput) (payment.CreateResult, error) {
	return payment.CreateResult{Verdict: idempotency.Created, Transaction: s.txn, Token: "test-token"}, nil
}
func (s stubService) ProcessPayment(ctx context.Context, id uuid.UUID) (*transaction.Txn, error) {
	return s.txn, nil
}
func (s stubService) GetPayment(ctx context.Context, id uuid.UUID) (*transaction.Txn, error) {
	return s.txn, nil
}
func (s stubService) ListTransactions(ctx context.Context, filter ports.TransactionFilter) (*ports.TransactionListResult, error) {
	return &ports.TransactionListResult{}, nil
}
func (s stubService) Authorize(ctx context.Context, transactionID uuid.UUID) (*transaction.Txn, error) {
	return nil, nil
}
func (s stubService) Capture(ctx context.Context, transactionID uuid.UUID) (*transaction.Txn, error) {
	return nil, nil
}
func (s stubService) Settle(ctx context.Context, transactionID uuid.UUID) (*transaction.Txn, error) {
	return nil, nil
}

type okPinger struct{}

func (okPinger) Ping(ctx context.Context) error { return nil }

func TestRouter_RoutesAndSetsRequestID(t *testing.T) {
	txn, _ := transaction.New(uuid.New(), uuid.New(), 1000, "BDT", transaction.PaymentMethodCard, "stripe", uuid.New(), "", "", nil, 30, transaction.CaptureModeAuto)
	logger := observability.NewSlogLoggerFromHandler(slog.NewJSONHandler(io.Discard, nil))

	router := NewRouter(Deps{
		Payment: handlers.NewPaymentHandler(stubService{txn: txn}),
		Health:  handlers.NewHealthHandler(handlers.Check{Name: "database", Pinger: okPinger{}}),
		Logger:  logger,
	})

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected /health 200, got %d", rec.Code)
	}
	if rec.Header().Get("X-Request-ID") == "" {
		t.Error("expected RequestID middleware to set X-Request-ID header")
	}

	req2 := httptest.NewRequest(http.MethodGet, "/api/v1/payments/"+txn.ID.String(), nil)
	req2 = req2.WithContext(middleware.ContextWithPrincipal(req2.Context(), middleware.Principal{TenantID: txn.TenantID.String()}))
	rec2 := httptest.NewRecorder()
	router.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusOK {
		t.Errorf("expected GET /api/v1/payments/{id} 200, got %d", rec2.Code)
	}
}
