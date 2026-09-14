package handlers

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/api/middleware"
	"github.com/crownroutes/payment-service/internal/domain/transaction"
	"github.com/crownroutes/payment-service/internal/ports"
)

type fakeBus struct {
	mu   sync.RWMutex
	subs map[uuid.UUID][]chan ports.StatusEvent
}

func newFakeBus() *fakeBus {
	return &fakeBus{subs: make(map[uuid.UUID][]chan ports.StatusEvent)}
}

func (b *fakeBus) Publish(_ context.Context, event ports.StatusEvent) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	chans := b.subs[event.TransactionID]
	for _, c := range chans {
		select {
		case c <- event:
		default:
		}
	}
}

func (b *fakeBus) Subscribe(txnID uuid.UUID) chan ports.StatusEvent {
	ch := make(chan ports.StatusEvent, 1)
	b.subs[txnID] = append(b.subs[txnID], ch)
	return ch
}

func (b *fakeBus) Unsubscribe(txnID uuid.UUID, ch chan ports.StatusEvent) {
	subs := b.subs[txnID]
	for i, s := range subs {
		if s == ch {
			b.subs[txnID] = append(subs[:i], subs[i+1:]...)
			break
		}
	}
}

type fakeTxnGetterSSE struct {
	txn *transaction.Txn
	err error
}

func (f *fakeTxnGetterSSE) GetByID(_ context.Context, _ uuid.UUID) (*transaction.Txn, error) {
	return f.txn, f.err
}

func sseRequest(method, path string) *http.Request {
	req := httptest.NewRequest(method, path, nil)
	var captured *http.Request
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured = r
	})
	middleware.RequestID(inner).ServeHTTP(httptest.NewRecorder(), req)
	return captured
}

func TestEvents_InvalidUUID(t *testing.T) {
	h := NewSSEHandler(newFakeBus(), &fakeTxnGetterSSE{}, noopLog{})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/payments/events", nil)
	req.SetPathValue("id", "not-a-uuid")
	rec := httptest.NewRecorder()

	h.Events(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestEvents_TransactionNotFound(t *testing.T) {
	h := NewSSEHandler(newFakeBus(), &fakeTxnGetterSSE{err: errors.New("not found")}, noopLog{})

	req := sseRequest(http.MethodGet, "/api/v1/payments/events")
	req.SetPathValue("id", uuid.NewString())
	rec := httptest.NewRecorder()

	h.Events(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestEvents_MissingToken(t *testing.T) {
	txn, _ := sampleTxnWithToken()
	h := NewSSEHandler(newFakeBus(), &fakeTxnGetterSSE{txn: txn}, noopLog{})

	req := sseRequest(http.MethodGet, "/api/v1/payments/events")
	req.SetPathValue("id", txn.ID.String())
	rec := httptest.NewRecorder()

	h.Events(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestEvents_InvalidToken(t *testing.T) {
	txn, _ := sampleTxnWithToken()
	h := NewSSEHandler(newFakeBus(), &fakeTxnGetterSSE{txn: txn}, noopLog{})

	req := sseRequest(http.MethodGet, "/api/v1/payments/events?token=wrong")
	req.SetPathValue("id", txn.ID.String())
	rec := httptest.NewRecorder()

	h.Events(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestEvents_TenantIDMismatch(t *testing.T) {
	txn, raw := sampleTxnWithToken()
	h := NewSSEHandler(newFakeBus(), &fakeTxnGetterSSE{txn: txn}, noopLog{})

	req := sseRequest(http.MethodGet, "/api/v1/payments/events?token="+raw)
	req.SetPathValue("id", txn.ID.String())
	ctx := middleware.ContextWithPrincipal(req.Context(), middleware.Principal{TenantID: "wrong-tenant"})
	req = req.WithContext(ctx)
	rec := httptest.NewRecorder()

	h.Events(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestEvents_SSEHeaders(t *testing.T) {
	txn, raw := sampleTxnWithToken()
	bus := newFakeBus()
	h := NewSSEHandler(bus, &fakeTxnGetterSSE{txn: txn}, noopLog{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req := sseRequest(http.MethodGet, "/api/v1/payments/events?token="+raw)
	req = req.WithContext(ctx)
	req.SetPathValue("id", txn.ID.String())

	rec := httptest.NewRecorder()
	rec.Body.Grow(4096)

	go h.Events(rec, req)

	time.Sleep(100 * time.Millisecond)
	cancel()
	time.Sleep(50 * time.Millisecond)

	ct := rec.Header().Get("Content-Type")
	if ct != "text/event-stream" {
		t.Fatalf("expected text/event-stream, got %s", ct)
	}
	cc := rec.Header().Get("Cache-Control")
	if cc != "no-cache" {
		t.Fatalf("expected no-cache, got %s", cc)
	}
}
