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

	appwebhook "github.com/crownroutes/payment-service/internal/app/webhook"
	"github.com/crownroutes/payment-service/internal/domain/gateway"
	"github.com/crownroutes/payment-service/internal/domain/transaction"
	"github.com/crownroutes/payment-service/internal/ports"
)

type noopLog struct{}

func (noopLog) Info(string, map[string]any)         {}
func (noopLog) Warn(string, map[string]any)         {}
func (noopLog) Error(string, map[string]any, error) {}
func (noopLog) Debug(string, map[string]any)        {}
func (noopLog) Trace(string, map[string]any)        {}
func (l noopLog) With(map[string]any) ports.Logger  { return l }

type fakeWebhookProcessor struct {
	calls      int
	lastStatus ports.GatewayPaymentStatus
	outcome    appwebhook.Outcome
}

func (f *fakeWebhookProcessor) Process(_ context.Context, _ string, ev appwebhook.Event, _ []byte) (appwebhook.Outcome, error) {
	f.calls++
	f.lastStatus = ports.GatewayPaymentStatus(ev.Status)
	return f.outcome, nil
}

type fakeWebhookParser struct {
	event *ports.GatewayWebhookEvent
	err   error
}

func (f fakeWebhookParser) ParseWebhook([]byte, map[string]string, string) (*ports.GatewayWebhookEvent, error) {
	return f.event, f.err
}

type fakeWebhookResolver struct {
	parser     ports.GatewayWebhookParser
	ok         bool
	refetch    bool
	statusResp *ports.GatewayPaymentResponse
	statusErr  error
}

func (f fakeWebhookResolver) WebhookParser(string) (ports.GatewayWebhookParser, bool) {
	return f.parser, f.ok
}

func (f fakeWebhookResolver) WebhookRequiresRefetch(string) bool {
	return f.refetch
}

func (f fakeWebhookResolver) CheckStatus(context.Context, string, ports.GatewayStatusRequest) (*ports.GatewayPaymentResponse, error) {
	return f.statusResp, f.statusErr
}

type fakeWebhookTxnRepo struct {
	txn *transaction.Txn
}

func (f fakeWebhookTxnRepo) GetByID(_ context.Context, _ uuid.UUID) (*transaction.Txn, error) {
	return f.txn, nil
}

func (f fakeWebhookTxnRepo) GetByGatewayReference(_ context.Context, _, _ string) (*transaction.Txn, error) {
	return f.txn, nil
}

type fakeWebhookTenantStore struct {
	cfg *gateway.TenantGatewayConfig
}

func (f fakeWebhookTenantStore) Get(_ context.Context, _ uuid.UUID, _ string) (*gateway.TenantGatewayConfig, error) {
	return f.cfg, nil
}

func (f fakeWebhookTenantStore) ListByGateway(_ context.Context, _ string) ([]*gateway.TenantGatewayConfig, error) {
	if f.cfg != nil {
		return []*gateway.TenantGatewayConfig{f.cfg}, nil
	}
	return nil, nil
}

func okEvent() *ports.GatewayWebhookEvent {
	return &ports.GatewayWebhookEvent{EventID: "evt_1", GatewayReferenceID: "ref_1", Status: ports.GatewayPaymentStatusSucceeded}
}

func newWebhookHandler(proc *fakeWebhookProcessor, parser ports.GatewayWebhookParser, txn *transaction.Txn, cfg *gateway.TenantGatewayConfig) *WebhookHandler {
	resolver := fakeWebhookResolver{parser: parser, ok: parser != nil}
	txnRepo := fakeWebhookTxnRepo{txn: txn}
	tenantStore := fakeWebhookTenantStore{cfg: cfg}
	return NewWebhookHandler(proc, resolver, txnRepo, tenantStore, nil, noopLog{})
}

func validTxn() *transaction.Txn {
	tenantID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	txn, _ := transaction.New(tenantID, uuid.Nil, 1000, "IQD", transaction.PaymentMethodCard, "fib", uuid.Nil, "", "", nil, 300, transaction.CaptureModeAuto)
	return txn
}

func fibConfig() *gateway.TenantGatewayConfig {
	cfg := gateway.FIBConfig{ClientID: "cid", ClientSecret: "cs", BaseURL: "https://fib.example.com"}
	raw, _ := json.Marshal(cfg)
	return &gateway.TenantGatewayConfig{
		TenantID: uuid.Nil, GatewayID: "fib", Provider: gateway.ProviderFIB,
		Config: raw, IsActive: true,
	}
}

func postWebhookV2(h *WebhookHandler, gateway, txnID string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/webhooks/gateway/"+gateway+"?transaction_id="+txnID, strings.NewReader(`raw`))
	req.SetPathValue("gateway_id", gateway)
	rec := httptest.NewRecorder()
	h.Handle(rec, req)
	return rec
}

func TestWebhook_MissingTransactionIDAndNoTenantConfig(t *testing.T) {
	h := newWebhookHandler(&fakeWebhookProcessor{}, nil, nil, nil)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/gateway/stripe", strings.NewReader(`raw`))
	req.SetPathValue("gateway_id", "stripe")
	rec := httptest.NewRecorder()
	h.Handle(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for missing transaction_id with no tenant config, got %d", rec.Code)
	}
}

func TestWebhook_ValidEventProcesses(t *testing.T) {
	proc := &fakeWebhookProcessor{outcome: appwebhook.Outcome{Resolved: true}}
	txn := validTxn()
	h := newWebhookHandler(proc, fakeWebhookParser{event: okEvent()}, txn, fibConfig())
	rec := postWebhookV2(h, "fib", txn.ID.String())
	if rec.Code != http.StatusOK || proc.calls != 1 {
		t.Fatalf("expected 200 + 1 call, got %d / %d (%s)", rec.Code, proc.calls, rec.Body.String())
	}
}

func TestWebhook_InvalidSignature401(t *testing.T) {
	proc := &fakeWebhookProcessor{}
	txn := validTxn()
	h := newWebhookHandler(proc, fakeWebhookParser{err: ports.ErrWebhookSignature}, txn, fibConfig())
	rec := postWebhookV2(h, "fib", txn.ID.String())
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for bad signature, got %d", rec.Code)
	}
	if proc.calls != 0 {
		t.Error("processor must not run on signature failure")
	}
}

func TestWebhook_ParseError400(t *testing.T) {
	txn := validTxn()
	h := newWebhookHandler(&fakeWebhookProcessor{}, fakeWebhookParser{err: ports.ErrWebhookParse}, txn, fibConfig())
	rec := postWebhookV2(h, "fib", txn.ID.String())
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for parse failure, got %d", rec.Code)
	}
}

func TestWebhook_UnknownGateway404(t *testing.T) {
	proc := &fakeWebhookProcessor{}
	// resolver returns ok=false (no parser registered)
	txn := validTxn()
	h := NewWebhookHandler(proc, fakeWebhookResolver{ok: false}, fakeWebhookTxnRepo{txn: txn}, fakeWebhookTenantStore{cfg: fibConfig()}, nil, noopLog{})
	rec := postWebhookV2(h, "mystery", txn.ID.String())
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for unregistered gateway, got %d", rec.Code)
	}
}

func TestWebhook_UnsignedGatewayRefetchesStatus(t *testing.T) {
	proc := &fakeWebhookProcessor{outcome: appwebhook.Outcome{Resolved: true}}
	txn := validTxn()

	// Webhook claims the payment SUCCEEDED, but the authoritative status is FAILED.
	// The unsigned-gateway path must re-fetch and use the authoritative status.
	authoritative := &ports.GatewayPaymentResponse{
		GatewayReferenceID: "ref_1",
		Status:             ports.GatewayPaymentStatusFailed,
	}
	resolver := fakeWebhookResolver{
		parser:     fakeWebhookParser{event: okEvent()},
		ok:         true,
		refetch:    true,
		statusResp: authoritative,
	}
	h := NewWebhookHandler(proc, resolver, fakeWebhookTxnRepo{txn: txn}, fakeWebhookTenantStore{cfg: fibConfig()}, nil, noopLog{})

	req := httptest.NewRequest(http.MethodPost, "/webhooks/gateway/fib", strings.NewReader(`raw`))
	req.SetPathValue("gateway_id", "fib")
	rec := httptest.NewRecorder()
	h.Handle(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (%s)", rec.Code, rec.Body.String())
	}
	if proc.calls != 1 {
		t.Fatalf("expected 1 processor call, got %d", proc.calls)
	}
	if got := proc.lastStatus; got != "failed" {
		t.Errorf("expected authoritative status failed to reach processor, got %s", got)
	}
}

func TestWebhook_UnsignedGatewayRefetchFailure500(t *testing.T) {
	proc := &fakeWebhookProcessor{}
	txn := validTxn()
	resolver := fakeWebhookResolver{
		parser:    fakeWebhookParser{event: okEvent()},
		ok:        true,
		refetch:   true,
		statusErr: errors.New("gateway down"),
	}
	h := NewWebhookHandler(proc, resolver, fakeWebhookTxnRepo{txn: txn}, fakeWebhookTenantStore{cfg: fibConfig()}, nil, noopLog{})

	req := httptest.NewRequest(http.MethodPost, "/webhooks/gateway/fib", strings.NewReader(`raw`))
	req.SetPathValue("gateway_id", "fib")
	rec := httptest.NewRecorder()
	h.Handle(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500 on refetch failure, got %d", rec.Code)
	}
	if proc.calls != 0 {
		t.Error("processor must not run when status cannot be verified")
	}
}

func TestWebhook_InvalidTxnID(t *testing.T) {
	h := newWebhookHandler(&fakeWebhookProcessor{}, fakeWebhookParser{event: okEvent()}, nil, nil)
	rec := postWebhookV2(h, "fib", "not-a-uuid")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid txn ID, got %d", rec.Code)
	}
}
