package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/crownroutes/payment-service/internal/api/response"
	"github.com/crownroutes/payment-service/internal/ports"
)

type fakeGatewayService struct {
	configs         []*ports.GatewayConfig
	err             error
	receivedMethods []string
}

func (f *fakeGatewayService) ListActiveGateways(_ context.Context, methods []string) ([]*ports.GatewayConfig, error) {
	f.receivedMethods = methods
	return f.configs, f.err
}

func TestList_Success(t *testing.T) {
	svc := &fakeGatewayService{configs: []*ports.GatewayConfig{
		{GatewayID: "stripe", DisplayName: "Stripe", IsActive: true, SupportedMethods: []string{"card"}, SupportedCurrencies: []string{"USD"}},
	}}
	h := NewGatewayHandler(svc)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/gateways", nil)
	rec := httptest.NewRecorder()

	h.List(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var env response.Envelope
	json.Unmarshal(rec.Body.Bytes(), &env)
	raw, _ := json.Marshal(env.Data)
	var out []gatewayResponse
	json.Unmarshal(raw, &out)
	if len(out) != 1 || out[0].GatewayID != "stripe" {
		t.Fatalf("unexpected response: %s", rec.Body.String())
	}
}

func TestList_DefaultPaymentMethods(t *testing.T) {
	svc := &fakeGatewayService{configs: []*ports.GatewayConfig{}}
	h := NewGatewayHandler(svc)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/gateways", nil)
	rec := httptest.NewRecorder()

	h.List(rec, req)

	expected := []string{"card", "upi", "netbanking", "wallet"}
	if len(svc.receivedMethods) != len(expected) {
		t.Fatalf("expected %d methods, got %d: %v", len(expected), len(svc.receivedMethods), svc.receivedMethods)
	}
}

func TestList_CustomPaymentMethods(t *testing.T) {
	svc := &fakeGatewayService{configs: []*ports.GatewayConfig{}}
	h := NewGatewayHandler(svc)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/gateways?payment_method=card,upi", nil)
	rec := httptest.NewRecorder()

	h.List(rec, req)

	expected := []string{"card", "upi"}
	if len(svc.receivedMethods) != len(expected) {
		t.Fatalf("expected %d methods, got %d: %v", len(expected), len(svc.receivedMethods), svc.receivedMethods)
	}
}

func TestList_ServiceError(t *testing.T) {
	svc := &fakeGatewayService{err: context.DeadlineExceeded}
	h := NewGatewayHandler(svc)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/gateways", nil)
	rec := httptest.NewRecorder()

	h.List(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", rec.Code)
	}
}

func TestList_SliceSafety(t *testing.T) {
	origMethods := []string{"card"}
	origCurrencies := []string{"USD"}
	svc := &fakeGatewayService{configs: []*ports.GatewayConfig{
		{GatewayID: "stripe", SupportedMethods: origMethods, SupportedCurrencies: origCurrencies},
	}}
	h := NewGatewayHandler(svc)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/gateways", nil)
	rec := httptest.NewRecorder()

	h.List(rec, req)

	var env response.Envelope
	json.Unmarshal(rec.Body.Bytes(), &env)
	raw, _ := json.Marshal(env.Data)
	var out []gatewayResponse
	json.Unmarshal(raw, &out)

	if len(out) == 0 {
		t.Fatal("expected at least one gateway in response")
	}
	out[0].SupportedMethods[0] = "mutated"
	if origMethods[0] == "mutated" {
		t.Fatal("response should not mutate original slices")
	}
}
