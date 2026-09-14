package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/api/middleware"
	"github.com/crownroutes/payment-service/internal/api/response"
	"github.com/crownroutes/payment-service/internal/domain/fees"
	"github.com/crownroutes/payment-service/internal/ports"
)

type fakeGatewayService struct {
	configs []*ports.GatewayConfig
	err     error
}

func (f *fakeGatewayService) ListActiveGateways(_ context.Context) ([]*ports.GatewayConfig, error) {
	return f.configs, f.err
}

type fakeFeeEstimator struct {
	models map[string]*ports.GatewayFeeModel
	rates  []fees.CurrencyRate
}

func (f *fakeFeeEstimator) GetFeeModel(_ context.Context, gatewayID, method string) (*ports.GatewayFeeModel, error) {
	if f.models == nil {
		return nil, nil
	}
	return f.models[gatewayID+"/"+method], nil
}
func (f *fakeFeeEstimator) GetCurrencyRates(_ context.Context, _ uuid.UUID) ([]fees.CurrencyRate, error) {
	return f.rates, nil
}

func withTenant(r *http.Request, tenantID string) *http.Request {
	return r.WithContext(middleware.ContextWithPrincipal(r.Context(), middleware.Principal{
		Role:     middleware.RoleService,
		TenantID: tenantID,
	}))
}

func TestList_Success(t *testing.T) {
	svc := &fakeGatewayService{configs: []*ports.GatewayConfig{
		{GatewayID: "stripe", DisplayName: "Stripe", IsActive: true, SupportedMethods: []string{"card"}, SupportedCurrencies: []string{"USD"}},
	}}
	h := NewGatewayHandler(svc, &fakeFeeEstimator{})

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

func TestList_NoTenant(t *testing.T) {
	svc := &fakeGatewayService{configs: []*ports.GatewayConfig{
		{GatewayID: "stripe", DisplayName: "Stripe", IsActive: true, SupportedMethods: []string{"card"}, SupportedCurrencies: []string{"USD"}},
	}}
	h := NewGatewayHandler(svc, &fakeFeeEstimator{})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/gateways", nil)
	rec := httptest.NewRecorder()

	h.List(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 without tenant, got %d", rec.Code)
	}
}

func TestList_ServiceError(t *testing.T) {
	svc := &fakeGatewayService{err: context.DeadlineExceeded}
	h := NewGatewayHandler(svc, &fakeFeeEstimator{})

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
	h := NewGatewayHandler(svc, &fakeFeeEstimator{})

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

func TestList_FeeBreakdownsPerMethod(t *testing.T) {
	svc := &fakeGatewayService{configs: []*ports.GatewayConfig{
		{GatewayID: "stripe", DisplayName: "Stripe", IsActive: true, SupportedMethods: []string{"card", "upi"}, SupportedCurrencies: []string{"USD"}},
	}}
	estimator := &fakeFeeEstimator{models: map[string]*ports.GatewayFeeModel{
		"stripe/card": {GatewayID: "stripe", PaymentMethod: "card", PercentageBPS: 200},
		"stripe/upi":  {GatewayID: "stripe", PaymentMethod: "upi", FixedFee: 500, PercentageBPS: 100},
	}}
	h := NewGatewayHandler(svc, estimator)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/gateways?currency=USD&amount=10000", nil)
	req = withTenant(req, "11111111-1111-1111-1111-111111111111")
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

	if len(out) != 1 {
		t.Fatalf("unexpected response: %s", rec.Body.String())
	}
	if out[0].FeeBreakdowns == nil {
		t.Fatal("expected fee_breakdowns in response")
	}
	if _, ok := out[0].FeeBreakdowns["card"]; !ok {
		t.Fatalf("expected breakdown for card, got keys %v", keysOf(out[0].FeeBreakdowns))
	}
	if _, ok := out[0].FeeBreakdowns["upi"]; !ok {
		t.Fatalf("expected breakdown for upi, got keys %v", keysOf(out[0].FeeBreakdowns))
	}
}

func keysOf(m map[string]*fees.Breakdown) []string {
	var keys []string
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
