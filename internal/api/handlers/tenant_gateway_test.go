package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"github.com/crownroutes/payment-service/internal/api/middleware"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/domain/gateway"
)

type fakeTenantConfigStore struct {
	configs map[string]*gateway.TenantGatewayConfig // key: tenantID/gatewayID
}

func newFakeTenantConfigStore() *fakeTenantConfigStore {
	return &fakeTenantConfigStore{configs: make(map[string]*gateway.TenantGatewayConfig)}
}

func (f *fakeTenantConfigStore) key(tenantID uuid.UUID, gatewayID string) string {
	return tenantID.String() + "/" + gatewayID
}

func (f *fakeTenantConfigStore) Get(_ context.Context, tenantID uuid.UUID, gatewayID string) (*gateway.TenantGatewayConfig, error) {
	cfg, ok := f.configs[f.key(tenantID, gatewayID)]
	if !ok {
		return nil, gateway.ErrNotFound
	}
	return cfg, nil
}

func (f *fakeTenantConfigStore) ListByTenant(_ context.Context, tenantID uuid.UUID) ([]*gateway.TenantGatewayConfig, error) {
	var out []*gateway.TenantGatewayConfig
	for _, cfg := range f.configs {
		if cfg.TenantID == tenantID {
			out = append(out, cfg)
		}
	}
	return out, nil
}

func (f *fakeTenantConfigStore) ListActiveByTenant(_ context.Context, tenantID uuid.UUID) ([]*gateway.TenantGatewayConfig, error) {
	var out []*gateway.TenantGatewayConfig
	for _, cfg := range f.configs {
		if cfg.TenantID == tenantID && cfg.IsActive {
			out = append(out, cfg)
		}
	}
	return out, nil
}

func (f *fakeTenantConfigStore) Upsert(_ context.Context, cfg *gateway.TenantGatewayConfig) (int, error) {
	existing, ok := f.configs[f.key(cfg.TenantID, cfg.GatewayID)]
	if ok {
		cfg.ConfigVersion = existing.ConfigVersion + 1
	} else {
		cfg.ConfigVersion = 1
	}
	cfg.CreatedAt = time.Now().UTC()
	cfg.UpdatedAt = time.Now().UTC()
	f.configs[f.key(cfg.TenantID, cfg.GatewayID)] = cfg
	return cfg.ConfigVersion, nil
}

func (f *fakeTenantConfigStore) SetActive(_ context.Context, tenantID uuid.UUID, gatewayID string, active bool) error {
	cfg, ok := f.configs[f.key(tenantID, gatewayID)]
	if !ok {
		return gateway.ErrNotFound
	}
	cfg.IsActive = active
	return nil
}

func (f *fakeTenantConfigStore) Delete(_ context.Context, tenantID uuid.UUID, gatewayID string) error {
	delete(f.configs, f.key(tenantID, gatewayID))
	return nil
}

func TestTenantGatewayHandler_List(t *testing.T) {
	store := newFakeTenantConfigStore()
	tenantID := uuid.New()
	store.configs[store.key(tenantID, "stripe")] = &gateway.TenantGatewayConfig{
		TenantID: tenantID, GatewayID: "stripe", Provider: gateway.ProviderStripe,
		ConfigVersion: 1, IsActive: true, CreatedAt: time.Now(), UpdatedAt: time.Now(),
	}

	handler := NewTenantGatewayHandler(store)
	req := httptest.NewRequest("GET", "/tenants/"+tenantID.String()+"/gateways", nil)
	req.SetPathValue("tenant_id", tenantID.String())
	req = req.WithContext(middleware.ContextWithPrincipal(req.Context(), middleware.Principal{Role: middleware.RoleOps}))
	w := httptest.NewRecorder()

	handler.List(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var env struct {
		Data []tenantGatewayResponse `json:"data"`
	}
	if err := json.NewDecoder(w.Body).Decode(&env); err != nil {
		t.Fatal(err)
	}
	if len(env.Data) != 1 {
		t.Fatalf("expected 1 config, got %d", len(env.Data))
	}
	if env.Data[0].GatewayID != "stripe" {
		t.Errorf("expected stripe, got %s", env.Data[0].GatewayID)
	}
}

func TestTenantGatewayHandler_Get_NotFound(t *testing.T) {
	store := newFakeTenantConfigStore()
	handler := NewTenantGatewayHandler(store)

	req := httptest.NewRequest("GET", "/tenants/"+uuid.New().String()+"/gateways/unknown", nil)
	req.SetPathValue("tenant_id", uuid.New().String())
	req.SetPathValue("gateway_id", "unknown")
	req = req.WithContext(middleware.ContextWithPrincipal(req.Context(), middleware.Principal{Role: middleware.RoleOps}))
	w := httptest.NewRecorder()

	handler.Get(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestTenantGatewayHandler_Upsert_Success(t *testing.T) {
	store := newFakeTenantConfigStore()
	handler := NewTenantGatewayHandler(store)
	tenantID := uuid.New()

	body := `{"gateway_id":"stripe","provider":"stripe","config":{"api_key":"sk_test_12345"}}`
	req := httptest.NewRequest("POST", "/tenants/"+tenantID.String()+"/gateways", bytes.NewBufferString(body))
	req.SetPathValue("tenant_id", tenantID.String())
	req = req.WithContext(middleware.ContextWithPrincipal(req.Context(), middleware.Principal{Role: middleware.RoleOps}))
	w := httptest.NewRecorder()

	handler.Upsert(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var env struct {
		Data map[string]any `json:"data"`
	}
	if err := json.NewDecoder(w.Body).Decode(&env); err != nil {
		t.Fatal(err)
	}
	if env.Data["gateway_id"] != "stripe" {
		t.Errorf("expected stripe, got %v", env.Data["gateway_id"])
	}
}

func TestTenantGatewayHandler_Upsert_InvalidProvider(t *testing.T) {
	store := newFakeTenantConfigStore()
	handler := NewTenantGatewayHandler(store)

	body := `{"gateway_id":"gw","provider":"paypal","config":{"key":"val"}}`
	req := httptest.NewRequest("POST", "/tenants/"+uuid.New().String()+"/gateways", bytes.NewBufferString(body))
	req.SetPathValue("tenant_id", uuid.New().String())
	req = req.WithContext(middleware.ContextWithPrincipal(req.Context(), middleware.Principal{Role: middleware.RoleOps}))
	w := httptest.NewRecorder()

	handler.Upsert(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestTenantGatewayHandler_Upsert_MissingConfig(t *testing.T) {
	store := newFakeTenantConfigStore()
	handler := NewTenantGatewayHandler(store)

	body := `{"gateway_id":"gw","provider":"stripe"}`
	req := httptest.NewRequest("POST", "/tenants/"+uuid.New().String()+"/gateways", bytes.NewBufferString(body))
	req.SetPathValue("tenant_id", uuid.New().String())
	req = req.WithContext(middleware.ContextWithPrincipal(req.Context(), middleware.Principal{Role: middleware.RoleOps}))
	w := httptest.NewRecorder()

	handler.Upsert(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestTenantGatewayHandler_Delete(t *testing.T) {
	store := newFakeTenantConfigStore()
	tenantID := uuid.New()
	store.configs[store.key(tenantID, "stripe")] = &gateway.TenantGatewayConfig{
		TenantID: tenantID, GatewayID: "stripe", Provider: gateway.ProviderStripe,
	}

	handler := NewTenantGatewayHandler(store)
	req := httptest.NewRequest("DELETE", "/tenants/"+tenantID.String()+"/gateways/stripe", nil)
	req.SetPathValue("tenant_id", tenantID.String())
	req.SetPathValue("gateway_id", "stripe")
	req = req.WithContext(middleware.ContextWithPrincipal(req.Context(), middleware.Principal{Role: middleware.RoleOps}))
	w := httptest.NewRecorder()

	handler.Delete(w, req)
	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w.Code)
	}
}
