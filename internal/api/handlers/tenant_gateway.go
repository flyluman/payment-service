package handlers

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/api/middleware"
	"github.com/crownroutes/payment-service/internal/domain/gateway"
	"github.com/crownroutes/payment-service/internal/ports"
)

type TenantGatewayStore interface {
	Get(ctx context.Context, tenantID uuid.UUID, gatewayID string) (*gateway.TenantGatewayConfig, error)
	ListByTenant(ctx context.Context, tenantID uuid.UUID) ([]*gateway.TenantGatewayConfig, error)
	ListActiveByTenant(ctx context.Context, tenantID uuid.UUID) ([]*gateway.TenantGatewayConfig, error)
	Upsert(ctx context.Context, cfg *gateway.TenantGatewayConfig) (int, error)
	SetActive(ctx context.Context, tenantID uuid.UUID, gatewayID string, active bool) error
	Delete(ctx context.Context, tenantID uuid.UUID, gatewayID string) error
}

type TenantGatewayHandler struct {
	store TenantGatewayStore
	audit ports.AuditLogStore
}

func NewTenantGatewayHandler(store TenantGatewayStore) *TenantGatewayHandler {
	return &TenantGatewayHandler{store: store}
}

func (h *TenantGatewayHandler) SetAuditLogStore(a ports.AuditLogStore) {
	h.audit = a
}

func (h *TenantGatewayHandler) checkAuth(w http.ResponseWriter, r *http.Request, tenantID string) bool {
	p, ok := middleware.PrincipalFromContext(r.Context())
	if !ok {
		writeError(w, r, http.StatusUnauthorized, "unauthorized", "missing authentication")
		return false
	}
	if p.Role == middleware.RoleOps {
		return true
	}
	if p.TenantID != tenantID {
		writeError(w, r, http.StatusForbidden, "forbidden", "cannot access other tenant's resources")
		return false
	}
	return true
}

type tenantGatewayResponse struct {
	GatewayID string `json:"gateway_id"`
	Provider  string `json:"provider"`
	IsActive  bool   `json:"is_active"`
}

type upsertTenantGatewayRequest struct {
	GatewayID string          `json:"gateway_id"`
	Provider  string          `json:"provider"`
	Config    json.RawMessage `json:"config"`
	IsActive  *bool           `json:"is_active"`
}

func (h *TenantGatewayHandler) List(w http.ResponseWriter, r *http.Request) {
	if !h.checkAuth(w, r, r.PathValue("tenant_id")) {
		return
	}
	tenantID, err := uuid.Parse(r.PathValue("tenant_id"))
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "invalid_tenant_id", "tenant_id must be a valid UUID")
		return
	}

	configs, err := h.store.ListByTenant(r.Context(), tenantID)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, "list_failed", "could not list tenant gateways")
		return
	}

	out := make([]tenantGatewayResponse, 0, len(configs))
	for _, cfg := range configs {
		out = append(out, tenantGatewayResponse{
			GatewayID: cfg.GatewayID,
			Provider:  string(cfg.Provider),
			IsActive:  cfg.IsActive,
		})
	}
	writeJSON(w, r, http.StatusOK, out)
}

func (h *TenantGatewayHandler) Get(w http.ResponseWriter, r *http.Request) {
	if !h.checkAuth(w, r, r.PathValue("tenant_id")) {
		return
	}
	tenantID, err := uuid.Parse(r.PathValue("tenant_id"))
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "invalid_tenant_id", "tenant_id must be a valid UUID")
		return
	}
	gatewayID := r.PathValue("gateway_id")
	if gatewayID == "" {
		writeError(w, r, http.StatusBadRequest, "missing_gateway_id", "gateway_id is required")
		return
	}

	cfg, err := h.store.Get(r.Context(), tenantID, gatewayID)
	if err != nil {
		writeError(w, r, http.StatusNotFound, "not_found", "tenant gateway config not found")
		return
	}
	writeJSON(w, r, http.StatusOK, tenantGatewayResponse{
		GatewayID: cfg.GatewayID,
		Provider:  string(cfg.Provider),
		IsActive:  cfg.IsActive,
	})
}

func (h *TenantGatewayHandler) Upsert(w http.ResponseWriter, r *http.Request) {
	if !h.checkAuth(w, r, r.PathValue("tenant_id")) {
		return
	}
	tenantID, err := uuid.Parse(r.PathValue("tenant_id"))
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "invalid_tenant_id", "tenant_id must be a valid UUID")
		return
	}

	var req upsertTenantGatewayRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, r, http.StatusBadRequest, "invalid_json", "request body must be valid JSON")
		return
	}
	if req.GatewayID == "" {
		writeError(w, r, http.StatusBadRequest, "missing_gateway_id", "gateway_id is required")
		return
	}
	if req.Provider == "" {
		writeError(w, r, http.StatusBadRequest, "missing_provider", "provider is required")
		return
	}
	provider := gateway.Provider(req.Provider)
	if !gateway.ValidProvider(provider) {
		writeError(w, r, http.StatusBadRequest, "invalid_provider", "provider must be one of: stripe, razorpay")
		return
	}
	if len(req.Config) == 0 {
		writeError(w, r, http.StatusBadRequest, "missing_config", "config payload is required")
		return
	}

	active := true
	if req.IsActive != nil {
		active = *req.IsActive
	}

	cfg := &gateway.TenantGatewayConfig{
		TenantID:  tenantID,
		GatewayID: req.GatewayID,
		Provider:  provider,
		Config:    req.Config,
		IsActive:  active,
	}

	_, err = h.store.Upsert(r.Context(), cfg)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, "upsert_failed", "could not save tenant gateway config")
		return
	}

	writeJSON(w, r, http.StatusOK, map[string]any{
		"gateway_id": req.GatewayID,
		"provider":   req.Provider,
		"is_active":  active,
	})
}

func (h *TenantGatewayHandler) Delete(w http.ResponseWriter, r *http.Request) {
	if !h.checkAuth(w, r, r.PathValue("tenant_id")) {
		return
	}
	tenantID, err := uuid.Parse(r.PathValue("tenant_id"))
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "invalid_tenant_id", "tenant_id must be a valid UUID")
		return
	}
	gatewayID := r.PathValue("gateway_id")
	if gatewayID == "" {
		writeError(w, r, http.StatusBadRequest, "missing_gateway_id", "gateway_id is required")
		return
	}

	if err := h.store.Delete(r.Context(), tenantID, gatewayID); err != nil {
		writeError(w, r, http.StatusInternalServerError, "delete_failed", "could not delete tenant gateway config")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
