package handlers

import (
	"encoding/json"
	"net/http"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/api/middleware"
	"github.com/crownroutes/payment-service/internal/ports"
)

type TenantWebhookConfigHandler struct {
	store ports.TenantWebhookConfigWriter
}

func NewTenantWebhookConfigHandler(store ports.TenantWebhookConfigWriter) *TenantWebhookConfigHandler {
	return &TenantWebhookConfigHandler{store: store}
}

type twhConfigRequest struct {
	EndpointURL   string `json:"endpoint_url"`
	SigningSecret string `json:"signing_secret"`
	IsActive      *bool  `json:"is_active"`
}

type twhConfigResponse struct {
	TenantID      string `json:"tenant_id"`
	EndpointURL   string `json:"endpoint_url"`
	SigningSecret string `json:"signing_secret"`
	IsActive      bool   `json:"is_active"`
}

func (h *TenantWebhookConfigHandler) List(w http.ResponseWriter, r *http.Request) {
	if _, ok := middleware.PrincipalFromContext(r.Context()); !ok {
		writeError(w, r, http.StatusUnauthorized, "unauthorized", "missing authentication")
		return
	}

	var (
		configs []*ports.TenantWebhookConfig
		err     error
	)
	if t := tenantIDFromCtx(r); t != uuid.Nil {
		configs, err = h.store.ListByTenant(r.Context(), t)
	} else {
		configs, err = h.store.List(r.Context())
	}
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, "list_failed", "could not list webhook configs")
		return
	}

	out := make([]twhConfigResponse, 0, len(configs))
	for _, c := range configs {
		out = append(out, twhConfigResponse{
			TenantID:      c.TenantID.String(),
			EndpointURL:   c.EndpointURL,
			SigningSecret: redactSecret(c.SigningSecret),
			IsActive:      c.IsActive,
		})
	}
	writeJSON(w, r, http.StatusOK, out)
}

func (h *TenantWebhookConfigHandler) Upsert(w http.ResponseWriter, r *http.Request) {
	if _, ok := middleware.PrincipalFromContext(r.Context()); !ok {
		writeError(w, r, http.StatusUnauthorized, "unauthorized", "missing authentication")
		return
	}

	tenantID, err := uuid.Parse(r.PathValue("tenant_id"))
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "invalid_tenant_id", "tenant_id must be a valid UUID")
		return
	}

	if t := tenantIDFromCtx(r); t != uuid.Nil && t != tenantID {
		writeError(w, r, http.StatusForbidden, "forbidden", "cannot manage another tenant's webhook config")
		return
	}

	var req twhConfigRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, r, http.StatusBadRequest, "invalid_json", "request body must be valid JSON")
		return
	}
	if req.EndpointURL == "" {
		writeError(w, r, http.StatusBadRequest, "missing_endpoint_url", "endpoint_url is required")
		return
	}

	isActive := true
	if req.IsActive != nil {
		isActive = *req.IsActive
	}

	cfg := &ports.TenantWebhookConfig{
		TenantID:      tenantID,
		EndpointURL:   req.EndpointURL,
		SigningSecret: req.SigningSecret,
		IsActive:      isActive,
	}

	if err := h.store.Upsert(r.Context(), cfg); err != nil {
		writeError(w, r, http.StatusInternalServerError, "upsert_failed", "could not upsert webhook config")
		return
	}

	writeJSON(w, r, http.StatusOK, twhConfigResponse{
		TenantID:      cfg.TenantID.String(),
		EndpointURL:   cfg.EndpointURL,
		SigningSecret: redactSecret(cfg.SigningSecret),
		IsActive:      cfg.IsActive,
	})
}

func redactSecret(secret string) string {
	if secret == "" {
		return ""
	}
	if len(secret) <= 4 {
		return "****"
	}
	return "****" + secret[len(secret)-4:]
}
