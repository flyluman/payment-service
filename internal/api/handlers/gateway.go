package handlers

import (
	"context"
	"net/http"
	"strings"

	"github.com/crownroutes/payment-service/internal/ports"
)

type GatewayService interface {
	ListActiveGateways(ctx context.Context, paymentMethods []string) ([]*ports.GatewayConfig, error)
}

type GatewayHandler struct {
	svc GatewayService
}

func NewGatewayHandler(svc GatewayService) *GatewayHandler {
	return &GatewayHandler{svc: svc}
}

type gatewayResponse struct {
	GatewayID           string   `json:"gateway_id"`
	DisplayName         string   `json:"display_name"`
	SupportedMethods    []string `json:"supported_methods"`
	SupportedCurrencies []string `json:"supported_currencies"`
}

func (h *GatewayHandler) List(w http.ResponseWriter, r *http.Request) {
	methods := strings.Split(strings.TrimSpace(r.URL.Query().Get("payment_method")), ",")
	if len(methods) == 0 || (len(methods) == 1 && methods[0] == "") {
		methods = []string{"card", "upi", "netbanking", "wallet"}
	}

	configs, err := h.svc.ListActiveGateways(r.Context(), methods)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, "gateway_list_failed", "could not list gateways")
		return
	}

	out := make([]gatewayResponse, len(configs))
	for i, cfg := range configs {
		out[i] = gatewayResponse{
			GatewayID:           cfg.GatewayID,
			DisplayName:         cfg.DisplayName,
			SupportedMethods:    append([]string(nil), cfg.SupportedMethods...),
			SupportedCurrencies: append([]string(nil), cfg.SupportedCurrencies...),
		}
	}

	writeJSON(w, r, http.StatusOK, out)
}
