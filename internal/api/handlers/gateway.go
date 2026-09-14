package handlers

import (
	"context"
	"net/http"
	"strconv"
	"strings"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/domain/fees"
	"github.com/crownroutes/payment-service/internal/ports"
)

type GatewayService interface {
	ListActiveGateways(ctx context.Context, paymentMethods []string) ([]*ports.GatewayConfig, error)
}

type FeeEstimator interface {
	GetFeeModel(ctx context.Context, gatewayID, paymentMethod string) (*ports.GatewayFeeModel, error)
	GetCurrencyRates(ctx context.Context, tenantID uuid.UUID) ([]fees.CurrencyRate, error)
}

type GatewayHandler struct {
	svc  GatewayService
	fees FeeEstimator
}

func NewGatewayHandler(svc GatewayService, fe FeeEstimator) *GatewayHandler {
	return &GatewayHandler{svc: svc, fees: fe}
}

type gatewayResponse struct {
	GatewayID           string          `json:"gateway_id"`
	DisplayName         string          `json:"display_name"`
	SupportedMethods    []string        `json:"supported_methods"`
	SupportedCurrencies []string        `json:"supported_currencies"`
	FeeBreakdown        *fees.Breakdown `json:"fee_breakdown,omitempty"`
}

func (h *GatewayHandler) List(w http.ResponseWriter, r *http.Request) {
	methods := strings.Split(strings.TrimSpace(r.URL.Query().Get("payment_method")), ",")
	if len(methods) == 0 || (len(methods) == 1 && methods[0] == "") {
		methods = []string{"card", "upi", "netbanking", "wallet"}
	}

	currency := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("currency")))
	var amount int64
	if a := r.URL.Query().Get("amount"); a != "" {
		amount, _ = strconv.ParseInt(a, 10, 64)
	}
	var tenantID uuid.UUID
	if tid := r.URL.Query().Get("tenant_id"); tid != "" {
		tenantID, _ = uuid.Parse(tid)
	}
	computeFees := currency != "" && amount > 0 && h.fees != nil

	var rates []fees.CurrencyRate
	if computeFees && tenantID != uuid.Nil {
		rates, _ = h.fees.GetCurrencyRates(r.Context(), tenantID)
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
		if computeFees && containsCurrency(cfg.SupportedCurrencies, currency) {
			method := methods[0]
			if fm, err := h.fees.GetFeeModel(r.Context(), cfg.GatewayID, method); err == nil && fm != nil {
				chargesCurrency := fm.ChargesCurrency
				if chargesCurrency == "" {
					chargesCurrency = currency
				}
				var fixedFee float64
				if fm.FixedFee > 0 {
					fixedFee = float64(fm.FixedFee)
				}
				var ratio float64
				if fm.PercentageBPS > 0 {
					ratio = float64(fm.PercentageBPS) / 100.0
				}
				if bd, err := fees.Calculate(amount, currency, chargesCurrency, fixedFee, ratio, rates); err == nil {
					if verr := fees.Validate(bd); verr == nil {
						out[i].FeeBreakdown = bd
					}
				}
			}
		}
	}

	writeJSON(w, r, http.StatusOK, out)
}

func containsCurrency(currencies []string, target string) bool {
	for _, c := range currencies {
		if strings.EqualFold(c, target) {
			return true
		}
	}
	return false
}
