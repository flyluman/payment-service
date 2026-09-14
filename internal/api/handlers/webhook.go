package handlers

import (
	"context"
	"errors"
	"io"
	"net/http"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/api/middleware"
	"github.com/crownroutes/payment-service/internal/domain/gateway"
	"github.com/crownroutes/payment-service/internal/domain/transaction"
	appwebhook "github.com/crownroutes/payment-service/internal/app/webhook"
	"github.com/crownroutes/payment-service/internal/ports"
)

type WebhookProcessor interface {
	Process(ctx context.Context, gatewayID string, ev appwebhook.Event, rawPayload []byte) (appwebhook.Outcome, error)
}
type WebhookParserResolver interface {
	WebhookParser(gatewayID string) (ports.GatewayWebhookParser, bool)
}
type WebhookTransactionGetter interface {
	GetByID(ctx context.Context, id uuid.UUID) (*transaction.Txn, error)
}
type WebhookTenantConfigGetter interface {
	Get(ctx context.Context, tenantID uuid.UUID, gatewayID string) (*gateway.TenantGatewayConfig, error)
}

type WebhookHandler struct {
	processor WebhookProcessor
	parsers   WebhookParserResolver
	txns      WebhookTransactionGetter
	tenants   WebhookTenantConfigGetter
	log       ports.Logger
}

func NewWebhookHandler(processor WebhookProcessor, parsers WebhookParserResolver, txns WebhookTransactionGetter, tenants WebhookTenantConfigGetter, log ports.Logger) *WebhookHandler {
	return &WebhookHandler{processor: processor, parsers: parsers, txns: txns, tenants: tenants, log: log}
}

func (h *WebhookHandler) Handle(w http.ResponseWriter, r *http.Request) {
	log := h.log
	if ctxLog := middleware.LoggerFromContext(r.Context()); ctxLog != nil {
		log = ctxLog
	}

	gatewayID := r.PathValue("gateway_id")

	txnIDStr := r.URL.Query().Get("transaction_id")
	if txnIDStr == "" {
		writeError(w, r, http.StatusBadRequest, "missing_transaction_id", "transaction_id query parameter is required")
		return
	}
	txnID, err := uuid.Parse(txnIDStr)
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "invalid_transaction_id", "transaction_id must be a valid UUID")
		return
	}

	txn, err := h.txns.GetByID(r.Context(), txnID)
	if err != nil {
		writeError(w, r, http.StatusNotFound, "transaction_not_found", "transaction not found")
		return
	}

	rawCfg, err := h.tenants.Get(r.Context(), txn.TenantID, gatewayID)
	if err != nil {
		log.Warn(ports.LogEventWebhookInboundInvalid, map[string]any{ports.FieldGatewayID: gatewayID, "reason": "tenant_config_not_found"})
		writeError(w, r, http.StatusUnauthorized, "unknown_tenant", "no tenant config for gateway")
		return
	}

	secret, err := gateway.ExtractWebhookSecret(gateway.Provider(rawCfg.Provider), rawCfg.Config)
	if err != nil {
		log.Warn(ports.LogEventWebhookInboundInvalid, map[string]any{ports.FieldGatewayID: gatewayID, "reason": "extract_secret"})
		writeError(w, r, http.StatusUnauthorized, "invalid_config", "could not extract webhook secret")
		return
	}

	rawBody, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "invalid_body", "could not read webhook body")
		return
	}

	parser, ok := h.parsers.WebhookParser(gatewayID)
	if !ok {
		writeError(w, r, http.StatusNotFound, "unknown_gateway", "no webhook parser for gateway")
		return
	}

	ev, err := parser.ParseWebhook(rawBody, headerMap(r), secret)
	if errors.Is(err, ports.ErrWebhookSignature) {
		log.Warn(ports.LogEventWebhookInboundInvalid, map[string]any{ports.FieldGatewayID: gatewayID})
		writeError(w, r, http.StatusUnauthorized, "invalid_signature", "webhook signature verification failed")
		return
	}
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "invalid_webhook", "could not parse webhook payload")
		return
	}
	if ev.EventID == "" || ev.GatewayReferenceID == "" {
		writeError(w, r, http.StatusBadRequest, "invalid_webhook", "event id and reference are required")
		return
	}

	outcome, err := h.processor.Process(r.Context(), gatewayID, appwebhook.Event{
		EventID:            ev.EventID,
		GatewayReferenceID: ev.GatewayReferenceID,
		Status:             normalizeStatus(ev.Status),
	}, rawBody)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, "webhook_processing_failed", "could not process webhook")
		return
	}

	writeJSON(w, r, http.StatusOK, map[string]any{
		"duplicate":  outcome.Duplicate,
		"resolved":  outcome.Resolved,
		"unknown_txn": outcome.UnknownTxn,
	})
}

func headerMap(r *http.Request) map[string]string {
	m := make(map[string]string, len(r.Header))
	for k := range r.Header {
		m[k] = r.Header.Get(k)
	}
	return m
}

func normalizeStatus(s ports.GatewayPaymentStatus) string {
	switch s {
	case ports.GatewayPaymentStatusSucceeded:
		return "succeeded"
	case ports.GatewayPaymentStatusFailed:
		return "failed"
	case ports.GatewayPaymentStatusCancelled:
		return "cancelled"
	default:
		return "pending"
	}
}
