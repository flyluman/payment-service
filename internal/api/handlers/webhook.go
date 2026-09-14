package handlers

import (
	"context"
	"errors"
	"io"
	"net/http"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/api/middleware"
	appwebhook "github.com/crownroutes/payment-service/internal/app/webhook"
	"github.com/crownroutes/payment-service/internal/domain/gateway"
	"github.com/crownroutes/payment-service/internal/domain/transaction"
	"github.com/crownroutes/payment-service/internal/ports"
)

type WebhookProcessor interface {
	Process(ctx context.Context, gatewayID string, ev appwebhook.Event, rawPayload []byte) (appwebhook.Outcome, error)
}
type WebhookParserResolver interface {
	WebhookParser(gatewayID string) (ports.GatewayWebhookParser, bool)
	WebhookRequiresRefetch(gatewayID string) bool
	CheckStatus(ctx context.Context, gatewayID string, req ports.GatewayStatusRequest) (*ports.GatewayPaymentResponse, error)
}
type WebhookTransactionGetter interface {
	GetByID(ctx context.Context, id uuid.UUID) (*transaction.Txn, error)
	GetByGatewayReference(ctx context.Context, gatewayID, reference string) (*transaction.Txn, error)
}
type WebhookTenantConfigGetter interface {
	Get(ctx context.Context, tenantID uuid.UUID, gatewayID string) (*gateway.TenantGatewayConfig, error)
	ListByGateway(ctx context.Context, gatewayID string) ([]*gateway.TenantGatewayConfig, error)
}

type DisputeIngestor interface {
	IngestGatewayDispute(ctx context.Context, gatewayID string, ev ports.GatewayDisputeEvent) error
}

type WebhookHandler struct {
	processor WebhookProcessor
	parsers   WebhookParserResolver
	txns      WebhookTransactionGetter
	tenants   WebhookTenantConfigGetter
	disputes  DisputeIngestor
	log       ports.Logger
}

func NewWebhookHandler(processor WebhookProcessor, parsers WebhookParserResolver, txns WebhookTransactionGetter, tenants WebhookTenantConfigGetter, disputes DisputeIngestor, log ports.Logger) *WebhookHandler {
	return &WebhookHandler{processor: processor, parsers: parsers, txns: txns, tenants: tenants, disputes: disputes, log: log}
}

func (h *WebhookHandler) Handle(w http.ResponseWriter, r *http.Request) {
	log := h.log
	if ctxLog := middleware.LoggerFromContext(r.Context()); ctxLog != nil {
		log = ctxLog
	}

	gatewayID := r.PathValue("gateway_id")

	txnIDStr := r.URL.Query().Get("transaction_id")
	var txnID uuid.UUID
	if txnIDStr != "" {
		var err error
		txnID, err = uuid.Parse(txnIDStr)
		if err != nil {
			writeError(w, r, http.StatusBadRequest, "invalid_transaction_id", "transaction_id must be a valid UUID")
			return
		}
	}

	var txn *transaction.Txn
	var tenantID uuid.UUID
	if txnIDStr != "" {
		var err error
		txn, err = h.txns.GetByID(r.Context(), txnID)
		if err != nil {
			writeError(w, r, http.StatusNotFound, "transaction_not_found", "transaction not found")
			return
		}
		tenantID = txn.TenantID
	} else {
		tcfg, err := h.tenants.ListByGateway(r.Context(), gatewayID)
		if err != nil || len(tcfg) == 0 {
			writeError(w, r, http.StatusUnauthorized, "unknown_tenant", "no tenant config for gateway")
			return
		}
		if len(tcfg) > 1 {
			h.log.Warn(ports.LogEventWebhookInboundInvalid, map[string]any{
				ports.FieldGatewayID: gatewayID,
				"reason":             "multiple_tenants_on_gateway_without_transaction_id",
			})
			writeError(w, r, http.StatusBadRequest, "ambiguous_tenant", "gateway is shared by multiple tenants; pass transaction_id")
			return
		}
		tenantID = tcfg[0].TenantID
	}

	rawCfg, err := h.tenants.Get(r.Context(), tenantID, gatewayID)
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

	// A transaction that was routed to a different gateway must not be advanced
	// by this webhook. Checked only after confirming the gateway is registered
	// so an unknown gateway yields 404 (above) rather than 400.
	if txn != nil && txn.GatewayID != gatewayID && txn.ActualGateway != gatewayID {
		writeError(w, r, http.StatusBadRequest, "gateway_mismatch", "transaction belongs to a different gateway")
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

	// Gateways with unsigned webhooks (e.g. FIB) must not have their status
	// taken from the payload; re-fetch the authoritative status instead.
	if h.parsers.WebhookRequiresRefetch(gatewayID) {
		refTxn, err := h.txns.GetByGatewayReference(r.Context(), gatewayID, ev.GatewayReferenceID)
		if err != nil {
			writeError(w, r, http.StatusInternalServerError, "webhook_processing_failed", "could not resolve webhook transaction")
			return
		}
		if refTxn == nil {
			writeJSON(w, r, http.StatusOK, map[string]any{"unknown_txn": true})
			return
		}
		if txnIDStr != "" && txn != nil && refTxn.ID != txn.ID {
			writeError(w, r, http.StatusBadRequest, "transaction_mismatch", "webhook reference does not match transaction")
			return
		}
		statusResp, err := h.parsers.CheckStatus(r.Context(), gatewayID, ports.GatewayStatusRequest{
			TransactionID:      refTxn.ID,
			TenantID:           refTxn.TenantID,
			GatewayReferenceID: ev.GatewayReferenceID,
			IdempotencyKey:     refTxn.GatewayIdempotencyKey,
		})
		if err != nil {
			h.log.Warn(ports.LogEventWebhookInboundInvalid, map[string]any{
				ports.FieldGatewayID: gatewayID,
				"reason":             "status_refetch_failed",
			})
			writeError(w, r, http.StatusInternalServerError, "status_refetch_failed", "could not verify payment status")
			return
		}
		ev.Status = statusResp.Status
	}

	if ev.Dispute != nil {
		if h.disputes == nil {
			writeError(w, r, http.StatusNotImplemented, "dispute_not_supported", "dispute webhooks not configured")
			return
		}
		if err := h.disputes.IngestGatewayDispute(r.Context(), gatewayID, *ev.Dispute); err != nil {
			log.Warn(ports.LogEventWebhookInboundInvalid, map[string]any{ports.FieldGatewayID: gatewayID, "reason": "dispute_ingest_failed"})
			writeError(w, r, http.StatusInternalServerError, "dispute_ingest_failed", "could not process dispute webhook")
			return
		}
		writeJSON(w, r, http.StatusOK, map[string]any{"accepted": true})
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
		"duplicate":   outcome.Duplicate,
		"resolved":    outcome.Resolved,
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
	case ports.GatewayPaymentStatusAuthorized:
		return "authorized"
	case ports.GatewayPaymentStatusFailed:
		return "failed"
	case ports.GatewayPaymentStatusCancelled:
		return "cancelled"
	case ports.GatewayPaymentStatusRefunded:
		return "refunded"
	default:
		return "pending"
	}
}
