package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/api/middleware"
	"github.com/crownroutes/payment-service/internal/app/idempotency"
	apprefund "github.com/crownroutes/payment-service/internal/app/refund"
	"github.com/crownroutes/payment-service/internal/domain/transaction"
	domainrefund "github.com/crownroutes/payment-service/internal/domain/refund"
)

type RefundService interface {
	InitiateRefund(ctx context.Context, in apprefund.InitiateInput) (apprefund.InitiateResult, error)
	ProcessRefund(ctx context.Context, refundID uuid.UUID) (*domainrefund.Refund, error)
}

type RefundTransactionGetter interface {
	GetByID(ctx context.Context, id uuid.UUID) (*transaction.Txn, error)
}

type RefundHandler struct {
	svc   RefundService
	txnBy RefundTransactionGetter
}

func NewRefundHandler(svc RefundService, txnBy RefundTransactionGetter) *RefundHandler {
	return &RefundHandler{svc: svc, txnBy: txnBy}
}

type initiateRefundRequest struct {
	Amount int64  `json:"amount"`
	Reason string `json:"reason"`
}

type initiateRefundResponse struct {
	ID              string  `json:"id"`
	TransactionID   string  `json:"transaction_id"`
	Status          string  `json:"status"`
	Amount          int64   `json:"amount"`
	GatewayRefundID *string `json:"gateway_refund_id,omitempty"`
}

func (h *RefundHandler) Initiate(w http.ResponseWriter, r *http.Request) {
	transactionID, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "invalid_id", "transaction id must be a valid UUID")
		return
	}

	// Tenant isolation: verify the transaction belongs to the caller's tenant.
	if principal, ok := middleware.PrincipalFromContext(r.Context()); ok && principal.Role == middleware.RoleService {
		txn, err := h.txnBy.GetByID(r.Context(), transactionID)
		if err != nil {
			writeError(w, r, http.StatusNotFound, "not_found", "transaction not found")
			return
		}
		if txn.TenantID.String() != principal.TenantID {
			writeError(w, r, http.StatusNotFound, "not_found", "transaction not found")
			return
		}
	}

	var req initiateRefundRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, r, http.StatusBadRequest, "invalid_request_body", "request body is not valid JSON")
		return
	}
	if req.Amount <= 0 {
		writeError(w, r, http.StatusBadRequest, "invalid_amount", "amount must be positive")
		return
	}
	if req.Reason == "" {
		writeError(w, r, http.StatusBadRequest, "missing_reason", "reason is required")
		return
	}

	idemKey := r.Header.Get("Idempotency-Key")
	if idemKey == "" {
		writeError(w, r, http.StatusBadRequest, "missing_idempotency_key", "Idempotency-Key header is required")
		return
	}

	result, err := h.svc.InitiateRefund(r.Context(), apprefund.InitiateInput{
		TransactionID:  transactionID,
		Amount:         req.Amount,
		Reason:         req.Reason,
		InitiatedBy:    "api",
		IdempotencyKey: idemKey,
	})
	if err != nil {
		var over domainrefund.ErrOverRefund
		switch {
		case errors.Is(err, apprefund.ErrNotRefundable):
			writeError(w, r, http.StatusUnprocessableEntity, "not_refundable", "transaction is not in a refundable state")
		case errors.As(err, &over):
			writeError(w, r, http.StatusUnprocessableEntity, "over_refund", over.Error())
		case errors.Is(err, idempotency.ErrKeyRequired):
			writeError(w, r, http.StatusBadRequest, "missing_idempotency_key", "Idempotency-Key header is required")
		default:
			writeError(w, r, http.StatusInternalServerError, "refund_failed", "could not initiate refund")
		}
		return
	}

	switch result.Verdict {
	case idempotency.Created:
		processed, err := h.svc.ProcessRefund(r.Context(), result.Refund.ID)
		if err != nil {
			writeJSON(w, r, http.StatusAccepted, toInitiateRefundResponse(result.Refund))
			return
		}
		writeJSON(w, r, http.StatusCreated, toInitiateRefundResponse(processed))
	case idempotency.Replayed:
		writeJSON(w, r, http.StatusOK, toInitiateRefundResponse(result.Refund))
	case idempotency.InProgress:
		writeError(w, r, http.StatusConflict, "idempotency_in_progress", "a request with this idempotency key is already in progress")
	case idempotency.KeyReused:
		writeError(w, r, http.StatusConflict, "idempotency_key_reused", "idempotency key reused with a different request body")
	default:
		writeError(w, r, http.StatusInternalServerError, "refund_failed", "could not initiate refund")
	}
}

func toInitiateRefundResponse(rf *domainrefund.Refund) initiateRefundResponse {
	resp := initiateRefundResponse{
		ID:            rf.ID.String(),
		TransactionID: rf.TransactionID.String(),
		Status:        string(rf.Status),
		Amount:        rf.Amount,
	}
	if rf.GatewayRefundID != "" {
		resp.GatewayRefundID = &rf.GatewayRefundID
	}
	return resp
}
