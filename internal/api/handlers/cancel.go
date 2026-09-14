package handlers

import (
	"context"
	"errors"
	"net/http"

	"github.com/google/uuid"

	appcancel "github.com/crownroutes/payment-service/internal/app/cancel"
	"github.com/crownroutes/payment-service/internal/app/idempotency"
	"github.com/crownroutes/payment-service/internal/api/middleware"
	"github.com/crownroutes/payment-service/internal/domain/transaction"
)

type CancelService interface {
	Cancel(ctx context.Context, in appcancel.CancelInput) (appcancel.Result, error)
}
type CancelHandler struct{ svc CancelService }

func NewCancelHandler(svc CancelService) *CancelHandler { return &CancelHandler{svc: svc} }

type cancelPaymentResponse struct {
	TransactionID string `json:"transaction_id"`
	Status        string `json:"status"`
}

func (h *CancelHandler) Cancel(w http.ResponseWriter, r *http.Request) {
	transactionID, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "invalid_id", "transaction id must be a valid UUID")
		return
	}

	tenantIDStr := middleware.TenantIDFromContext(r.Context())
	var tenantID uuid.UUID
	if tenantIDStr != "" {
		tenantID, err = uuid.Parse(tenantIDStr)
		if err != nil {
			writeError(w, r, http.StatusUnauthorized, "unauthorized", "invalid tenant")
			return
		}
	}

	idemKey := r.Header.Get("Idempotency-Key")
	if idemKey == "" {
		writeError(w, r, http.StatusBadRequest, "missing_idempotency_key", "Idempotency-Key header is required")
		return
	}

	res, err := h.svc.Cancel(r.Context(), appcancel.CancelInput{
		TransactionID:  transactionID,
		TenantID:       tenantID,
		By:             transaction.ActorTenant,
		Via:            transaction.CancelViaAPI,
		IdempotencyKey: idemKey,
	})
	if err != nil {
		if errors.Is(err, idempotency.ErrKeyRequired) {
			writeError(w, r, http.StatusBadRequest, "missing_idempotency_key", "Idempotency-Key header is required")
			return
		}
		writeError(w, r, http.StatusInternalServerError, "cancel_failed", "could not process cancellation")
		return
	}

	switch res.Verdict {
	case idempotency.InProgress:
		writeError(w, r, http.StatusConflict, "idempotency_in_progress", "a request with this idempotency key is already in progress")
	case idempotency.KeyReused:
		writeError(w, r, http.StatusConflict, "idempotency_key_reused", "idempotency key reused with a different request body")
	default:
		writeJSON(w, r, http.StatusOK, cancelPaymentResponse{
			TransactionID: transactionID.String(),
			Status:        string(res.Status),
		})
	}
}
