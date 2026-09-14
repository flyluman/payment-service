package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/api/middleware"
	"github.com/crownroutes/payment-service/internal/api/response"
	"github.com/crownroutes/payment-service/internal/app/payment"
	"github.com/crownroutes/payment-service/internal/domain/transaction"
	"github.com/crownroutes/payment-service/internal/ports"
)

type TransactionGetter interface {
	GetByID(ctx context.Context, id uuid.UUID) (*transaction.Txn, error)
}

type SSEHandler struct {
	bus  ports.EventBus
	repo TransactionGetter
	log  ports.Logger
}

func NewSSEHandler(bus ports.EventBus, repo TransactionGetter, log ports.Logger) *SSEHandler {
	return &SSEHandler{bus: bus, repo: repo, log: log}
}

func (h *SSEHandler) Events(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		response.WriteError(w, http.StatusBadRequest, "invalid_id", "transaction id must be a valid UUID", middleware.RequestIDFromContext(r.Context()))
		return
	}

	txn, err := h.repo.GetByID(r.Context(), id)
	if err != nil {
		response.WriteError(w, http.StatusNotFound, "not_found", "transaction not found", middleware.RequestIDFromContext(r.Context()))
		return
	}

	tenantID := middleware.TenantIDFromContext(r.Context())
	if tenantID != "" {
		if txn.TenantID.String() != tenantID {
			response.WriteError(w, http.StatusNotFound, "not_found", "transaction not found", middleware.RequestIDFromContext(r.Context()))
			return
		}
	} else {
		token := r.URL.Query().Get("token")
		if token == "" || !payment.VerifyToken(token, txn.TokenHash) {
			response.WriteError(w, http.StatusUnauthorized, "unauthorized", "invalid or missing token", middleware.RequestIDFromContext(r.Context()))
			return
		}
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		response.WriteError(w, http.StatusInternalServerError, "streaming_not_supported", "server-sent events not supported", middleware.RequestIDFromContext(r.Context()))
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ch := h.bus.Subscribe(id)
	defer h.bus.Unsubscribe(id, ch)

	fmt.Fprintf(w, "event: connected\ndata: {}\n\n")
	flusher.Flush()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case ev := <-ch:
			data, _ := json.Marshal(map[string]string{"status": string(ev.Status)})
			fmt.Fprintf(w, "event: status\ndata: %s\n\n", data)
			flusher.Flush()
		case <-ticker.C:
			fmt.Fprintf(w, ": heartbeat\n\n")
			flusher.Flush()
		}
	}
}
