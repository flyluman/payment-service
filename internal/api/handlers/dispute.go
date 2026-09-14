package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/api/middleware"
	"github.com/crownroutes/payment-service/internal/domain/dispute"
	"github.com/crownroutes/payment-service/internal/ports"
)

type DisputeService interface {
	CreateDispute(ctx context.Context, d *dispute.Dispute) error
	GetDispute(ctx context.Context, tenantID, id uuid.UUID) (*dispute.Dispute, error)
	ListDisputes(ctx context.Context, filters ports.DisputeFilters) ([]*dispute.Dispute, error)
	SubmitEvidence(ctx context.Context, tenantID, disputeID uuid.UUID, ev *dispute.Evidence) error
	ListEvidence(ctx context.Context, tenantID, disputeID uuid.UUID) ([]*dispute.Evidence, error)
}

type DisputeHandler struct {
	svc DisputeService
}

func NewDisputeHandler(svc DisputeService) *DisputeHandler {
	return &DisputeHandler{svc: svc}
}

type disputeResponse struct {
	ID                  string  `json:"id"`
	TransactionID       string  `json:"transaction_id"`
	GatewayID           string  `json:"gateway_id"`
	Reason              string  `json:"reason"`
	Status              string  `json:"status"`
	Amount              int64   `json:"amount"`
	Currency            string  `json:"currency"`
	EvidenceDueBy       *string `json:"evidence_due_by,omitempty"`
	EvidenceSubmittedAt *string `json:"evidence_submitted_at,omitempty"`
	CreatedAt           string  `json:"created_at"`
	ResolvedAt          *string `json:"resolved_at,omitempty"`
	IsUrgent            bool    `json:"is_urgent"`
	DaysUntilDue        *int    `json:"days_until_due,omitempty"`
}

type evidenceResponse struct {
	ID          string `json:"id"`
	DisputeID   string `json:"dispute_id"`
	Type        string `json:"evidence_type"`
	FileURL     string `json:"file_url"`
	Notes       string `json:"notes,omitempty"`
	SubmittedAt string `json:"submitted_at"`
}

func toDisputeResponse(d *dispute.Dispute) disputeResponse {
	r := disputeResponse{
		ID:            d.ID.String(),
		TransactionID: d.TransactionID.String(),
		GatewayID:     d.GatewayID,
		Reason:        d.Reason,
		Status:        string(d.Status),
		Amount:        d.Amount,
		Currency:      d.Currency,
		CreatedAt:     d.CreatedAt.Format(time.RFC3339Nano),
		IsUrgent:      d.IsUrgent(7),
		DaysUntilDue:  d.DaysUntilDue(),
	}
	if d.EvidenceDueBy != nil {
		s := d.EvidenceDueBy.Format(time.RFC3339)
		r.EvidenceDueBy = &s
	}
	if d.EvidenceSubmittedAt != nil {
		s := d.EvidenceSubmittedAt.Format(time.RFC3339)
		r.EvidenceSubmittedAt = &s
	}
	if d.ResolvedAt != nil {
		s := d.ResolvedAt.Format(time.RFC3339)
		r.ResolvedAt = &s
	}
	return r
}

func toEvidenceResponse(e *dispute.Evidence) evidenceResponse {
	return evidenceResponse{
		ID:          e.ID.String(),
		DisputeID:   e.DisputeID.String(),
		Type:        string(e.Type),
		FileURL:     e.FileURL,
		Notes:       e.Notes,
		SubmittedAt: e.SubmittedAt.Format(time.RFC3339),
	}
}

func tenantIDFromCtx(r *http.Request) uuid.UUID {
	t := middleware.TenantIDFromContext(r.Context())
	if t == "" {
		return uuid.Nil
	}
	id, err := uuid.Parse(t)
	if err != nil {
		return uuid.Nil
	}
	return id
}

func (h *DisputeHandler) List(w http.ResponseWriter, r *http.Request) {
	if _, ok := middleware.PrincipalFromContext(r.Context()); !ok {
		writeError(w, r, http.StatusUnauthorized, "unauthorized", "missing authentication")
		return
	}

	filters := ports.DisputeFilters{
		Limit: 50,
	}
	if t := middleware.TenantIDFromContext(r.Context()); t != "" {
		if id, err := uuid.Parse(t); err == nil {
			filters.TenantID = id
		}
	}
	if s := r.URL.Query().Get("status"); s != "" {
		st := dispute.Status(s)
		filters.Status = &st
	}
	if g := r.URL.Query().Get("gateway_id"); g != "" {
		filters.GatewayID = &g
	}
	if from := r.URL.Query().Get("date_from"); from != "" {
		t, err := time.Parse(time.RFC3339, from)
		if err != nil {
			writeError(w, r, http.StatusBadRequest, "invalid_from", "from must be RFC3339")
			return
		}
		filters.DateFrom = &t
	}
	if to := r.URL.Query().Get("date_to"); to != "" {
		t, err := time.Parse(time.RFC3339, to)
		if err != nil {
			writeError(w, r, http.StatusBadRequest, "invalid_to", "to must be RFC3339")
			return
		}
		filters.DateTo = &t
	}

	disputes, err := h.svc.ListDisputes(r.Context(), filters)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, "list_failed", "could not list disputes")
		return
	}

	out := make([]disputeResponse, 0, len(disputes))
	for _, d := range disputes {
		out = append(out, toDisputeResponse(d))
	}
	writeJSON(w, r, http.StatusOK, out)
}

func (h *DisputeHandler) Get(w http.ResponseWriter, r *http.Request) {
	if _, ok := middleware.PrincipalFromContext(r.Context()); !ok {
		writeError(w, r, http.StatusUnauthorized, "unauthorized", "missing authentication")
		return
	}
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "invalid_id", "id must be a valid UUID")
		return
	}

	d, err := h.svc.GetDispute(r.Context(), tenantIDFromCtx(r), id)
	if err != nil {
		writeError(w, r, http.StatusNotFound, "not_found", "dispute not found")
		return
	}
	writeJSON(w, r, http.StatusOK, toDisputeResponse(d))
}

func (h *DisputeHandler) SubmitEvidence(w http.ResponseWriter, r *http.Request) {
	if _, ok := middleware.PrincipalFromContext(r.Context()); !ok {
		writeError(w, r, http.StatusUnauthorized, "unauthorized", "missing authentication")
		return
	}
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "invalid_id", "id must be a valid UUID")
		return
	}

	var req struct {
		Type    string `json:"evidence_type"`
		FileURL string `json:"file_url"`
		Notes   string `json:"notes"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, r, http.StatusBadRequest, "invalid_json", "request body must be valid JSON")
		return
	}
	if req.Type == "" || req.FileURL == "" {
		writeError(w, r, http.StatusBadRequest, "missing_fields", "evidence_type and file_url are required")
		return
	}

	ev := &dispute.Evidence{
		ID:          uuid.New(),
		Type:        dispute.EvidenceType(req.Type),
		FileURL:     req.FileURL,
		Notes:       req.Notes,
		SubmittedAt: time.Now().UTC(),
	}

	if err := h.svc.SubmitEvidence(r.Context(), tenantIDFromCtx(r), id, ev); err != nil {
		if errors.Is(err, dispute.ErrNotFound) {
			writeError(w, r, http.StatusNotFound, "not_found", "dispute not found")
			return
		}
		writeError(w, r, http.StatusConflict, "submit_failed", err.Error())
		return
	}
	writeJSON(w, r, http.StatusCreated, toEvidenceResponse(ev))
}

func (h *DisputeHandler) ListEvidence(w http.ResponseWriter, r *http.Request) {
	if _, ok := middleware.PrincipalFromContext(r.Context()); !ok {
		writeError(w, r, http.StatusUnauthorized, "unauthorized", "missing authentication")
		return
	}
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "invalid_id", "id must be a valid UUID")
		return
	}

	evidence, err := h.svc.ListEvidence(r.Context(), tenantIDFromCtx(r), id)
	if err != nil {
		if errors.Is(err, dispute.ErrNotFound) {
			writeError(w, r, http.StatusNotFound, "not_found", "dispute not found")
			return
		}
		writeError(w, r, http.StatusInternalServerError, "list_failed", "could not list evidence")
		return
	}

	out := make([]evidenceResponse, 0, len(evidence))
	for _, e := range evidence {
		out = append(out, toEvidenceResponse(e))
	}
	writeJSON(w, r, http.StatusOK, out)
}
