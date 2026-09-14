package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/api/middleware"
	"github.com/crownroutes/payment-service/internal/ports"
)

type DeadLetterService interface {
	ListDeadLetters(ctx context.Context, filter ports.DeadLetterFilter) ([]ports.DeadLetter, error)
	ReplayDeadLetter(ctx context.Context, id uuid.UUID, actor, reason string) (uuid.UUID, error)
}

type DeadLetterHandler struct {
	svc DeadLetterService
}

func NewDeadLetterHandler(svc DeadLetterService) *DeadLetterHandler {
	return &DeadLetterHandler{svc: svc}
}

func (h *DeadLetterHandler) List(w http.ResponseWriter, r *http.Request) {
	if _, ok := middleware.PrincipalFromContext(r.Context()); !ok {
		writeError(w, r, http.StatusUnauthorized, "unauthorized", "missing authentication")
		return
	}

	filter := ports.DeadLetterFilter{Limit: 50}
	if resolved := r.URL.Query().Get("resolved"); resolved != "" {
		v := resolved == "true"
		filter.Resolved = &v
	}
	if et := r.URL.Query().Get("event_type"); et != "" {
		filter.EventType = &et
	}
	if from := r.URL.Query().Get("date_from"); from != "" {
		t, err := time.Parse(time.RFC3339, from)
		if err != nil {
			writeError(w, r, http.StatusBadRequest, "invalid_date_from", "date_from must be RFC3339")
			return
		}
		filter.DateFrom = &t
	}
	if to := r.URL.Query().Get("date_to"); to != "" {
		t, err := time.Parse(time.RFC3339, to)
		if err != nil {
			writeError(w, r, http.StatusBadRequest, "invalid_date_to", "date_to must be RFC3339")
			return
		}
		filter.DateTo = &t
	}
	if limStr := r.URL.Query().Get("limit"); limStr != "" {
		v, err := strconv.Atoi(limStr)
		if err != nil || v <= 0 || v > 100 {
			writeError(w, r, http.StatusBadRequest, "invalid_limit", "limit must be between 1 and 100")
			return
		}
		filter.Limit = v
	}

	letters, err := h.svc.ListDeadLetters(r.Context(), filter)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, "list_failed", err.Error())
		return
	}

	type letterResponse struct {
		ID            string  `json:"id"`
		AggregateID   string  `json:"aggregate_id"`
		EventType     string  `json:"event_type"`
		ErrorMessage  string  `json:"error_message"`
		Attempts      int     `json:"attempts"`
		CreatedAt     string  `json:"created_at"`
		ResolvedAt    *string `json:"resolved_at,omitempty"`
		ResolvedBy    *string `json:"resolved_by,omitempty"`
	}
	out := make([]letterResponse, 0, len(letters))
	for _, l := range letters {
		r := letterResponse{
			ID:           l.ID.String(),
			AggregateID:  l.AggregateID.String(),
			EventType:    l.EventType,
			ErrorMessage: l.ErrorMessage,
			Attempts:     l.Attempts,
			CreatedAt:    l.CreatedAt.Format(time.RFC3339Nano),
		}
		if l.ResolvedAt != nil {
			s := l.ResolvedAt.Format(time.RFC3339Nano)
			r.ResolvedAt = &s
		}
		if l.ResolvedBy != "" {
			r.ResolvedBy = &l.ResolvedBy
		}
		out = append(out, r)
	}
	writeJSON(w, r, http.StatusOK, out)
}

func (h *DeadLetterHandler) Replay(w http.ResponseWriter, r *http.Request) {
	p, ok := middleware.PrincipalFromContext(r.Context())
	if !ok {
		writeError(w, r, http.StatusUnauthorized, "unauthorized", "missing authentication")
		return
	}
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "invalid_id", "id must be a valid UUID")
		return
	}

	var req struct {
		Reason string `json:"reason"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)

	newID, err := h.svc.ReplayDeadLetter(r.Context(), id, p.UserID, req.Reason)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, "replay_failed", err.Error())
		return
	}
	writeJSON(w, r, http.StatusOK, map[string]any{
		"new_event_id": newID.String(),
	})
}
