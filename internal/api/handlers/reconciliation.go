package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/api/middleware"
	"github.com/crownroutes/payment-service/internal/domain/reconciliation"
	"github.com/crownroutes/payment-service/internal/ports"
)

type ReconciliationService interface {
	CreateJob(ctx context.Context, gatewayID string, periodStart, periodEnd *time.Time, transactionID *uuid.UUID, triggeredBy string) (*reconciliation.Job, error)
	RunJob(ctx context.Context, jobID uuid.UUID) error
	GetJob(ctx context.Context, id uuid.UUID) (*reconciliation.Job, error)
	ListJobs(ctx context.Context, filter ports.ReconciliationJobFilter) ([]*reconciliation.Job, error)
	GetEntries(ctx context.Context, jobID uuid.UUID, filter ports.ReconciliationEntryFilter) ([]*reconciliation.Entry, error)
	ResolveEntry(ctx context.Context, entryID uuid.UUID, resolvedBy, notes string) error
}

type ReconciliationHandler struct {
	svc ReconciliationService
}

func NewReconciliationHandler(svc ReconciliationService) *ReconciliationHandler {
	return &ReconciliationHandler{svc: svc}
}

type createJobRequest struct {
	GatewayID     string  `json:"gateway_id"`
	PeriodStart   *string `json:"period_start,omitempty"`
	PeriodEnd     *string `json:"period_end,omitempty"`
	TransactionID *string `json:"transaction_id,omitempty"`
}

type resolveEntryRequest struct {
	Notes string `json:"notes"`
}

func (h *ReconciliationHandler) CreateJob(w http.ResponseWriter, r *http.Request) {
	var req createJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, r, http.StatusBadRequest, "invalid_body", "invalid request body")
		return
	}
	if req.GatewayID == "" {
		writeError(w, r, http.StatusBadRequest, "missing_gateway_id", "gateway_id is required")
		return
	}

	actor := "ops"
	if p, ok := middleware.PrincipalFromContext(r.Context()); ok {
		if p.Role == middleware.RoleOps {
			actor = "ops"
		} else {
			actor = "service:" + p.TenantID
		}
	}

	var periodStart, periodEnd *time.Time
	var txnID *uuid.UUID

	if req.TransactionID != nil {
		id, err := uuid.Parse(*req.TransactionID)
		if err != nil {
			writeError(w, r, http.StatusBadRequest, "invalid_transaction_id", "invalid transaction_id")
			return
		}
		txnID = &id
	} else {
		if req.PeriodStart == nil || req.PeriodEnd == nil {
			writeError(w, r, http.StatusBadRequest, "missing_period", "period_start and period_end are required for batch jobs")
			return
		}
		ps, err := time.Parse(time.RFC3339, *req.PeriodStart)
		if err != nil {
			writeError(w, r, http.StatusBadRequest, "invalid_period_start", "invalid period_start format")
			return
		}
		pe, err := time.Parse(time.RFC3339, *req.PeriodEnd)
		if err != nil {
			writeError(w, r, http.StatusBadRequest, "invalid_period_end", "invalid period_end format")
			return
		}
		periodStart = &ps
		periodEnd = &pe
	}

	job, err := h.svc.CreateJob(r.Context(), req.GatewayID, periodStart, periodEnd, txnID, actor)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, "create_job_failed", "could not create reconciliation job")
		return
	}

	writeJSON(w, r, http.StatusCreated, job)
}

func (h *ReconciliationHandler) RunJob(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "invalid_id", "invalid job id")
		return
	}

	if err := h.svc.RunJob(r.Context(), id); err != nil {
		writeError(w, r, http.StatusInternalServerError, "run_job_failed", "could not run reconciliation job")
		return
	}

	writeJSON(w, r, http.StatusOK, map[string]string{"status": "completed"})
}

func (h *ReconciliationHandler) GetJob(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "invalid_id", "invalid job id")
		return
	}

	job, err := h.svc.GetJob(r.Context(), id)
	if err != nil {
		writeError(w, r, http.StatusNotFound, "not_found", "reconciliation job not found")
		return
	}

	writeJSON(w, r, http.StatusOK, job)
}

func (h *ReconciliationHandler) ListJobs(w http.ResponseWriter, r *http.Request) {
	filter := ports.ReconciliationJobFilter{Limit: 50}

	if gid := r.URL.Query().Get("gateway_id"); gid != "" {
		filter.GatewayID = &gid
	}
	if status := r.URL.Query().Get("status"); status != "" {
		filter.Status = &status
	}

	jobs, err := h.svc.ListJobs(r.Context(), filter)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, "list_jobs_failed", "could not list reconciliation jobs")
		return
	}

	writeJSON(w, r, http.StatusOK, jobs)
}

func (h *ReconciliationHandler) GetEntries(w http.ResponseWriter, r *http.Request) {
	jobID, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "invalid_id", "invalid job id")
		return
	}

	filter := ports.ReconciliationEntryFilter{Limit: 100}
	if mt := r.URL.Query().Get("mismatch_type"); mt != "" {
		filter.MismatchType = &mt
	}
	if rs := r.URL.Query().Get("resolution_status"); rs != "" {
		filter.ResolutionStatus = &rs
	}

	entries, err := h.svc.GetEntries(r.Context(), jobID, filter)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, "list_entries_failed", "could not list reconciliation entries")
		return
	}

	writeJSON(w, r, http.StatusOK, entries)
}

func (h *ReconciliationHandler) ResolveEntry(w http.ResponseWriter, r *http.Request) {
	entryID, err := uuid.Parse(r.PathValue("entry_id"))
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "invalid_entry_id", "invalid entry id")
		return
	}

	var req resolveEntryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, r, http.StatusBadRequest, "invalid_body", "invalid request body")
		return
	}

	actor := "ops"
	if p, ok := middleware.PrincipalFromContext(r.Context()); ok {
		if p.Role == middleware.RoleOps {
			actor = "ops"
		} else {
			actor = "service:" + p.TenantID
		}
	}

	if err := h.svc.ResolveEntry(r.Context(), entryID, actor, req.Notes); err != nil {
		writeError(w, r, http.StatusInternalServerError, "resolve_failed", "could not resolve entry")
		return
	}

	writeJSON(w, r, http.StatusOK, map[string]string{"status": "resolved"})
}
