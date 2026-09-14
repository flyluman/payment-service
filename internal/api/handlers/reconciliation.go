package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/api/middleware"
	reconapp "github.com/crownroutes/payment-service/internal/app/reconciliation"
	"github.com/crownroutes/payment-service/internal/domain/reconciliation"
	"github.com/crownroutes/payment-service/internal/ports"
)

type ReconciliationService interface {
	CreateJob(ctx context.Context, gatewayID string, tenantID *uuid.UUID, periodStart, periodEnd *time.Time, transactionID *uuid.UUID, triggeredBy string) (*reconciliation.Job, error)
	RunJob(ctx context.Context, actor string, jobID uuid.UUID) error
	GetJob(ctx context.Context, actor string, id uuid.UUID) (*reconciliation.Job, error)
	ListJobs(ctx context.Context, filter ports.ReconciliationJobFilter) ([]*reconciliation.Job, error)
	GetEntries(ctx context.Context, actor string, jobID uuid.UUID, filter ports.ReconciliationEntryFilter) ([]*reconciliation.Entry, error)
	ResolveEntry(ctx context.Context, actor string, entryID uuid.UUID, resolvedBy, notes string) error
}

type ReconciliationHandler struct {
	svc ReconciliationService
}

func NewReconciliationHandler(svc ReconciliationService) *ReconciliationHandler {
	return &ReconciliationHandler{svc: svc}
}

type createJobRequest struct {
	GatewayID     string  `json:"gateway_id"`
	TenantID      *string `json:"tenant_id,omitempty"`
	PeriodStart   *string `json:"period_start,omitempty"`
	PeriodEnd     *string `json:"period_end,omitempty"`
	TransactionID *string `json:"transaction_id,omitempty"`
}

type resolveEntryRequest struct {
	Notes string `json:"notes"`
}

func reconActor(r *http.Request) string {
	p, ok := middleware.PrincipalFromContext(r.Context())
	if !ok {
		return "ops"
	}
	if p.Role == middleware.RoleOps {
		return "ops"
	}
	return "service:" + p.TenantID
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

	actor := reconActor(r)

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

	tenantID, err := reconcileTenantID(r, req.TenantID)
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "missing_tenant_id", "tenant_id is required for reconciliation jobs")
		return
	}

	job, err := h.svc.CreateJob(r.Context(), req.GatewayID, tenantID, periodStart, periodEnd, txnID, actor)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, "create_job_failed", "could not create reconciliation job")
		return
	}

	writeJSON(w, r, http.StatusCreated, job)
}

// reconcileTenantID resolves the tenant a reconciliation job belongs to. Service
// tokens always scope to their own tenant; ops tokens must supply tenant_id
// explicitly (batch jobs are never global).
func reconcileTenantID(r *http.Request, requested *string) (*uuid.UUID, error) {
	p, ok := middleware.PrincipalFromContext(r.Context())
	if !ok || p.Role == middleware.RoleOps {
		if requested == nil {
			return nil, errors.New("tenant_id required")
		}
		id, err := uuid.Parse(*requested)
		if err != nil {
			return nil, err
		}
		return &id, nil
	}
	id, err := uuid.Parse(p.TenantID)
	if err != nil {
		return nil, err
	}
	return &id, nil
}

func (h *ReconciliationHandler) RunJob(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "invalid_id", "invalid job id")
		return
	}

	if err := h.svc.RunJob(r.Context(), reconActor(r), id); err != nil {
		if errors.Is(err, reconapp.ErrNotFound) {
			writeError(w, r, http.StatusNotFound, "not_found", "reconciliation job not found")
			return
		}
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

	job, err := h.svc.GetJob(r.Context(), reconActor(r), id)
	if err != nil {
		writeError(w, r, http.StatusNotFound, "not_found", "reconciliation job not found")
		return
	}

	writeJSON(w, r, http.StatusOK, job)
}

func (h *ReconciliationHandler) ListJobs(w http.ResponseWriter, r *http.Request) {
	filter := ports.ReconciliationJobFilter{Limit: 50}

	if a := reconActor(r); a != "ops" {
		filter.Actor = &a
	}
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

	entries, err := h.svc.GetEntries(r.Context(), reconActor(r), jobID, filter)
	if err != nil {
		if errors.Is(err, reconapp.ErrNotFound) {
			writeError(w, r, http.StatusNotFound, "not_found", "reconciliation job not found")
			return
		}
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

	actor := reconActor(r)

	if err := h.svc.ResolveEntry(r.Context(), actor, entryID, actor, req.Notes); err != nil {
		if errors.Is(err, reconapp.ErrNotFound) {
			writeError(w, r, http.StatusNotFound, "not_found", "reconciliation entry not found")
			return
		}
		writeError(w, r, http.StatusInternalServerError, "resolve_failed", "could not resolve entry")
		return
	}

	writeJSON(w, r, http.StatusOK, map[string]string{"status": "resolved"})
}
