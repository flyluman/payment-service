package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/domain/reconciliation"
	"github.com/crownroutes/payment-service/internal/ports"
)

// --- fake service ---

type fakeReconSvc struct {
	jobs    []*reconciliation.Job
	entries []*reconciliation.Entry
}

func (f *fakeReconSvc) CreateJob(_ context.Context, _ string, _ *uuid.UUID, _, _ *time.Time, _ *uuid.UUID, _ string) (*reconciliation.Job, error) {
	job := &reconciliation.Job{
		ID:        uuid.New(),
		Status:    reconciliation.JobStatusPending,
		CreatedAt: time.Now().UTC(),
	}
	f.jobs = append(f.jobs, job)
	return job, nil
}

func (f *fakeReconSvc) RunJob(_ context.Context, _ string, _ uuid.UUID) error { return nil }

func (f *fakeReconSvc) GetJob(_ context.Context, _ string, id uuid.UUID) (*reconciliation.Job, error) {
	for _, j := range f.jobs {
		if j.ID == id {
			return j, nil
		}
	}
	return &reconciliation.Job{ID: id, Status: reconciliation.JobStatusPending}, nil
}

func (f *fakeReconSvc) ListJobs(_ context.Context, _ ports.ReconciliationJobFilter) ([]*reconciliation.Job, error) {
	return f.jobs, nil
}

func (f *fakeReconSvc) GetEntries(_ context.Context, _ string, _ uuid.UUID, _ ports.ReconciliationEntryFilter) ([]*reconciliation.Entry, error) {
	return f.entries, nil
}

func (f *fakeReconSvc) ResolveEntry(_ context.Context, _ string, _ uuid.UUID, _, _ string) error {
	return nil
}

// --- tests ---

func TestCreateJob_Success(t *testing.T) {
	h := NewReconciliationHandler(&fakeReconSvc{})
	body := `{"gateway_id": "stripe", "tenant_id": "` + uuid.NewString() + `", "period_start": "2026-01-01T00:00:00Z", "period_end": "2026-01-31T23:59:59Z"}`
	req := httptest.NewRequest("POST", "/api/v1/reconciliation/jobs", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	h.CreateJob(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("status: got %d, want 201", w.Code)
	}
	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	data := resp["data"].(map[string]any)
	if data["Status"] != "PENDING" {
		t.Errorf("status: got %v, want PENDING", data["Status"])
	}
}

func TestCreateJob_BatchWithoutTenantID(t *testing.T) {
	h := NewReconciliationHandler(&fakeReconSvc{})
	body := `{"gateway_id": "stripe", "period_start": "2026-01-01T00:00:00Z", "period_end": "2026-01-31T23:59:59Z"}`
	req := httptest.NewRequest("POST", "/api/v1/reconciliation/jobs", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	h.CreateJob(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400 (batch jobs require tenant_id)", w.Code)
	}
}

func TestCreateJob_MissingGatewayID(t *testing.T) {
	h := NewReconciliationHandler(&fakeReconSvc{})
	body := `{}`
	req := httptest.NewRequest("POST", "/api/v1/reconciliation/jobs", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	h.CreateJob(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400", w.Code)
	}
}

func TestCreateJob_MissingPeriod(t *testing.T) {
	h := NewReconciliationHandler(&fakeReconSvc{})
	body := `{"gateway_id": "stripe"}`
	req := httptest.NewRequest("POST", "/api/v1/reconciliation/jobs", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	h.CreateJob(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400", w.Code)
	}
}

func TestGetJob_Success(t *testing.T) {
	svc := &fakeReconSvc{}
	job := &reconciliation.Job{ID: uuid.New(), Status: reconciliation.JobStatusPending}
	svc.jobs = append(svc.jobs, job)

	h := NewReconciliationHandler(svc)
	req := httptest.NewRequest("GET", "/api/v1/reconciliation/jobs/"+job.ID.String(), nil)
	req.SetPathValue("id", job.ID.String())
	w := httptest.NewRecorder()

	h.GetJob(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status: got %d, want 200", w.Code)
	}
}

func TestListJobs_Success(t *testing.T) {
	svc := &fakeReconSvc{
		jobs: []*reconciliation.Job{
			{ID: uuid.New(), Status: reconciliation.JobStatusPending},
			{ID: uuid.New(), Status: reconciliation.JobStatusCompleted},
		},
	}
	h := NewReconciliationHandler(svc)
	req := httptest.NewRequest("GET", "/api/v1/reconciliation/jobs", nil)
	w := httptest.NewRecorder()

	h.ListJobs(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status: got %d, want 200", w.Code)
	}
	var resp map[string]any
	json.NewDecoder(w.Body).Decode(&resp)
	data := resp["data"].([]any)
	if len(data) != 2 {
		t.Errorf("count: got %d, want 2", len(data))
	}
}

func TestRunJob_Success(t *testing.T) {
	h := NewReconciliationHandler(&fakeReconSvc{})
	jobID := uuid.New()
	req := httptest.NewRequest("POST", "/api/v1/reconciliation/jobs/"+jobID.String()+"/run", nil)
	req.SetPathValue("id", jobID.String())
	w := httptest.NewRecorder()

	h.RunJob(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status: got %d, want 200", w.Code)
	}
}

func TestResolveEntry_Success(t *testing.T) {
	h := NewReconciliationHandler(&fakeReconSvc{})
	jobID := uuid.New()
	entryID := uuid.New()
	body := `{"notes": "looks good"}`
	req := httptest.NewRequest("POST", "/api/v1/reconciliation/jobs/"+jobID.String()+"/entries/"+entryID.String()+"/resolve", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	req.SetPathValue("id", jobID.String())
	req.SetPathValue("entry_id", entryID.String())
	w := httptest.NewRecorder()

	h.ResolveEntry(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status: got %d, want 200", w.Code)
	}
}

func TestResolveEntry_InvalidEntryID(t *testing.T) {
	h := NewReconciliationHandler(&fakeReconSvc{})
	jobID := uuid.New()
	req := httptest.NewRequest("POST", "/api/v1/reconciliation/jobs/"+jobID.String()+"/entries/not-a-uuid/resolve", bytes.NewBufferString(`{"notes":"ok"}`))
	req.Header.Set("Content-Type", "application/json")
	req.SetPathValue("id", jobID.String())
	req.SetPathValue("entry_id", "not-a-uuid")
	w := httptest.NewRecorder()

	h.ResolveEntry(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status: got %d, want 400", w.Code)
	}
}
