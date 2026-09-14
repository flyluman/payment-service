package refundreaper

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	domainrefund "github.com/crownroutes/payment-service/internal/domain/refund"
	"github.com/crownroutes/payment-service/internal/ports"
)

type fakeLister struct {
	ids []uuid.UUID
	err error
}

func (f *fakeLister) ListStaleRefunds(context.Context, time.Duration, int, int) ([]uuid.UUID, error) {
	return f.ids, f.err
}

type fakeRetrier struct {
	retried []uuid.UUID
	errs    map[uuid.UUID]error
}

func (f *fakeRetrier) RetryStaleRefund(_ context.Context, id uuid.UUID) (*domainrefund.Refund, error) {
	f.retried = append(f.retried, id)
	if err := f.errs[id]; err != nil {
		return nil, err
	}
	return &domainrefund.Refund{ID: id, Status: domainrefund.StatusRefunded}, nil
}

type fakeLogger struct{ errs, warns, infos int }

func (f *fakeLogger) Info(string, map[string]any)         { f.infos++ }
func (f *fakeLogger) Warn(string, map[string]any)         { f.warns++ }
func (f *fakeLogger) Error(string, map[string]any, error) { f.errs++ }
func (f *fakeLogger) Debug(string, map[string]any)        {}
func (f *fakeLogger) Trace(string, map[string]any)        {}
func (f *fakeLogger) With(map[string]any) ports.Logger    { return f }

func TestRunOnce_RetriesStaleRefunds(t *testing.T) {
	id1 := uuid.New()
	id2 := uuid.New()
	lister := &fakeLister{ids: []uuid.UUID{id1, id2}}
	retrier := &fakeRetrier{errs: map[uuid.UUID]error{}}
	log := &fakeLogger{}

	r := New(lister, retrier, log, Config{StaleAfter: time.Minute, MaxAttempts: 3, BatchLimit: 10})
	if err := r.RunOnce(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(retrier.retried) != 2 {
		t.Fatalf("expected 2 refunds retried, got %d", len(retrier.retried))
	}
}

func TestRunOnce_LogsFailuresAndContinues(t *testing.T) {
	id1 := uuid.New()
	id2 := uuid.New()
	lister := &fakeLister{ids: []uuid.UUID{id1, id2}}
	retrier := &fakeRetrier{errs: map[uuid.UUID]error{id2: errors.New("boom")}}
	log := &fakeLogger{}

	r := New(lister, retrier, log, Config{})
	if err := r.RunOnce(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(retrier.retried) != 2 {
		t.Errorf("expected both refunds attempted, got %d", len(retrier.retried))
	}
	if log.errs != 1 {
		t.Errorf("expected 1 error logged, got %d", log.errs)
	}
}

func TestRunOnce_ListErrorIsSwallowed(t *testing.T) {
	r := New(&fakeLister{err: errors.New("db down")}, &fakeRetrier{}, &fakeLogger{}, Config{})
	if err := r.RunOnce(context.Background()); err != nil {
		t.Fatalf("list errors should be logged, not returned: %v", err)
	}
}
