package payment

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/domain/transaction"
	"github.com/crownroutes/payment-service/internal/ports"
)

type fakeRepo struct {
	inserted []*transaction.Txn
	store    map[uuid.UUID]*transaction.Txn
	updates  int
	failOn   bool
}

func (r *fakeRepo) Insert(ctx context.Context, t *transaction.Txn) error {
	if r.failOn {
		return errors.New("insert failed")
	}
	r.inserted = append(r.inserted, t)
	if r.store == nil {
		r.store = map[uuid.UUID]*transaction.Txn{}
	}
	r.store[t.ID] = t
	return nil
}

func (r *fakeRepo) GetByID(ctx context.Context, id uuid.UUID) (*transaction.Txn, error) {
	t, ok := r.store[id]
	if !ok {
		return nil, errors.New("not found")
	}
	clone := *t
	return &clone, nil
}

func (r *fakeRepo) UpdateStatus(ctx context.Context, t *transaction.Txn) error {
	r.updates++
	t.Version++
	if r.store == nil {
		r.store = map[uuid.UUID]*transaction.Txn{}
	}
	r.store[t.ID] = t
	return nil
}

func (r *fakeRepo) UpdateGatewayReference(ctx context.Context, id uuid.UUID, gatewayRefID string, expectedVersion int) error {
	t, ok := r.store[id]
	if !ok {
		return errors.New("not found")
	}
	t.GatewayReferenceID = gatewayRefID
	return nil
}

func (r *fakeRepo) List(ctx context.Context, filter ports.TransactionFilter) (*ports.TransactionListResult, error) {
	return &ports.TransactionListResult{}, nil
}

type fakeLease struct {
	acquired bool
	cached   []byte
	written  []byte
}

func (l *fakeLease) Acquire(ctx context.Context, leaseKey, transactionID uuid.UUID, ttlSec int) (bool, []byte, error) {
	return l.acquired, l.cached, nil
}

func (l *fakeLease) WriteCachedResponse(ctx context.Context, leaseKey uuid.UUID, response []byte) error {
	l.written = response
	return nil
}

func (l *fakeLease) TryAcquireDirect(ctx context.Context, leaseKey, transactionID uuid.UUID, ttlSec int) (bool, error) {
	return l.acquired, nil
}

type fakeAdapter struct {
	resp *ports.GatewayPaymentResponse
	err  error
}

func (a *fakeAdapter) InitiatePayment(ctx context.Context, req ports.GatewayPaymentRequest) (*ports.GatewayPaymentResponse, error) {
	return a.resp, a.err
}
func (a *fakeAdapter) CheckStatus(ctx context.Context, req ports.GatewayStatusRequest) (*ports.GatewayPaymentResponse, error) {
	return a.resp, a.err
}
func (a *fakeAdapter) Refund(ctx context.Context, req ports.GatewayRefundRequest) (*ports.GatewayRefundResponse, error) {
	return nil, nil
}
func (a *fakeAdapter) Cancel(ctx context.Context, req ports.GatewayCancelRequest) (*ports.GatewayCancelResponse, error) {
	return nil, nil
}
func (a *fakeAdapter) Capabilities() ports.GatewayCapabilities { return ports.GatewayCapabilities{} }

type fakeRegistry struct {
	adapter ports.GatewayAdapter
	err     error
}

func (r *fakeRegistry) Get(gatewayID string) (ports.GatewayAdapter, error) {
	return r.adapter, r.err
}

type fakeOutbox struct {
	events []ports.OutboxEvent
	failOn bool
}

func (o *fakeOutbox) Write(ctx context.Context, event ports.OutboxEvent) error {
	if o.failOn {
		return errors.New("outbox write failed")
	}
	o.events = append(o.events, event)
	return nil
}

type fakeConfig struct {
	timeout time.Duration
	err     error
}

func (c *fakeConfig) GetGatewayConfig(ctx context.Context, gatewayID string) (*ports.GatewayConfig, error) {
	return &ports.GatewayConfig{GatewayID: gatewayID, IsActive: true}, c.err
}

func (c *fakeConfig) GetProcessingTimeout(ctx context.Context, gatewayID, paymentMethod string) (time.Duration, error) {
	return c.timeout, c.err
}

func (c *fakeConfig) GetFeeModel(ctx context.Context, gatewayID, paymentMethod string) (*ports.GatewayFeeModel, error) {
	return nil, nil
}

type fakeTransactor struct{ committed bool }

func (t *fakeTransactor) WithinTx(ctx context.Context, fn func(ctx context.Context) error) error {
	if err := fn(ctx); err != nil {
		return err
	}
	t.committed = true
	return nil
}

type noopLogger struct{}

func (noopLogger) Info(string, map[string]any)         {}
func (noopLogger) Warn(string, map[string]any)         {}
func (noopLogger) Error(string, map[string]any, error) {}
func (noopLogger) Debug(string, map[string]any)        {}
func (noopLogger) Trace(string, map[string]any)        {}
func (l noopLogger) With(map[string]any) ports.Logger  { return l }

type noopMetrics struct{}

func (noopMetrics) Increment(string, map[string]string)          {}
func (noopMetrics) Histogram(string, float64, map[string]string) {}
func (noopMetrics) Gauge(string, float64, map[string]string)     {}

func newTestService(repo *fakeRepo, outbox *fakeOutbox, config *fakeConfig, tx *fakeTransactor, reg *fakeRegistry) *Service {
	if reg == nil {
		reg = &fakeRegistry{adapter: &fakeAdapter{
			resp: &ports.GatewayPaymentResponse{GatewayReferenceID: "ref_123"},
		}}
	}
	return NewService(repo, outbox, config, tx, &fakeLease{acquired: true}, reg, noopLogger{}, noopMetrics{})
}

func validInput() CreateInput {
	return CreateInput{
		TenantID:      uuid.New(),
		UserID:        uuid.New(),
		GatewayID:     "razorpay",
		Amount:        150000,
		Currency:      "BDT",
		PaymentMethod: transaction.PaymentMethodCard,
		CustomerID:    uuid.New(),
		CustomerEmail: "buyer@example.com",
		Description:   "order #42",
		CallbackURL:   "https://example.com/callback",
		RedirectURL:   "https://example.com/redirect",
	}
}

func TestCreate_Success(t *testing.T) {
	repo := &fakeRepo{}
	outbox := &fakeOutbox{}
	config := &fakeConfig{timeout: 30 * time.Second}
	tx := &fakeTransactor{}
	svc := newTestService(repo, outbox, config, tx, nil)

	res, err := svc.Create(context.Background(), validInput())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	txn := res.Transaction

	if txn.Status != transaction.StatusProcessing {
		t.Errorf("expected status PROCESSING, got %s", txn.Status)
	}
	if txn.GatewayID != "razorpay" || txn.AttemptedGateway != "razorpay" {
		t.Errorf("expected gateway razorpay, got gateway=%q attempted=%q", txn.GatewayID, txn.AttemptedGateway)
	}
	if txn.EstimatedTimeoutSeconds != 30 {
		t.Errorf("expected timeout 30s, got %d", txn.EstimatedTimeoutSeconds)
	}
	if !tx.committed {
		t.Error("expected transaction to be committed")
	}
	if len(repo.inserted) != 1 {
		t.Fatalf("expected 1 inserted transaction, got %d", len(repo.inserted))
	}
	if len(outbox.events) != 2 {
		t.Fatalf("expected 2 outbox events, got %d", len(outbox.events))
	}
	ev := outbox.events[0]
	if ev.EventType != ports.EventTypeTransactionCreated {
		t.Errorf("expected event %s, got %s", ports.EventTypeTransactionCreated, ev.EventType)
	}
	if ev.AggregateID != txn.ID {
		t.Errorf("expected event aggregate %s, got %s", txn.ID, ev.AggregateID)
	}
	ev2 := outbox.events[1]
	if ev2.EventType != ports.EventTypeGatewayInitiate {
		t.Errorf("expected event %s, got %s", ports.EventTypeGatewayInitiate, ev2.EventType)
	}
}

func TestCreate_NoCandidate(t *testing.T) {
	repo := &fakeRepo{}
	outbox := &fakeOutbox{}
	config := &fakeConfig{timeout: 30 * time.Second}
	tx := &fakeTransactor{}
	svc := newTestService(repo, outbox, config, tx, nil)

	in := validInput()
	in.GatewayID = ""
	_, err := svc.Create(context.Background(), in)
	if !errors.Is(err, ErrNoGateway) {
		t.Fatalf("expected ErrNoGateway, got %v", err)
	}
	if len(repo.inserted) != 0 || len(outbox.events) != 0 {
		t.Error("expected no writes when no gateway specified")
	}
}

func TestCreate_OutboxFailureRollsBack(t *testing.T) {
	repo := &fakeRepo{}
	outbox := &fakeOutbox{failOn: true}
	config := &fakeConfig{timeout: 30 * time.Second}
	tx := &fakeTransactor{}
	svc := newTestService(repo, outbox, config, tx, nil)

	_, err := svc.Create(context.Background(), validInput())
	if err == nil {
		t.Fatal("expected error when outbox write fails")
	}
	if tx.committed {
		t.Error("expected transaction not to commit when outbox write fails")
	}
}

func TestCreate_InvalidTimeout(t *testing.T) {
	repo := &fakeRepo{}
	outbox := &fakeOutbox{}
	config := &fakeConfig{timeout: 0}
	tx := &fakeTransactor{}
	svc := newTestService(repo, outbox, config, tx, nil)

	if _, err := svc.Create(context.Background(), validInput()); err == nil {
		t.Fatal("expected error for non-positive processing timeout")
	}
}
