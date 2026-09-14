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

func processingExpiredTxn() *transaction.Txn {
	t, _ := transaction.New(uuid.New(), uuid.New(), 150000, "BDT", transaction.PaymentMethodCard, "stripe", uuid.New(), "b@example.com", "order", nil, 30)
	t.AttemptedGateway = "stripe"
	t.ActualGateway = "stripe"
	t.Status = transaction.StatusProcessing
	t.GatewayReferenceID = "pi_stuck"
	past := time.Now().UTC().Add(-time.Hour)
	d := time.Second
	t.ProcessingStartedAt = &past
	t.ProcessingTimeout = &d
	return t
}

func pendingTxn() *transaction.Txn {
	t, _ := transaction.New(uuid.New(), uuid.New(), 150000, "BDT", transaction.PaymentMethodCard, "stripe", uuid.New(), "b@example.com", "order", nil, 30)
	t.AttemptedGateway = "stripe"
	return t
}

func processService(repo *fakeRepo, lease *fakeLease, reg *fakeRegistry) *Service {
	return NewService(repo, &fakeOutbox{}, &fakeConfig{}, &fakeTransactor{}, lease, reg, noopLogger{}, noopMetrics{})
}

func seedRepo(txn *transaction.Txn) *fakeRepo {
	return &fakeRepo{store: map[uuid.UUID]*transaction.Txn{txn.ID: txn}}
}

type fakeCancelResolver struct {
	called bool
	gotID  uuid.UUID
	gotAmt int64
}

func (f *fakeCancelResolver) ResolveCancelRefund(_ context.Context, id uuid.UUID, amount int64) error {
	f.called = true
	f.gotID = id
	f.gotAmt = amount
	return nil
}

func TestProcessPayment_CancelResolutionHookFiresOnSucceededWithIntent(t *testing.T) {
	txn := pendingTxn()
	txn.CancelIntent = true
	reg := &fakeRegistry{adapter: &fakeAdapter{resp: &ports.GatewayPaymentResponse{Status: ports.GatewayPaymentStatusSucceeded}}}
	resolver := &fakeCancelResolver{}
	svc := NewService(seedRepo(txn), &fakeOutbox{}, &fakeConfig{}, &fakeTransactor{}, &fakeLease{acquired: true}, reg, noopLogger{}, noopMetrics{})
	svc.SetCancelResolver(resolver)

	got, err := svc.ProcessPayment(context.Background(), txn.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != transaction.StatusSucceeded {
		t.Fatalf("expected SUCCEEDED, got %s", got.Status)
	}
	if !resolver.called {
		t.Fatal("expected cancel-resolution hook to fire")
	}
	if resolver.gotID != txn.ID || resolver.gotAmt != txn.Amount {
		t.Errorf("hook called with wrong args: id=%s amt=%d", resolver.gotID, resolver.gotAmt)
	}
}

func TestProcessPayment_NoCancelResolutionWithoutIntent(t *testing.T) {
	txn := pendingTxn() // CancelIntent false
	reg := &fakeRegistry{adapter: &fakeAdapter{resp: &ports.GatewayPaymentResponse{Status: ports.GatewayPaymentStatusSucceeded}}}
	resolver := &fakeCancelResolver{}
	svc := NewService(seedRepo(txn), &fakeOutbox{}, &fakeConfig{}, &fakeTransactor{}, &fakeLease{acquired: true}, reg, noopLogger{}, noopMetrics{})
	svc.SetCancelResolver(resolver)

	if _, err := svc.ProcessPayment(context.Background(), txn.ID); err != nil {
		t.Fatal(err)
	}
	if resolver.called {
		t.Error("hook should not fire without cancel intent")
	}
}

type fakeBreaker struct {
	failures  int
	successes int
}

func (f *fakeBreaker) RecordFailure(context.Context, string) error { f.failures++; return nil }
func (f *fakeBreaker) RecordSuccess(context.Context, string) error { f.successes++; return nil }

func TestProcessPayment_RecordsBreakerFailureOnNetworkError(t *testing.T) {
	txn := pendingTxn()
	reg := &fakeRegistry{adapter: &fakeAdapter{err: &ports.GatewayError{Category: ports.ErrorCategoryNetworkTimeout}}}
	breaker := &fakeBreaker{}
	svc := NewService(seedRepo(txn), &fakeOutbox{}, &fakeConfig{}, &fakeTransactor{}, &fakeLease{acquired: true}, reg, noopLogger{}, noopMetrics{})
	svc.SetCircuitBreaker(breaker)

	if _, err := svc.ProcessPayment(context.Background(), txn.ID); err != nil {
		t.Fatal(err)
	}
	if breaker.failures != 1 || breaker.successes != 0 {
		t.Errorf("network error should record a breaker failure, got failures=%d successes=%d", breaker.failures, breaker.successes)
	}
}

func TestProcessPayment_RecordsBreakerSuccessOnDecline(t *testing.T) {
	txn := pendingTxn()
	reg := &fakeRegistry{adapter: &fakeAdapter{err: &ports.GatewayError{Category: ports.ErrorCategoryHardDecline}}}
	breaker := &fakeBreaker{}
	svc := NewService(seedRepo(txn), &fakeOutbox{}, &fakeConfig{}, &fakeTransactor{}, &fakeLease{acquired: true}, reg, noopLogger{}, noopMetrics{})
	svc.SetCircuitBreaker(breaker)

	if _, err := svc.ProcessPayment(context.Background(), txn.ID); err != nil {
		t.Fatal(err)
	}
	if breaker.successes != 1 || breaker.failures != 0 {
		t.Errorf("a decline means the gateway is healthy; expected breaker success, got failures=%d successes=%d", breaker.failures, breaker.successes)
	}
}

func TestProcessPayment_RecordsBreakerSuccessOnSuccess(t *testing.T) {
	txn := pendingTxn()
	reg := &fakeRegistry{adapter: &fakeAdapter{resp: &ports.GatewayPaymentResponse{Status: ports.GatewayPaymentStatusSucceeded}}}
	breaker := &fakeBreaker{}
	svc := NewService(seedRepo(txn), &fakeOutbox{}, &fakeConfig{}, &fakeTransactor{}, &fakeLease{acquired: true}, reg, noopLogger{}, noopMetrics{})
	svc.SetCircuitBreaker(breaker)

	if _, err := svc.ProcessPayment(context.Background(), txn.ID); err != nil {
		t.Fatal(err)
	}
	if breaker.successes != 1 || breaker.failures != 0 {
		t.Errorf("succeeded payment should record breaker success, got failures=%d successes=%d", breaker.failures, breaker.successes)
	}
}

func TestProcessPayment_GatewaySucceeded(t *testing.T) {
	txn := pendingTxn()
	repo := seedRepo(txn)
	outbox := &fakeOutbox{}
	reg := &fakeRegistry{adapter: &fakeAdapter{resp: &ports.GatewayPaymentResponse{GatewayReferenceID: "pi_1", Status: ports.GatewayPaymentStatusSucceeded}}}
	svc := NewService(repo, outbox, &fakeConfig{}, &fakeTransactor{}, &fakeLease{acquired: true}, reg, noopLogger{}, noopMetrics{})

	got, err := svc.ProcessPayment(context.Background(), txn.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Status != transaction.StatusSucceeded {
		t.Errorf("expected SUCCEEDED, got %s", got.Status)
	}
	if got.GatewayReferenceID != "pi_1" {
		t.Errorf("expected ref pi_1, got %s", got.GatewayReferenceID)
	}
	if len(outbox.events) != 1 || outbox.events[0].EventType != ports.EventTypeTransactionSucceeded {
		t.Errorf("expected PAYMENT_SUCCEEDED outbox event, got %+v", outbox.events)
	}
}

func TestProcessPayment_TerminalEventCarriesPostTransitionVersion(t *testing.T) {
	txn := pendingTxn()
	repo := seedRepo(txn)
	outbox := &fakeOutbox{}
	reg := &fakeRegistry{adapter: &fakeAdapter{resp: &ports.GatewayPaymentResponse{GatewayReferenceID: "pi_tv", Status: ports.GatewayPaymentStatusSucceeded}}}
	svc := NewService(repo, outbox, &fakeConfig{}, &fakeTransactor{}, &fakeLease{acquired: true}, reg, noopLogger{}, noopMetrics{})

	if _, err := svc.ProcessPayment(context.Background(), txn.ID); err != nil {
		t.Fatal(err)
	}
	if len(outbox.events) != 1 {
		t.Fatalf("expected 1 outbox event, got %d", len(outbox.events))
	}
	ev := outbox.events[0]
	reloaded, err := repo.GetByID(context.Background(), txn.ID)
	if err != nil {
		t.Fatalf("reload txn: %v", err)
	}
	if ev.AggregateVersion != reloaded.Version {
		t.Errorf("event aggregate version %d should match post-transition txn version %d", ev.AggregateVersion, reloaded.Version)
	}
}

func TestProcessPayment_GatewayHardDecline(t *testing.T) {
	txn := pendingTxn()
	repo := seedRepo(txn)
	outbox := &fakeOutbox{}
	reg := &fakeRegistry{adapter: &fakeAdapter{err: &ports.GatewayError{Category: ports.ErrorCategoryHardDecline, Code: "do_not_honor", GatewayCode: "declined"}}}
	svc := NewService(repo, outbox, &fakeConfig{}, &fakeTransactor{}, &fakeLease{acquired: true}, reg, noopLogger{}, noopMetrics{})

	got, err := svc.ProcessPayment(context.Background(), txn.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Status != transaction.StatusFailed {
		t.Errorf("expected FAILED on hard decline, got %s", got.Status)
	}
	if got.FailureReason == nil || got.FailureReason.Code != "do_not_honor" {
		t.Errorf("expected failure reason with code do_not_honor, got %+v", got.FailureReason)
	}
	if len(outbox.events) != 1 || outbox.events[0].EventType != ports.EventTypeTransactionFailed {
		t.Errorf("expected PAYMENT_FAILED event, got %+v", outbox.events)
	}
}

func TestProcessPayment_NetworkTimeoutStaysProcessing(t *testing.T) {
	txn := pendingTxn()
	repo := seedRepo(txn)
	outbox := &fakeOutbox{}
	reg := &fakeRegistry{adapter: &fakeAdapter{err: &ports.GatewayError{Category: ports.ErrorCategoryNetworkTimeout}}}
	svc := NewService(repo, outbox, &fakeConfig{}, &fakeTransactor{}, &fakeLease{acquired: true}, reg, noopLogger{}, noopMetrics{})

	got, err := svc.ProcessPayment(context.Background(), txn.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Status != transaction.StatusProcessing {
		t.Errorf("expected PROCESSING on network timeout, got %s", got.Status)
	}
	if len(outbox.events) != 0 {
		t.Errorf("no outbox event should be emitted for non-terminal outcome, got %d", len(outbox.events))
	}
}

func TestProcessPayment_LeaseNotAcquired(t *testing.T) {
	txn := pendingTxn()
	repo := seedRepo(txn)
	svc := processService(repo, &fakeLease{acquired: false}, &fakeRegistry{adapter: &fakeAdapter{}})

	got, err := svc.ProcessPayment(context.Background(), txn.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Status != transaction.StatusPending {
		t.Errorf("a non-acquired lease should leave the txn PENDING, got %s", got.Status)
	}
	if repo.updates != 0 {
		t.Errorf("no repo writes expected when lease not acquired, got %d updates", repo.updates)
	}
}

func TestProcessPayment_NonPendingIsNoop(t *testing.T) {
	txn := pendingTxn()
	txn.Status = transaction.StatusProcessing
	repo := seedRepo(txn)
	svc := processService(repo, &fakeLease{}, &fakeRegistry{adapter: &fakeAdapter{}})

	got, err := svc.ProcessPayment(context.Background(), txn.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Status != transaction.StatusProcessing {
		t.Errorf("a non-PENDING txn should be returned as-is, got %s", got.Status)
	}
}

func TestProcessPayment_UnknownGateway(t *testing.T) {
	txn := pendingTxn()
	repo := seedRepo(txn)
	svc := processService(repo, &fakeLease{acquired: true}, &fakeRegistry{err: errors.New("unknown gateway")})

	_, err := svc.ProcessPayment(context.Background(), txn.ID)
	if err == nil {
		t.Fatal("expected error for unknown gateway")
	}
}

func recoverService(repo *fakeRepo, reg *fakeRegistry, outbox *fakeOutbox) *Service {
	return NewService(repo, outbox, &fakeConfig{}, &fakeTransactor{}, &fakeLease{acquired: true}, reg, noopLogger{}, noopMetrics{})
}

func TestRecoverExpiredLease_FinalizesSucceeded(t *testing.T) {
	txn := processingExpiredTxn()
	repo := seedRepo(txn)
	outbox := &fakeOutbox{}
	reg := &fakeRegistry{adapter: &fakeAdapter{resp: &ports.GatewayPaymentResponse{
		GatewayReferenceID: "pi_stuck", Status: ports.GatewayPaymentStatusSucceeded,
	}}}
	svc := recoverService(repo, reg, outbox)

	got, err := svc.RecoverExpiredLease(context.Background(), txn.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Status != transaction.StatusSucceeded {
		t.Errorf("status check found the payment SUCCEEDED; expected the stuck txn finalized, got %s", got.Status)
	}
	if len(outbox.events) != 1 || outbox.events[0].EventType != ports.EventTypeTransactionSucceeded {
		t.Errorf("expected one PAYMENT_SUCCEEDED event on recovery, got %+v", outbox.events)
	}
}

func TestRecoverExpiredLease_FinalizesFailed(t *testing.T) {
	txn := processingExpiredTxn()
	outbox := &fakeOutbox{}
	reg := &fakeRegistry{adapter: &fakeAdapter{resp: &ports.GatewayPaymentResponse{
		GatewayReferenceID: "pi_stuck", Status: ports.GatewayPaymentStatusFailed, ErrorCode: "declined",
	}}}
	svc := recoverService(seedRepo(txn), reg, outbox)

	got, err := svc.RecoverExpiredLease(context.Background(), txn.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Status != transaction.StatusFailed {
		t.Errorf("expected FAILED, got %s", got.Status)
	}
	if len(outbox.events) != 1 || outbox.events[0].EventType != ports.EventTypeTransactionFailed {
		t.Errorf("expected one PAYMENT_FAILED event, got %+v", outbox.events)
	}
}

func TestRecoverExpiredLease_StillPendingAtGatewayStaysProcessing(t *testing.T) {
	txn := processingExpiredTxn()
	repo := seedRepo(txn)
	outbox := &fakeOutbox{}
	reg := &fakeRegistry{adapter: &fakeAdapter{resp: &ports.GatewayPaymentResponse{
		GatewayReferenceID: "pi_stuck", Status: ports.GatewayPaymentStatusProcessing,
	}}}
	svc := recoverService(repo, reg, outbox)

	got, err := svc.RecoverExpiredLease(context.Background(), txn.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Status != transaction.StatusProcessing {
		t.Errorf("an indeterminate gateway status must leave the txn PROCESSING for the next sweep, got %s", got.Status)
	}
	if repo.updates != 0 || len(outbox.events) != 0 {
		t.Errorf("no write should happen while the outcome is unknown, updates=%d events=%d", repo.updates, len(outbox.events))
	}
}

func TestRecoverExpiredLease_NetworkErrorStaysProcessing(t *testing.T) {
	txn := processingExpiredTxn()
	repo := seedRepo(txn)
	reg := &fakeRegistry{adapter: &fakeAdapter{err: &ports.GatewayError{Category: ports.ErrorCategoryNetworkTimeout}}}
	svc := recoverService(repo, reg, &fakeOutbox{})

	got, err := svc.RecoverExpiredLease(context.Background(), txn.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Status != transaction.StatusProcessing || repo.updates != 0 {
		t.Errorf("a status-check network error must not finalize, got status=%s updates=%d", got.Status, repo.updates)
	}
}

func TestRecoverExpiredLease_LeaseNotExpiredIsNoop(t *testing.T) {
	txn := processingExpiredTxn()
	future := time.Now().UTC().Add(time.Hour)
	txn.ProcessingStartedAt = &future // lease still valid
	repo := seedRepo(txn)
	svc := recoverService(repo, &fakeRegistry{adapter: &fakeAdapter{}}, &fakeOutbox{})

	got, err := svc.RecoverExpiredLease(context.Background(), txn.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Status != transaction.StatusProcessing || repo.updates != 0 {
		t.Errorf("a still-valid lease must be left alone, got status=%s updates=%d", got.Status, repo.updates)
	}
}

func TestRecoverExpiredLease_NonProcessingIsNoop(t *testing.T) {
	txn := pendingTxn() // PENDING, not stuck
	repo := seedRepo(txn)
	svc := recoverService(repo, &fakeRegistry{adapter: &fakeAdapter{}}, &fakeOutbox{})

	if _, err := svc.RecoverExpiredLease(context.Background(), txn.ID); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if repo.updates != 0 {
		t.Errorf("a non-PROCESSING transaction needs no recovery, updates=%d", repo.updates)
	}
}
