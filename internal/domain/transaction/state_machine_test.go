package transaction

import (
	"errors"
	"testing"
	"time"
)

func TestTransitionState_ValidTransitions(t *testing.T) {
	cases := []struct {
		from Status
		to   Status
	}{
		{StatusPending, StatusProcessing},
		{StatusPending, StatusAuthorized},
		{StatusPending, StatusCaptured},
		{StatusPending, StatusCancelled},
		{StatusPending, StatusFailed},
		{StatusProcessing, StatusCaptured},
		{StatusProcessing, StatusAuthorized},
		{StatusProcessing, StatusFailed},
		{StatusAuthorized, StatusCaptured},
		{StatusAuthorized, StatusFailed},
		{StatusAuthorized, StatusCancelled},
		{StatusCaptured, StatusSettled},
		{StatusCaptured, StatusRefundPending},
		{StatusCaptured, StatusRefunded},
		{StatusCaptured, StatusRefundFailed},
		{StatusCaptured, StatusDisputed},
		{StatusSettled, StatusRefundPending},
		{StatusSettled, StatusRefunded},
		{StatusSettled, StatusRefundFailed},
		{StatusSettled, StatusDisputed},
		{StatusRefundPending, StatusRefunded},
		{StatusRefundPending, StatusRefundFailed},
		{StatusRefundPending, StatusPartiallyRefunded},
		{StatusPartiallyRefunded, StatusRefundPending},
		{StatusPartiallyRefunded, StatusRefunded},
		{StatusPartiallyRefunded, StatusRefundFailed},
		{StatusRefundFailed, StatusRefundPending},
		{StatusDisputed, StatusCaptured},
		{StatusDisputed, StatusRefunded},
		{StatusFailed, StatusCancelled},
	}
	for _, c := range cases {
		tx := &Txn{Status: c.from, CancelIntent: true}
		if err := TransitionState(tx, c.to, ActorSystem); err != nil {
			t.Errorf("%s → %s: unexpected error %v", c.from, c.to, err)
		}
		if tx.Status != c.to {
			t.Errorf("%s → %s: status not updated, got %s", c.from, c.to, tx.Status)
		}
	}
}

func TestTransitionState_InvalidTransitions(t *testing.T) {
	cases := []struct {
		from Status
		to   Status
	}{
		{StatusPending, StatusRefunded},
		{StatusProcessing, StatusCancelled},
		{StatusProcessing, StatusPending},
		{StatusCaptured, StatusProcessing},
		{StatusCaptured, StatusPending},
		{StatusCaptured, StatusFailed},
		{StatusCancelled, StatusPending},
		{StatusRefunded, StatusRefundFailed},
		{StatusRefunded, StatusPending},
		{StatusSettled, StatusPending},
		{StatusSettled, StatusCaptured},
	}
	for _, c := range cases {
		tx := &Txn{Status: c.from}
		err := TransitionState(tx, c.to, ActorSystem)
		var invalid ErrInvalidTransition
		if !errors.As(err, &invalid) {
			t.Errorf("%s → %s: expected ErrInvalidTransition, got %v", c.from, c.to, err)
		}
		if tx.Status != c.from {
			t.Errorf("%s → %s: status mutated on invalid transition, got %s", c.from, c.to, tx.Status)
		}
	}
}

func TestTransitionState_TerminalStatesHaveNoTransitions(t *testing.T) {
	for _, terminal := range []Status{StatusCancelled, StatusRefunded} {
		for _, to := range AllStatuses() {
			tx := &Txn{Status: terminal}
			if err := TransitionState(tx, to, ActorSystem); err == nil {
				t.Errorf("terminal %s → %s should be rejected", terminal, to)
			}
		}
	}
}

func TestTransitionState_FailedToCancelledRequiresCancelIntent(t *testing.T) {
	tx := &Txn{Status: StatusFailed, CancelIntent: false}
	if err := TransitionState(tx, StatusCancelled, ActorSystem); err == nil {
		t.Fatal("expected error for FAILED → CANCELLED without cancel intent")
	}
	if tx.Status != StatusFailed {
		t.Errorf("status should remain FAILED, got %s", tx.Status)
	}

	tx = &Txn{Status: StatusFailed, CancelIntent: true}
	if err := TransitionState(tx, StatusCancelled, ActorSystem); err != nil {
		t.Fatalf("expected success for FAILED → CANCELLED with cancel intent, got %v", err)
	}
}

func TestTransitionState_ClearsLeaseFieldsLeavingProcessing(t *testing.T) {
	now := time.Now().UTC()
	timeout := 30 * time.Second
	for _, to := range []Status{StatusCaptured, StatusFailed} {
		tx := &Txn{
			Status:              StatusProcessing,
			ProcessingStartedAt: &now,
			ProcessingTimeout:   &timeout,
		}
		if err := TransitionState(tx, to, ActorSystem); err != nil {
			t.Fatalf("PROCESSING → %s: %v", to, err)
		}
		if tx.ProcessingStartedAt != nil || tx.ProcessingTimeout != nil {
			t.Errorf("PROCESSING → %s: lease fields not cleared", to)
		}
	}
}

func TestTransitionState_NilTransaction(t *testing.T) {
	if err := TransitionState(nil, StatusProcessing, ActorSystem); err == nil {
		t.Fatal("expected error for nil transaction")
	}
}

func TestTransitionState_UpdatesTimestamp(t *testing.T) {
	tx := &Txn{Status: StatusPending, UpdatedAt: time.Unix(0, 0)}
	if err := TransitionState(tx, StatusProcessing, ActorSystem); err != nil {
		t.Fatal(err)
	}
	if tx.UpdatedAt.Equal(time.Unix(0, 0)) {
		t.Error("UpdatedAt not refreshed on transition")
	}
}

func TestValidTransitionsFrom_ReturnsCopy(t *testing.T) {
	got := ValidTransitionsFrom(StatusPending)
	if len(got) != 5 {
		t.Fatalf("expected 5 transitions from PENDING, got %d", len(got))
	}
	got[0] = StatusRefunded
	again := ValidTransitionsFrom(StatusPending)
	if again[0] == StatusRefunded {
		t.Error("ValidTransitionsFrom returned a mutable reference to the internal table")
	}
}

func TestAllStatuses(t *testing.T) {
	if len(AllStatuses()) != 12 {
		t.Errorf("expected 12 statuses, got %d", len(AllStatuses()))
	}
}
