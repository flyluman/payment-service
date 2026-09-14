package transaction

import (
	"fmt"
	"time"
)

type ErrInvalidTransition struct {
	From  Status
	To    Status
	Actor Actor
}

func (e ErrInvalidTransition) Error() string {
	return fmt.Sprintf("transaction: invalid state transition %s → %s (actor: %s)", e.From, e.To, e.Actor)
}

var transitionTable = map[Status][]Status{
	StatusPending:           {StatusProcessing, StatusAuthorized, StatusCaptured, StatusFailed, StatusCancelled},
	StatusProcessing:        {StatusCaptured, StatusAuthorized, StatusFailed, StatusCancelled},
	StatusAuthorized:        {StatusProcessing, StatusCaptured, StatusFailed, StatusCancelled},
	StatusCaptured:          {StatusSettled, StatusRefundPending, StatusRefunded, StatusRefundFailed, StatusDisputed},
	StatusSettled:           {StatusRefundPending, StatusRefunded, StatusRefundFailed, StatusDisputed},
	StatusFailed:            {StatusCancelled},
	StatusRefundPending:     {StatusRefunded, StatusRefundFailed, StatusPartiallyRefunded},
	StatusPartiallyRefunded: {StatusRefundPending, StatusRefunded, StatusRefundFailed},
	StatusRefunded:          {},
	StatusRefundFailed:      {StatusRefundPending},
	StatusDisputed:          {StatusCaptured, StatusRefunded},
	StatusCancelled:         {},
}

func isValidTransition(from, to Status) bool {
	allowed, ok := transitionTable[from]
	if !ok {
		return false
	}
	for _, s := range allowed {
		if s == to {
			return true
		}
	}
	return false
}

func applyTransitionEffects(tx *Txn, from, to Status, now time.Time) {
	switch {
	case to == StatusFailed || to == StatusCancelled || to == StatusCaptured || to == StatusSettled || to == StatusAuthorized:
		if from == StatusProcessing {
			tx.ProcessingStartedAt = nil
			tx.ProcessingTimeout = nil
		}
	}
}

func TransitionState(tx *Txn, toState Status, actor Actor) error {
	if tx == nil {
		return fmt.Errorf("transaction: nil transaction")
	}
	if !isValidTransition(tx.Status, toState) {
		return ErrInvalidTransition{From: tx.Status, To: toState, Actor: actor}
	}
	if tx.Status == StatusFailed && toState == StatusCancelled && !tx.CancelIntent {
		return fmt.Errorf("transaction: cancel intent required for FAILED → CANCELLED")
	}

	previous := tx.Status
	now := time.Now().UTC()

	tx.Status = toState
	tx.UpdatedAt = now

	applyTransitionEffects(tx, previous, toState, now)

	return nil
}

func ValidTransitionsFrom(s Status) []Status {
	result, ok := transitionTable[s]
	if !ok {
		return nil
	}
	out := make([]Status, len(result))
	copy(out, result)
	return out
}

func AllStatuses() []Status {
	all := []Status{
		StatusPending,
		StatusProcessing,
		StatusAuthorized,
		StatusCaptured,
		StatusSettled,
		StatusFailed,
		StatusCancelled,
		StatusRefundPending,
		StatusPartiallyRefunded,
		StatusRefunded,
		StatusRefundFailed,
		StatusDisputed,
	}
	out := make([]Status, len(all))
	copy(out, all)
	return out
}
