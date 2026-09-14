package dispute

import (
	"time"

	"github.com/google/uuid"
)

type Status string

const (
	StatusNeedsResponse Status = "NEEDS_RESPONSE"
	StatusUnderReview   Status = "UNDER_REVIEW"
	StatusWon           Status = "WON"
	StatusLost          Status = "LOST"
	StatusAccepted      Status = "ACCEPTED"
	StatusExpired       Status = "EXPIRED"
)

var validTransitions = map[Status][]Status{
	StatusNeedsResponse: {StatusUnderReview, StatusAccepted, StatusExpired},
	StatusUnderReview:   {StatusWon, StatusLost, StatusAccepted},
}

type Dispute struct {
	ID                  uuid.UUID  `json:"id"`
	TransactionID       uuid.UUID  `json:"transaction_id"`
	GatewayID           string     `json:"gateway_id"`
	GatewayDisputeID    string     `json:"gateway_dispute_id"`
	Reason              string     `json:"reason"`
	Status              Status     `json:"status"`
	Amount              int64      `json:"amount"`
	Currency            string     `json:"currency"`
	EvidenceDueBy       *time.Time `json:"evidence_due_by,omitempty"`
	EvidenceSubmittedAt *time.Time `json:"evidence_submitted_at,omitempty"`
	CreatedAt           time.Time  `json:"created_at"`
	ResolvedAt          *time.Time `json:"resolved_at,omitempty"`
}

func (d *Dispute) IsUrgent(warningDays int) bool {
	if d.EvidenceDueBy == nil {
		return false
	}
	if d.Status != StatusNeedsResponse && d.Status != StatusUnderReview {
		return false
	}
	return time.Until(*d.EvidenceDueBy) <= time.Duration(warningDays)*24*time.Hour
}

func (d *Dispute) DaysUntilDue() *int {
	if d.EvidenceDueBy == nil {
		return nil
	}
	days := int(time.Until(*d.EvidenceDueBy).Hours() / 24)
	return &days
}

func (d *Dispute) CanSubmitEvidence() bool {
	return d.Status == StatusNeedsResponse || d.Status == StatusUnderReview
}

func (d *Dispute) Transition(to Status) error {
	allowed, ok := validTransitions[d.Status]
	if !ok {
		return ErrInvalidTransition
	}
	for _, s := range allowed {
		if s == to {
			d.Status = to
			if to == StatusWon || to == StatusLost || to == StatusAccepted || to == StatusExpired {
				now := time.Now().UTC()
				d.ResolvedAt = &now
			}
			return nil
		}
	}
	return ErrInvalidTransition
}

var ErrInvalidTransition = &DisputeError{Code: "INVALID_TRANSITION", Message: "invalid status transition"}
var ErrNotFound = &DisputeError{Code: "NOT_FOUND", Message: "dispute not found"}
var ErrAlreadyExists = &DisputeError{Code: "ALREADY_EXISTS", Message: "dispute already exists"}

type DisputeError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e *DisputeError) Error() string {
	return e.Message
}
