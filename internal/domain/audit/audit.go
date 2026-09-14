package audit

import (
	"time"

	"github.com/google/uuid"
)

type Entry struct {
	ID            uuid.UUID
	TransactionID *uuid.UUID
	EventType     string
	Actor         string
	PreviousState string
	NewState      string
	Reason        string
	Metadata      map[string]any
	CreatedAt     time.Time
}

const (
	EventTypeStateChange      = "STATE_CHANGE"
	EventTypeConfigUpdate     = "CONFIG_UPDATE"
	EventTypeOpsAction        = "OPS_ACTION"
	EventTypeWebhookReceived  = "WEBHOOK_RECEIVED"
	EventTypeRefundInitiated  = "REFUND_INITIATED"
)

func NewEntry(
	transactionID *uuid.UUID,
	eventType string,
	actor string,
	previousState string,
	newState string,
	reason string,
	metadata map[string]any,
) *Entry {
	return &Entry{
		ID:            uuid.Must(uuid.NewV7()),
		TransactionID: transactionID,
		EventType:     eventType,
		Actor:         actor,
		PreviousState: previousState,
		NewState:      newState,
		Reason:        reason,
		Metadata:      metadata,
		CreatedAt:     time.Now().UTC(),
	}
}
