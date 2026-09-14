package ports

import (
	"context"
	"time"

	"github.com/google/uuid"
)

type AuditEntry struct {
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
	AuditEventTypeStateChange = "STATE_CHANGE"
	AuditEventTypeConfigUpdate = "CONFIG_UPDATE"
	AuditEventTypeOpsAction   = "OPS_ACTION"
	AuditEventTypeWebhookReceived = "WEBHOOK_RECEIVED"
	AuditEventTypeRefundInitiated = "REFUND_INITIATED"
)

const (
	LogEventAuditWrite = "audit.write"
)

type AuditLogStore interface {
	WriteEntry(ctx context.Context, entry *AuditEntry) error
	ListByTransaction(ctx context.Context, transactionID uuid.UUID, limit int) ([]AuditEntry, error)
}
