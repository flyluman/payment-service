package ports

import (
	"context"
	"time"

	"github.com/google/uuid"
)

type OutboxWriter interface {
	Write(ctx context.Context, event OutboxEvent) error
	MarkPublished(ctx context.Context, id uuid.UUID, createdAt time.Time) error
	MarkFailed(ctx context.Context, id uuid.UUID, createdAt time.Time, lastErr string, nextAttempt time.Time) error
	MarkExhausted(ctx context.Context, id uuid.UUID, createdAt time.Time, lastErr string) error
	PollPending(ctx context.Context, shards []int, batchSize int) ([]PendingEvent, error)
	ReplayDeadLetter(ctx context.Context, deadLetterID uuid.UUID, actor, reason string) (uuid.UUID, error)
}

type PendingEvent struct {
	ID               uuid.UUID
	AggregateID      uuid.UUID
	AggregateType    string
	EventType        string
	Payload          []byte
	EventVersion     int
	AggregateVersion int
	Attempts         int
	CreatedAt        time.Time
}

type OutboxEvent struct {
	ID               uuid.UUID
	AggregateID      uuid.UUID
	AggregateType    string // e.g. "transaction", "refund"
	EventType        string
	Payload          []byte // must be JSON-encoded; maps to JSONB column
	EventVersion     int
	AggregateVersion int        // per-aggregate monotonic sequence; 0 = not applicable
	NextAttemptAt    *time.Time // nil = publish immediately (next_attempt_at = NOW())
}

const (
	EventTypeTransactionCreated   = "TRANSACTION_CREATED"
	EventTypeTransactionCaptured  = "TRANSACTION_CAPTURED"
	EventTypeTransactionFailed    = "TRANSACTION_FAILED"
	EventTypeTransactionCancelled = "TRANSACTION_CANCELLED"

	EventTypeRefundInitiated = "REFUND_INITIATED"
	EventTypeRefundSucceeded = "REFUND_SUCCEEDED"
	EventTypeRefundFailed    = "REFUND_FAILED"

	EventTypeAuditStateChange = "AUDIT_STATE_CHANGE"

	EventTypeTransactionCallback = "TRANSACTION_CALLBACK"

	EventTypeGatewayInitiate = "GATEWAY_INITIATE"

	EventTypeDisputeCreated = "DISPUTE_CREATED"
	EventTypeDisputeUpdated = "DISPUTE_UPDATED"
)

type OutboxStatus string

const (
	OutboxStatusPending   OutboxStatus = "PENDING"
	OutboxStatusPublished OutboxStatus = "PUBLISHED"
	OutboxStatusFailed    OutboxStatus = "FAILED"
)

type DeadLetter struct {
	ID               uuid.UUID
	OriginalEventID  uuid.UUID
	AggregateID      uuid.UUID
	AggregateType    string
	EventType        string
	Payload          []byte
	EventVersion     int
	AggregateVersion int
	FailureReason    string
	ErrorMessage     string
	Attempts         int
	FailedAt         time.Time
	CreatedAt        time.Time
	ResolvedAt       *time.Time
	ResolvedBy       string
}

type TenantWebhookWriter interface {
	WriteDelivery(ctx context.Context, delivery TenantWebhookDelivery) error
}

type TenantWebhookDelivery struct {
	ID            uuid.UUID
	TenantID      uuid.UUID
	TransactionID uuid.UUID
	EventType     string
	Payload       []byte
	EndpointURL   string
	Attempts      int
}

type TenantWebhookDispatcher interface {
	Dispatch(ctx context.Context, tenantID uuid.UUID, txnID uuid.UUID, eventType string, payload []byte) error
}

type DeadLetterFilter struct {
	Resolved  *bool
	EventType *string
	DateFrom  *time.Time
	DateTo    *time.Time
	Limit     int
}
