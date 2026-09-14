package notification

import (
	"time"

	"github.com/google/uuid"
)

type NotificationType string

const (
	PaymentSuccess  NotificationType = "PAYMENT_SUCCESS"
	PaymentFailure  NotificationType = "PAYMENT_FAILURE"
	RefundCompleted NotificationType = "REFUND_COMPLETED"
	RefundFailed    NotificationType = "REFUND_FAILED"
	DisputeOpened   NotificationType = "DISPUTE_OPENED"
	DisputeWon      NotificationType = "DISPUTE_WON"
	DisputeLost     NotificationType = "DISPUTE_LOST"
)

type Channel string

const (
	ChannelEmail Channel = "EMAIL"
	ChannelSMS   Channel = "SMS"
)

type NotificationStatus string

const (
	StatusPending NotificationStatus = "PENDING"
	StatusSent    NotificationStatus = "SENT"
	StatusFailed  NotificationStatus = "FAILED"
)

type Notification struct {
	ID           uuid.UUID          `json:"id"`
	TenantID     uuid.UUID          `json:"tenant_id"`
	UserID       *uuid.UUID         `json:"user_id,omitempty"`
	Type         NotificationType   `json:"notification_type"`
	Channel      Channel            `json:"channel"`
	Recipient    string             `json:"recipient"`
	TemplateName string             `json:"template_name"`
	TemplateData map[string]any     `json:"template_data"`
	Status       NotificationStatus `json:"status"`
	Attempts     int                `json:"attempts"`
	LastError    string             `json:"last_error,omitempty"`
	SentAt       *time.Time         `json:"sent_at,omitempty"`
	CreatedAt    time.Time          `json:"created_at"`
}

type Template struct {
	Name     string `json:"name"`
	Subject  string `json:"subject,omitempty"`
	BodyText string `json:"body_text"`
	BodyHTML string `json:"body_html,omitempty"`
	SMSText  string `json:"sms_text,omitempty"`
}

type Preference struct {
	ID                uuid.UUID        `json:"id"`
	TenantID          uuid.UUID        `json:"tenant_id"`
	UserID            *uuid.UUID       `json:"user_id,omitempty"`
	Type              NotificationType `json:"notification_type"`
	Channel           Channel          `json:"channel"`
	Enabled           bool             `json:"enabled"`
	RecipientOverride string           `json:"recipient_override,omitempty"`
}
