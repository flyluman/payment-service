package ports

import (
	"context"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/domain/notification"
)

type NotificationStore interface {
	Insert(ctx context.Context, n *notification.Notification) error
	ListPendingNotifications(ctx context.Context, limit int) ([]*notification.Notification, error)
	MarkNotificationSent(ctx context.Context, id uuid.UUID) error
	MarkNotificationFailed(ctx context.Context, id uuid.UUID, lastError string) error
}

type NotificationTemplateStore interface {
	GetNotificationTemplate(ctx context.Context, name string) (*notification.Template, error)
}

type NotificationPreferenceStore interface {
	GetNotificationPreferences(ctx context.Context, tenantID uuid.UUID, userID *uuid.UUID) ([]*notification.Preference, error)
}

type EmailSender interface {
	SendEmail(ctx context.Context, to, subject, bodyText, bodyHTML string) error
}

type SMSSender interface {
	SendSMS(ctx context.Context, to, text string) error
}
