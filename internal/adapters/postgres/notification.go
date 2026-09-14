package postgres

import (
	"context"
	"encoding/json"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/crownroutes/payment-service/internal/domain/notification"
)

type NotificationStore struct {
	db *DB
}

func NewNotificationStore(db *DB) *NotificationStore {
	return &NotificationStore{db: db}
}

func (s *NotificationStore) Insert(ctx context.Context, n *notification.Notification) error {
	td, err := json.Marshal(n.TemplateData)
	if err != nil {
		return err
	}
	tx, err := txFromContext(ctx)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO notifications (id, tenant_id, user_id, notification_type, channel, recipient, template_name, template_data, status, attempts, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'PENDING', 0, NOW())
	`, n.ID, n.TenantID, n.UserID, string(n.Type), string(n.Channel),
		n.Recipient, n.TemplateName, td)
	return err
}

func (s *NotificationStore) ListPendingNotifications(ctx context.Context, limit int) ([]*notification.Notification, error) {
	rows, err := s.db.Pool().Query(ctx, `
		SELECT id, tenant_id, user_id, notification_type, channel, recipient, template_name, template_data, status, attempts, last_error, sent_at, created_at
		FROM notifications
		WHERE status = 'PENDING'
		ORDER BY created_at ASC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var notifs []*notification.Notification
	for rows.Next() {
		n, err := scanNotification(rows)
		if err != nil {
			return nil, err
		}
		notifs = append(notifs, n)
	}
	return notifs, rows.Err()
}

func (s *NotificationStore) MarkNotificationSent(ctx context.Context, id uuid.UUID) error {
	_, err := s.db.Pool().Exec(ctx, `
		UPDATE notifications SET status = 'SENT', sent_at = NOW() WHERE id = $1
	`, id)
	return err
}

func (s *NotificationStore) MarkNotificationFailed(ctx context.Context, id uuid.UUID, lastError string) error {
	_, err := s.db.Pool().Exec(ctx, `
		UPDATE notifications
		SET status = CASE WHEN attempts >= 5 THEN 'FAILED' ELSE 'PENDING' END,
		    attempts = attempts + 1,
		    last_error = $2
		WHERE id = $1
	`, id, lastError)
	return err
}

type NotificationTemplateStore struct {
	db *DB
}

func NewNotificationTemplateStore(db *DB) *NotificationTemplateStore {
	return &NotificationTemplateStore{db: db}
}

func (s *NotificationTemplateStore) GetNotificationTemplate(ctx context.Context, name string) (*notification.Template, error) {
	row := s.db.Pool().QueryRow(ctx, `
		SELECT name, subject, body_text, body_html, sms_text
		FROM notification_templates WHERE name = $1
	`, name)
	var t notification.Template
	err := row.Scan(&t.Name, &t.Subject, &t.BodyText, &t.BodyHTML, &t.SMSText)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &t, nil
}

type NotificationPreferenceStore struct {
	db *DB
}

func NewNotificationPreferenceStore(db *DB) *NotificationPreferenceStore {
	return &NotificationPreferenceStore{db: db}
}

func (s *NotificationPreferenceStore) GetNotificationPreferences(ctx context.Context, tenantID uuid.UUID, userID *uuid.UUID) ([]*notification.Preference, error) {
	rows, err := s.db.Pool().Query(ctx, `
		SELECT id, tenant_id, user_id, notification_type, channel, enabled, recipient_override
		FROM notification_preferences
		WHERE tenant_id = $1 AND (user_id = $2 OR ($2 IS NULL AND user_id IS NULL))
	`, tenantID, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var prefs []*notification.Preference
	for rows.Next() {
		var p notification.Preference
		if err := rows.Scan(&p.ID, &p.TenantID, &p.UserID, &p.Type, &p.Channel, &p.Enabled, &p.RecipientOverride); err != nil {
			return nil, err
		}
		prefs = append(prefs, &p)
	}
	return prefs, rows.Err()
}

func scanNotification(rows pgx.Rows) (*notification.Notification, error) {
	var n notification.Notification
	var ntype, channel, status string
	var templateData []byte
	if err := rows.Scan(&n.ID, &n.TenantID, &n.UserID, &ntype, &channel,
		&n.Recipient, &n.TemplateName, &templateData, &status, &n.Attempts,
		&n.LastError, &n.SentAt, &n.CreatedAt); err != nil {
		return nil, err
	}
	n.Type = notification.NotificationType(ntype)
	n.Channel = notification.Channel(channel)
	n.Status = notification.NotificationStatus(status)
	if templateData != nil {
		n.TemplateData = make(map[string]any)
		_ = json.Unmarshal(templateData, &n.TemplateData)
	}
	return &n, nil
}
