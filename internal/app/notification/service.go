package notification

import (
	"bytes"
	"context"
	"fmt"
	"html/template"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/domain/notification"
	"github.com/crownroutes/payment-service/internal/ports"
)

type Service struct {
	store       ports.NotificationStore
	templates   ports.NotificationTemplateStore
	preferences ports.NotificationPreferenceStore
	email       ports.EmailSender
	sms         ports.SMSSender
}

func NewService(
	store ports.NotificationStore,
	templates ports.NotificationTemplateStore,
	preferences ports.NotificationPreferenceStore,
	email ports.EmailSender,
	sms ports.SMSSender,
) *Service {
	return &Service{
		store:       store,
		templates:   templates,
		preferences: preferences,
		email:       email,
		sms:         sms,
	}
}

func (s *Service) Dispatch(ctx context.Context, n *notification.Notification) error {
	prefs, err := s.preferences.GetNotificationPreferences(ctx, n.TenantID, n.UserID)
	if err != nil {
		return err
	}

	enabled := true
	recipient := n.Recipient
	for _, p := range prefs {
		if p.Type == n.Type && p.Channel == n.Channel {
			enabled = p.Enabled
			if p.RecipientOverride != "" {
				recipient = p.RecipientOverride
			}
			break
		}
	}

	if !enabled {
		return nil
	}

	n.Recipient = recipient
	n.ID = uuid.New()

	return s.store.Insert(ctx, n)
}

func (s *Service) ProcessQueue(ctx context.Context) error {
	notifs, err := s.store.ListPendingNotifications(ctx, 20)
	if err != nil {
		return err
	}

	for _, n := range notifs {
		tmpl, err := s.templates.GetNotificationTemplate(ctx, n.TemplateName)
		if err != nil {
			continue
		}
		if tmpl == nil {
			continue
		}
		if err := s.sendNotification(ctx, n, tmpl); err != nil {
			_ = s.store.MarkNotificationFailed(ctx, n.ID, err.Error())
			continue
		}
		_ = s.store.MarkNotificationSent(ctx, n.ID)
	}
	return nil
}

func (s *Service) sendNotification(ctx context.Context, n *notification.Notification, tmpl *notification.Template) error {
	switch n.Channel {
	case notification.ChannelEmail:
		subject := renderTmpl(tmpl.Subject, n.TemplateData)
		bodyText := renderTmpl(tmpl.BodyText, n.TemplateData)
		bodyHTML := renderTmpl(tmpl.BodyHTML, n.TemplateData)
		return s.email.SendEmail(ctx, n.Recipient, subject, bodyText, bodyHTML)
	case notification.ChannelSMS:
		smsText := renderTmpl(tmpl.SMSText, n.TemplateData)
		return s.sms.SendSMS(ctx, n.Recipient, smsText)
	default:
		return fmt.Errorf("unsupported channel: %s", n.Channel)
	}
}

func renderTmpl(tmplText string, data map[string]any) string {
	t, err := template.New("").Parse(tmplText)
	if err != nil {
		return tmplText
	}
	var buf bytes.Buffer
	if err := t.Execute(&buf, data); err != nil {
		return tmplText
	}
	return buf.String()
}
