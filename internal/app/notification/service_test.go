package notification

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/domain/notification"
)

type fakeStore struct {
	inserted  []*notification.Notification
	pending   []*notification.Notification
	markedSent []uuid.UUID
	markedFail map[uuid.UUID]string
	insertErr  error
	listErr    error
}

func (f *fakeStore) Insert(_ context.Context, n *notification.Notification) error {
	if f.insertErr != nil {
		return f.insertErr
	}
	f.inserted = append(f.inserted, n)
	return nil
}

func (f *fakeStore) ListPendingNotifications(_ context.Context, _ int) ([]*notification.Notification, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	return f.pending, nil
}

func (f *fakeStore) MarkNotificationSent(_ context.Context, id uuid.UUID) error {
	f.markedSent = append(f.markedSent, id)
	return nil
}

func (f *fakeStore) MarkNotificationFailed(_ context.Context, id uuid.UUID, lastError string) error {
	if f.markedFail == nil {
		f.markedFail = map[uuid.UUID]string{}
	}
	f.markedFail[id] = lastError
	return nil
}

type fakeTemplateStore struct {
	tmpl *notification.Template
	err  error
}

func (f *fakeTemplateStore) GetNotificationTemplate(_ context.Context, _ string) (*notification.Template, error) {
	return f.tmpl, f.err
}

type fakePreferenceStore struct {
	prefs []*notification.Preference
	err   error
}

func (f *fakePreferenceStore) GetNotificationPreferences(_ context.Context, _ uuid.UUID, _ *uuid.UUID) ([]*notification.Preference, error) {
	return f.prefs, f.err
}

type fakeEmailSender struct {
	sent []string
	err  error
}

func (f *fakeEmailSender) SendEmail(_ context.Context, to, subject, bodyText, bodyHTML string) error {
	if f.err != nil {
		return f.err
	}
	f.sent = append(f.sent, to)
	return nil
}

type fakeSMSSender struct {
	sent []string
	err  error
}

func (f *fakeSMSSender) SendSMS(_ context.Context, to, text string) error {
	if f.err != nil {
		return f.err
	}
	f.sent = append(f.sent, to)
	return nil
}

func newTestService(store *fakeStore, templates *fakeTemplateStore, prefs *fakePreferenceStore, email *fakeEmailSender, sms *fakeSMSSender) *Service {
	return NewService(store, templates, prefs, email, sms)
}

func baseNotification() *notification.Notification {
	return &notification.Notification{
		TenantID:     uuid.New(),
		Type:         notification.PaymentSuccess,
		Channel:      notification.ChannelEmail,
		Recipient:    "user@example.com",
		TemplateName: "payment_success",
		TemplateData: map[string]any{"amount": "10.00"},
	}
}

func TestDispatch_EnqueuesWhenEnabled(t *testing.T) {
	store := &fakeStore{}
	s := newTestService(store, nil, &fakePreferenceStore{}, nil, nil)

	n := baseNotification()
	if err := s.Dispatch(context.Background(), n); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(store.inserted) != 1 {
		t.Fatalf("expected 1 insert, got %d", len(store.inserted))
	}
	if store.inserted[0].Recipient != "user@example.com" {
		t.Errorf("recipient should be unchanged without preferences, got %q", store.inserted[0].Recipient)
	}
	if store.inserted[0].ID == uuid.Nil {
		t.Error("dispatched notification should get a fresh ID")
	}
}

func TestDispatch_DisabledByPreferenceSkipsInsert(t *testing.T) {
	store := &fakeStore{}
	s := newTestService(store, nil, &fakePreferenceStore{prefs: []*notification.Preference{
		{Type: notification.PaymentSuccess, Channel: notification.ChannelEmail, Enabled: false},
	}}, nil, nil)

	if err := s.Dispatch(context.Background(), baseNotification()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(store.inserted) != 0 {
		t.Fatalf("disabled preference must not enqueue, got %d inserts", len(store.inserted))
	}
}

func TestDispatch_PreferenceOverrideWins(t *testing.T) {
	store := &fakeStore{}
	s := newTestService(store, nil, &fakePreferenceStore{prefs: []*notification.Preference{
		{Type: notification.PaymentSuccess, Channel: notification.ChannelEmail, Enabled: true, RecipientOverride: "ops@example.com"},
	}}, nil, nil)

	if err := s.Dispatch(context.Background(), baseNotification()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := store.inserted[0].Recipient; got != "ops@example.com" {
		t.Errorf("recipient override not applied, got %q", got)
	}
}

func TestDispatch_NonMatchingPreferenceIgnored(t *testing.T) {
	store := &fakeStore{}
	s := newTestService(store, nil, &fakePreferenceStore{prefs: []*notification.Preference{
		{Type: notification.RefundCompleted, Channel: notification.ChannelEmail, Enabled: false},
	}}, nil, nil)

	if err := s.Dispatch(context.Background(), baseNotification()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(store.inserted) != 1 {
		t.Fatalf("non-matching preference must not disable, got %d inserts", len(store.inserted))
	}
}

func TestDispatch_PreferenceStoreErrorPropagates(t *testing.T) {
	s := newTestService(&fakeStore{}, nil, &fakePreferenceStore{err: errors.New("db down")}, nil, nil)
	err := s.Dispatch(context.Background(), baseNotification())
	if err == nil {
		t.Fatal("expected preference store error to propagate")
	}
}

func TestDispatch_InsertErrorPropagates(t *testing.T) {
	s := newTestService(&fakeStore{insertErr: errors.New("insert failed")}, nil, &fakePreferenceStore{}, nil, nil)
	err := s.Dispatch(context.Background(), baseNotification())
	if err == nil || err.Error() != "insert failed" {
		t.Fatalf("expected 'insert failed', got %v", err)
	}
}

func TestProcessQueue_SendsEmailAndMarksSent(t *testing.T) {
	n := baseNotification()
	n.ID = uuid.New()
	store := &fakeStore{pending: []*notification.Notification{n}}
	email := &fakeEmailSender{}
	s := newTestService(store, &fakeTemplateStore{tmpl: &notification.Template{
		Subject: "Payment of {{.amount}} received",
		BodyText: "You paid {{.amount}}.",
		BodyHTML: "<p>You paid {{.amount}}.</p>",
	}}, &fakePreferenceStore{}, email, nil)

	if err := s.ProcessQueue(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(email.sent) != 1 {
		t.Fatalf("expected 1 email, got %d", len(email.sent))
	}
	if len(store.markedSent) != 1 || store.markedSent[0] != n.ID {
		t.Errorf("expected notification %s marked sent, got %v", n.ID, store.markedSent)
	}
}

func TestProcessQueue_SendsSMSText(t *testing.T) {
	n := baseNotification()
	n.Channel = notification.ChannelSMS
	n.ID = uuid.New()
	store := &fakeStore{pending: []*notification.Notification{n}}
	sms := &fakeSMSSender{}
	s := newTestService(store, &fakeTemplateStore{tmpl: &notification.Template{
		SMSText: "You paid {{.amount}}.",
	}}, &fakePreferenceStore{}, nil, sms)

	if err := s.ProcessQueue(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(sms.sent) != 1 {
		t.Fatalf("expected 1 SMS, got %d", len(sms.sent))
	}
}

func TestProcessQueue_SendFailureMarksFailed(t *testing.T) {
	n := baseNotification()
	n.ID = uuid.New()
	store := &fakeStore{pending: []*notification.Notification{n}}
	s := newTestService(store, &fakeTemplateStore{tmpl: &notification.Template{BodyText: "body"}}, &fakePreferenceStore{}, &fakeEmailSender{err: errors.New("smtp down")}, nil)

	if err := s.ProcessQueue(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(store.markedFail) != 1 {
		t.Fatalf("expected failure recorded, got %v", store.markedFail)
	}
	if store.markedFail[n.ID] != "smtp down" {
		t.Errorf("expected last error 'smtp down', got %q", store.markedFail[n.ID])
	}
	if len(store.markedSent) != 0 {
		t.Error("failed notification must not be marked sent")
	}
}

func TestProcessQueue_MissingTemplateSkips(t *testing.T) {
	n := baseNotification()
	n.ID = uuid.New()
	store := &fakeStore{pending: []*notification.Notification{n}}
	email := &fakeEmailSender{}
	s := newTestService(store, &fakeTemplateStore{tmpl: nil}, &fakePreferenceStore{}, email, nil)

	if err := s.ProcessQueue(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(email.sent) != 0 {
		t.Fatal("notification without a template must not be sent")
	}
	if len(store.markedSent) != 0 || len(store.markedFail) != 0 {
		t.Error("notification without a template must not change status")
	}
}
