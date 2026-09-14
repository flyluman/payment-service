package webhook

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/domain/notification"
	"github.com/crownroutes/payment-service/internal/domain/transaction"
	"github.com/crownroutes/payment-service/internal/ports"
)

type callbackPayload struct {
	TransactionID string `json:"transaction_id"`
	Status        string `json:"status"`
	CallbackURL   string `json:"callback_url"`
}

type Event struct {
	EventID            string
	GatewayReferenceID string
	Status             string
}

type Outcome struct {
	Duplicate     bool
	UnknownTxn    bool
	Resolved      bool
	Status        transaction.Status
	TransactionID uuid.UUID
}

type TransactionRepository interface {
	GetByGatewayReference(ctx context.Context, gatewayID, reference string) (*transaction.Txn, error)
	UpdateStatus(ctx context.Context, t *transaction.Txn) error
}

type WebhookRepo interface {
	RecordEvent(ctx context.Context, eventID, gatewayID string) (bool, error)
	InsertGatewayMetadata(ctx context.Context, transactionID uuid.UUID, gatewayID string, payload []byte) error
}

type EventWriter interface {
	Write(ctx context.Context, event ports.OutboxEvent) error
}

type Transactor interface {
	WithinTx(ctx context.Context, fn func(ctx context.Context) error) error
}

type Service struct {
	txns         TransactionRepository
	webhooks     WebhookRepo
	outbox       EventWriter
	tx           Transactor
	audit        ports.AuditLogStore
	notif        ports.NotificationDispatcher
	twDispatcher ports.TenantWebhookDispatcher
	log          ports.Logger
	metrics      ports.MetricRecorder
	bus          ports.EventBus
}

func NewService(txns TransactionRepository, webhooks WebhookRepo, outbox EventWriter, tx Transactor, log ports.Logger, metrics ports.MetricRecorder) *Service {
	return &Service{txns: txns, webhooks: webhooks, outbox: outbox, tx: tx, log: log, metrics: metrics}
}

func (s *Service) SetEventBus(bus ports.EventBus) { s.bus = bus }
func (s *Service) SetAuditLogStore(a ports.AuditLogStore) { s.audit = a }
func (s *Service) SetNotificationService(n ports.NotificationDispatcher) { s.notif = n }
func (s *Service) SetTenantWebhookDispatcher(d ports.TenantWebhookDispatcher) { s.twDispatcher = d }

type webhookEventPayload struct {
	TransactionID    string `json:"transaction_id"`
	Status           string `json:"status"`
	Gateway          string `json:"gateway"`
	Source           string `json:"source"`
	AggregateVersion int    `json:"aggregate_version"`
}

func (s *Service) Process(ctx context.Context, gatewayID string, ev Event, rawPayload []byte) (Outcome, error) {
	var outcome Outcome
	err := s.tx.WithinTx(ctx, func(ctx context.Context) error {
		recorded, err := s.webhooks.RecordEvent(ctx, ev.EventID, gatewayID)
		if err != nil {
			return err
		}
		if !recorded {
			outcome.Duplicate = true
			return nil
		}

		txn, err := s.txns.GetByGatewayReference(ctx, gatewayID, ev.GatewayReferenceID)
		if err != nil {
			return fmt.Errorf("lookup transaction: %w", err)
		}
		if txn == nil {
			outcome.UnknownTxn = true
			return nil
		}

		if err := s.webhooks.InsertGatewayMetadata(ctx, txn.ID, gatewayID, rawPayload); err != nil {
			return err
		}

		newStatus, terminal := mapWebhookStatus(ev.Status)
		if !terminal || txn.Status != transaction.StatusProcessing {
			outcome.Status = txn.Status
			return nil
		}

		if err := transaction.TransitionState(txn, newStatus, transaction.ActorGateway); err != nil {
			return err
		}
		if newStatus == transaction.StatusFailed {
			txn.FailureReason = &transaction.FailureReason{
				Category: "gateway_webhook_failed",
				Source:   transaction.FailureReasonSourceGateway,
			}
		}
		if err := s.txns.UpdateStatus(ctx, txn); err != nil {
			return err
		}

		if s.audit != nil {
			_ = s.audit.WriteEntry(ctx, &ports.AuditEntry{
				TransactionID: &txn.ID,
				EventType:     ports.AuditEventTypeWebhookReceived,
				Actor:         string(transaction.ActorGateway),
				PreviousState: string(transaction.StatusProcessing),
				NewState:      string(newStatus),
				Reason:        "webhook_received",
			})
		}

		event, err := s.buildEvent(txn, newStatus, gatewayID)
		if err != nil {
			return err
		}
		if err := s.outbox.Write(ctx, event); err != nil {
			return err
		}

		if txn.CallbackURL != "" {
			cbPayload, err := json.Marshal(callbackPayload{
				TransactionID: txn.ID.String(),
				Status:        string(newStatus),
				CallbackURL:   txn.CallbackURL,
			})
			if err != nil {
				return err
			}
			if err := s.outbox.Write(ctx, ports.OutboxEvent{
				AggregateID:      txn.ID,
				AggregateType:    "transaction",
				EventType:        ports.EventTypeTransactionCallback,
				Payload:          cbPayload,
				EventVersion:     1,
				AggregateVersion: txn.Version,
			}); err != nil {
				return err
			}
		}

		s.dispatchTerminalNotification(ctx, txn, newStatus)
		s.dispatchTenantWebhook(ctx, txn, newStatus)

		outcome.Resolved = true
		outcome.Status = newStatus
		outcome.TransactionID = txn.ID
		return nil
	})
	if err != nil {
		return Outcome{}, fmt.Errorf("webhook: process %s/%s: %w", gatewayID, ev.EventID, err)
	}

	switch {
	case outcome.Duplicate:
		s.log.Info(ports.LogEventWebhookInboundDuplicate, map[string]any{ports.FieldGatewayID: gatewayID})
	case outcome.Resolved:
		s.log.Info(ports.LogEventWebhookInboundReceived, map[string]any{
			ports.FieldGatewayID: gatewayID,
			ports.FieldNewState:  string(outcome.Status),
		})
		if s.bus != nil {
			s.bus.Publish(ctx, ports.StatusEvent{
				TransactionID: outcome.TransactionID,
				Status:        outcome.Status,
			})
		}
	}
	return outcome, nil
}

func (s *Service) buildEvent(txn *transaction.Txn, status transaction.Status, gatewayID string) (ports.OutboxEvent, error) {
	eventType := ports.EventTypeTransactionFailed
	switch status {
	case transaction.StatusCaptured:
		eventType = ports.EventTypeTransactionCaptured
	case transaction.StatusCancelled:
		eventType = ports.EventTypeTransactionCancelled
	}
	payload, err := json.Marshal(webhookEventPayload{
		TransactionID:    txn.ID.String(),
		Status:           string(status),
		Gateway:          gatewayID,
		Source:           "webhook",
		AggregateVersion: txn.Version,
	})
	if err != nil {
		return ports.OutboxEvent{}, fmt.Errorf("marshal webhook event: %w", err)
	}
	return ports.OutboxEvent{
		AggregateID:      txn.ID,
		AggregateType:    "transaction",
		EventType:        eventType,
		Payload:          payload,
		EventVersion:     1,
		AggregateVersion: txn.Version,
	}, nil
}

func mapWebhookStatus(s string) (transaction.Status, bool) {
	switch s {
	case "succeeded", "success", "captured", "paid":
		return transaction.StatusCaptured, true
	case "authorized":
		return transaction.StatusAuthorized, true
	case "failed", "failure":
		return transaction.StatusFailed, true
	case "cancelled", "canceled":
		return transaction.StatusCancelled, true
	default:
		return "", false
	}
}

func (s *Service) dispatchTerminalNotification(ctx context.Context, txn *transaction.Txn, status transaction.Status) {
	if s.notif == nil || txn.CustomerEmail == "" {
		return
	}

	data := map[string]any{
		"amount":         txn.Amount,
		"currency":       txn.Currency,
		"transaction_id": txn.ID.String(),
	}

	switch status {
	case transaction.StatusCaptured:
		s.notif.Dispatch(ctx, &notification.Notification{
			TenantID:     txn.TenantID,
			UserID:       &txn.UserID,
			Type:         notification.PaymentSuccess,
			Channel:      notification.ChannelEmail,
			Recipient:    txn.CustomerEmail,
			TemplateName: "PAYMENT_SUCCESS",
			TemplateData: data,
		})
	case transaction.StatusFailed:
		reason := ""
		if txn.FailureReason != nil {
			reason = txn.FailureReason.GatewayMessage
		}
		data["reason"] = reason
		s.notif.Dispatch(ctx, &notification.Notification{
			TenantID:     txn.TenantID,
			UserID:       &txn.UserID,
			Type:         notification.PaymentFailure,
			Channel:      notification.ChannelEmail,
			Recipient:    txn.CustomerEmail,
			TemplateName: "PAYMENT_FAILURE",
			TemplateData: data,
		})
	}
}

func (s *Service) dispatchTenantWebhook(ctx context.Context, txn *transaction.Txn, status transaction.Status) {
	if s.twDispatcher == nil {
		return
	}

	var eventType string
	switch status {
	case transaction.StatusCaptured:
		eventType = ports.EventTypeTransactionCaptured
	case transaction.StatusFailed:
		eventType = ports.EventTypeTransactionFailed
	case transaction.StatusCancelled:
		eventType = ports.EventTypeTransactionCancelled
	default:
		return
	}

	payload, err := json.Marshal(map[string]any{
		"transaction_id": txn.ID.String(),
		"status":         string(status),
		"amount":         txn.Amount,
		"currency":       txn.Currency,
	})
	if err != nil {
		return
	}

	_ = s.twDispatcher.Dispatch(ctx, txn.TenantID, txn.ID, eventType, payload)
}
