package dispute

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/domain/dispute"
	"github.com/crownroutes/payment-service/internal/domain/notification"
	"github.com/crownroutes/payment-service/internal/domain/transaction"
	"github.com/crownroutes/payment-service/internal/ports"
)

type TransactionReader interface {
	GetByID(ctx context.Context, id uuid.UUID) (*transaction.Txn, error)
	GetByGatewayReference(ctx context.Context, gatewayID, reference string) (*transaction.Txn, error)
	UpdateStatus(ctx context.Context, t *transaction.Txn) error
}

type EventWriter interface {
	Write(ctx context.Context, event ports.OutboxEvent) error
}

type Transactor interface {
	WithinTx(ctx context.Context, fn func(ctx context.Context) error) error
}

type Service struct {
	store       ports.DisputeStore
	evidence    ports.DisputeEvidenceStore
	txns        TransactionReader
	tx          Transactor
	outbox      EventWriter
	audit       ports.AuditLogStore
	notif       ports.NotificationDispatcher
	twDispatcher ports.TenantWebhookDispatcher
	bus         ports.EventBus
}

func NewService(store ports.DisputeStore, evidence ports.DisputeEvidenceStore, txns TransactionReader, tx Transactor) *Service {
	return &Service{store: store, evidence: evidence, txns: txns, tx: tx}
}

func (s *Service) SetOutboxWriter(outbox EventWriter) { s.outbox = outbox }
func (s *Service) SetAuditLogStore(a ports.AuditLogStore) { s.audit = a }
func (s *Service) SetEventBus(bus ports.EventBus) { s.bus = bus }
func (s *Service) SetNotificationService(n ports.NotificationDispatcher) { s.notif = n }
func (s *Service) SetTenantWebhookDispatcher(d ports.TenantWebhookDispatcher) { s.twDispatcher = d }

func (s *Service) CreateDispute(ctx context.Context, d *dispute.Dispute) error {
	existing, err := s.store.GetByGatewayDisputeID(ctx, d.GatewayID, d.GatewayDisputeID)
	if err != nil && !isNotFound(err) {
		return err
	}
	if existing != nil {
		existing.Status = d.Status
		existing.Reason = d.Reason
		existing.EvidenceDueBy = d.EvidenceDueBy
		if d.Status == dispute.StatusWon || d.Status == dispute.StatusLost || d.Status == dispute.StatusAccepted || d.Status == dispute.StatusExpired {
			existing.ResolvedAt = d.ResolvedAt
		}
		if err := s.store.Update(ctx, existing); err != nil {
			return err
		}
		if s.outbox != nil {
			payload, err := json.Marshal(map[string]any{
				"dispute_id":     d.ID.String(),
				"transaction_id": d.TransactionID.String(),
				"gateway_id":     d.GatewayID,
				"status":         string(d.Status),
				"reason":         d.Reason,
			})
			if err == nil {
				_ = s.outbox.Write(ctx, ports.OutboxEvent{
					AggregateID:   d.TransactionID,
					AggregateType: "transaction",
					EventType:     ports.EventTypeDisputeUpdated,
					Payload:       payload,
					EventVersion:  1,
				})
			}
		}
		return s.transitionParentOnDisputeUpdate(ctx, d.TransactionID, d.Status)
	}

	if err := s.store.Create(ctx, d); err != nil {
		return err
	}
	if s.outbox != nil {
		payload, err := json.Marshal(map[string]any{
			"dispute_id":     d.ID.String(),
			"transaction_id": d.TransactionID.String(),
			"gateway_id":     d.GatewayID,
			"status":         string(d.Status),
			"reason":         d.Reason,
		})
		if err == nil {
			_ = s.outbox.Write(ctx, ports.OutboxEvent{
				AggregateID:   d.TransactionID,
				AggregateType: "transaction",
				EventType:     ports.EventTypeDisputeCreated,
				Payload:       payload,
				EventVersion:  1,
			})
		}
	}
	return s.transitionParentOnDisputeCreate(ctx, d.TransactionID)
}

func (s *Service) IngestGatewayDispute(ctx context.Context, gatewayID string, ev ports.GatewayDisputeEvent) error {
	txn, err := s.txns.GetByGatewayReference(ctx, gatewayID, ev.GatewayReferenceID)
	if err != nil {
		return fmt.Errorf("dispute: lookup transaction by gateway reference %s: %w", ev.GatewayReferenceID, err)
	}
	if txn == nil {
		return fmt.Errorf("dispute: no transaction for gateway reference %s", ev.GatewayReferenceID)
	}

	now := time.Now().UTC()
	d := &dispute.Dispute{
		ID:               uuid.New(),
		TransactionID:    txn.ID,
		GatewayID:        gatewayID,
		GatewayDisputeID: ev.GatewayDisputeID,
		Status:           dispute.Status(ev.Status),
		Reason:           ev.Reason,
		Amount:           ev.Amount,
		Currency:         ev.Currency,
		EvidenceDueBy:    ev.EvidenceDueBy,
		CreatedAt:        now,
	}
	if ev.ResolvedAt != nil {
		d.ResolvedAt = ev.ResolvedAt
	}
	return s.CreateDispute(ctx, d)
}

func (s *Service) transitionParentOnDisputeCreate(ctx context.Context, txnID uuid.UUID) error {
	txn, err := s.txns.GetByID(ctx, txnID)
	if err != nil {
		return fmt.Errorf("dispute: load transaction %s: %w", txnID, err)
	}
	if txn == nil {
		return nil
	}
	if err := transaction.TransitionState(txn, transaction.StatusDisputed, transaction.ActorGateway); err != nil {
		return nil
	}
	s.dispatchDisputeNotification(ctx, txn, notification.DisputeOpened)
	s.dispatchDisputeWebhook(ctx, txn, ports.EventTypeDisputeCreated)
	if s.bus != nil {
		s.bus.Publish(ctx, ports.StatusEvent{
			TransactionID: txn.ID,
			Status:        txn.Status,
		})
	}
	return s.txns.UpdateStatus(ctx, txn)
}

func (s *Service) transitionParentOnDisputeUpdate(ctx context.Context, txnID uuid.UUID, disputeStatus dispute.Status) error {
	txn, err := s.txns.GetByID(ctx, txnID)
	if err != nil {
		return fmt.Errorf("dispute: load transaction %s: %w", txnID, err)
	}
	if txn == nil || txn.Status != transaction.StatusDisputed {
		return nil
	}
	switch disputeStatus {
	case dispute.StatusWon:
		if err := transaction.TransitionState(txn, transaction.StatusCaptured, transaction.ActorSystem); err != nil {
			return nil
		}
		if err := s.txns.UpdateStatus(ctx, txn); err != nil {
			return err
		}
		s.dispatchDisputeNotification(ctx, txn, notification.DisputeWon)
		s.dispatchDisputeWebhook(ctx, txn, ports.EventTypeDisputeUpdated)
		if s.bus != nil {
			s.bus.Publish(ctx, ports.StatusEvent{
				TransactionID: txn.ID,
				Status:        txn.Status,
			})
		}
		if s.outbox != nil {
			payload, err := json.Marshal(map[string]any{
				"transaction_id": txnID.String(),
				"dispute_status": string(disputeStatus),
				"new_txn_status": string(transaction.StatusCaptured),
			})
			if err == nil {
				_ = s.outbox.Write(ctx, ports.OutboxEvent{
					AggregateID:   txnID,
					AggregateType: "transaction",
					EventType:     ports.EventTypeDisputeUpdated,
					Payload:       payload,
					EventVersion:  1,
				})
			}
		}
		return nil
	case dispute.StatusLost:
		if err := transaction.TransitionState(txn, transaction.StatusRefunded, transaction.ActorSystem); err != nil {
			return nil
		}
		if err := s.txns.UpdateStatus(ctx, txn); err != nil {
			return err
		}
		s.dispatchDisputeNotification(ctx, txn, notification.DisputeLost)
		s.dispatchDisputeWebhook(ctx, txn, ports.EventTypeDisputeUpdated)
		if s.bus != nil {
			s.bus.Publish(ctx, ports.StatusEvent{
				TransactionID: txn.ID,
				Status:        txn.Status,
			})
		}
		if s.outbox != nil {
			payload, err := json.Marshal(map[string]any{
				"transaction_id": txnID.String(),
				"dispute_status": string(disputeStatus),
				"new_txn_status": string(transaction.StatusRefunded),
			})
			if err == nil {
				_ = s.outbox.Write(ctx, ports.OutboxEvent{
					AggregateID:   txnID,
					AggregateType: "transaction",
					EventType:     ports.EventTypeDisputeUpdated,
					Payload:       payload,
					EventVersion:  1,
				})
			}
		}
		return nil
	}
	return nil
}

func (s *Service) GetDispute(ctx context.Context, id uuid.UUID) (*dispute.Dispute, error) {
	return s.store.GetByID(ctx, id)
}

func (s *Service) ListDisputes(ctx context.Context, filters ports.DisputeFilters) ([]*dispute.Dispute, error) {
	return s.store.List(ctx, filters)
}

func (s *Service) SubmitEvidence(ctx context.Context, disputeID uuid.UUID, ev *dispute.Evidence) error {
	d, err := s.store.GetByID(ctx, disputeID)
	if err != nil {
		return err
	}
	if !d.CanSubmitEvidence() {
		return dispute.ErrInvalidTransition
	}
	ev.DisputeID = disputeID
	if err := s.evidence.Add(ctx, ev); err != nil {
		return err
	}
	if d.EvidenceSubmittedAt == nil {
		now := ev.SubmittedAt
		d.EvidenceSubmittedAt = &now
		return s.store.Update(ctx, d)
	}
	return nil
}

func (s *Service) ListEvidence(ctx context.Context, disputeID uuid.UUID) ([]*dispute.Evidence, error) {
	return s.evidence.ListByDisputeID(ctx, disputeID)
}

func isNotFound(err error) bool {
	if de, ok := err.(*dispute.DisputeError); ok && de.Code == "NOT_FOUND" {
		return true
	}
	return false
}

func (s *Service) dispatchDisputeNotification(ctx context.Context, txn *transaction.Txn, notifType notification.NotificationType) {
	if s.notif == nil || txn.CustomerEmail == "" {
		return
	}

	data := map[string]any{
		"transaction_id": txn.ID.String(),
	}

	tmplName := "DISPUTE_OPENED"
	switch notifType {
	case notification.DisputeWon:
		tmplName = "DISPUTE_WON"
	case notification.DisputeLost:
		tmplName = "DISPUTE_LOST"
	}

	s.notif.Dispatch(ctx, &notification.Notification{
		TenantID:     txn.TenantID,
		UserID:       &txn.UserID,
		Type:         notifType,
		Channel:      notification.ChannelEmail,
		Recipient:    txn.CustomerEmail,
		TemplateName: tmplName,
		TemplateData: data,
	})
}

func (s *Service) dispatchDisputeWebhook(ctx context.Context, txn *transaction.Txn, eventType string) {
	if s.twDispatcher == nil {
		return
	}

	payload, err := json.Marshal(map[string]any{
		"transaction_id": txn.ID.String(),
		"status":         string(txn.Status),
	})
	if err != nil {
		return
	}

	_ = s.twDispatcher.Dispatch(ctx, txn.TenantID, txn.ID, eventType, payload)
}
