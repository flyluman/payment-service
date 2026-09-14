package refund

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/app/idempotency"
	"github.com/crownroutes/payment-service/internal/domain/refund"
	"github.com/crownroutes/payment-service/internal/domain/transaction"
	"github.com/crownroutes/payment-service/internal/ports"
)

var ErrNotRefundable = errors.New("refund: transaction is not in a refundable state")

func isRefundableStatus(s transaction.Status) bool {
	switch s {
	case transaction.StatusCaptured, transaction.StatusSettled, transaction.StatusPartiallyRefunded, transaction.StatusRefundFailed:
		return true
	}
	return false
}

type TransactionReader interface {
	GetByID(ctx context.Context, id uuid.UUID) (*transaction.Txn, error)
	UpdateStatus(ctx context.Context, t *transaction.Txn) error
}

type RefundRepo interface {
	LockParentTransaction(ctx context.Context, transactionID uuid.UUID) error
	SumActiveRefunds(ctx context.Context, transactionID uuid.UUID) (int64, error)
	Insert(ctx context.Context, rf *refund.Refund) error
	GetByID(ctx context.Context, id uuid.UUID) (*refund.Refund, error)
	UpdateStatus(ctx context.Context, rf *refund.Refund) error
	ClaimProcessing(ctx context.Context, rf *refund.Refund) (bool, error)
	ListStaleRefunds(ctx context.Context, olderThan time.Duration, maxAttempts, limit int) ([]uuid.UUID, error)
	ExistsByReason(ctx context.Context, transactionID uuid.UUID, reason string) (bool, error)
}

const ReasonCancelResolution = "cancel_resolution"

type EventWriter interface {
	Write(ctx context.Context, event ports.OutboxEvent) error
}

type Transactor interface {
	WithinTx(ctx context.Context, fn func(ctx context.Context) error) error
}

type GatewayRegistry interface {
	Get(gatewayID string) (ports.GatewayAdapter, error)
}

type Service struct {
	txns         TransactionReader
	refunds      RefundRepo
	outbox       EventWriter
	tx           Transactor
	gateways     GatewayRegistry
	idem         *idempotency.Guard
	audit        ports.AuditLogStore
	notif        ports.NotificationDispatcher
	twDispatcher ports.TenantWebhookDispatcher
	log          ports.Logger
	metrics      ports.MetricRecorder
	bus          ports.EventBus
}

func NewService(txns TransactionReader, refunds RefundRepo, outbox EventWriter, tx Transactor, gateways GatewayRegistry, log ports.Logger, metrics ports.MetricRecorder) *Service {
	return &Service{txns: txns, refunds: refunds, outbox: outbox, tx: tx, gateways: gateways, log: log, metrics: metrics}
}

func (s *Service) SetIdempotency(g *idempotency.Guard) { s.idem = g }
func (s *Service) SetAuditLogStore(a ports.AuditLogStore) { s.audit = a }
func (s *Service) SetEventBus(bus ports.EventBus) { s.bus = bus }
func (s *Service) SetNotificationService(n ports.NotificationDispatcher) { s.notif = n }
func (s *Service) SetTenantWebhookDispatcher(d ports.TenantWebhookDispatcher) { s.twDispatcher = d }

type InitiateInput struct {
	TransactionID  uuid.UUID
	Amount         int64
	Reason         string
	InitiatedBy    string
	IdempotencyKey string
}

type InitiateResult struct {
	Verdict idempotency.Verdict
	Refund  *refund.Refund
}

type idempotencyRefundResponse struct {
	RefundID string `json:"refund_id"`
}

type refundInitiatedPayload struct {
	RefundID         string `json:"refund_id"`
	TransactionID    string `json:"transaction_id"`
	TenantID         string `json:"tenant_id"`
	UserID           string `json:"user_id"`
	Amount           int64  `json:"amount"`
	Reason           string `json:"reason"`
	AggregateVersion int    `json:"aggregate_version"`
}

func (s *Service) InitiateRefund(ctx context.Context, in InitiateInput) (InitiateResult, error) {
	parent, err := s.txns.GetByID(ctx, in.TransactionID)
	if err != nil {
		return InitiateResult{}, fmt.Errorf("refund: load transaction %s: %w", in.TransactionID, err)
	}
	if !isRefundableStatus(parent.Status) {
		return InitiateResult{}, ErrNotRefundable
	}

	if s.idem == nil {
		var rf *refund.Refund
		err := s.tx.WithinTx(ctx, func(ctx context.Context) error {
			if err := transaction.TransitionState(parent, transaction.StatusRefundPending, transaction.ActorSystem); err != nil {
				return err
			}
			if err := s.txns.UpdateStatus(ctx, parent); err != nil {
				return err
			}
			r, err := s.insertRefund(ctx, in, parent)
			if err != nil {
				return err
			}
			rf = r
			return nil
		})
		if err != nil {
			return InitiateResult{}, s.mapInitiateError(in, err)
		}
		s.logInitiated(rf, in)
		return InitiateResult{Verdict: idempotency.Created, Refund: rf}, nil
	}

	if in.IdempotencyKey == "" {
		return InitiateResult{}, idempotency.ErrKeyRequired
	}
	composite := idempotency.Composite(parent.TenantID.String(), "initiate_refund", in.IdempotencyKey)
	requestHash := idempotency.RequestHash(
		in.TransactionID.String(), strconv.FormatInt(in.Amount, 10), in.Reason, in.InitiatedBy,
	)

	var created *refund.Refund
	res, err := s.idem.Execute(ctx, composite, requestHash, func(ctx context.Context) ([]byte, error) {
		if err := transaction.TransitionState(parent, transaction.StatusRefundPending, transaction.ActorSystem); err != nil {
			return nil, err
		}
		if err := s.txns.UpdateStatus(ctx, parent); err != nil {
			return nil, err
		}
		r, err := s.insertRefund(ctx, in, parent)
		if err != nil {
			return nil, err
		}
		created = r
		return json.Marshal(idempotencyRefundResponse{RefundID: r.ID.String()})
	})
	if err != nil {
		return InitiateResult{}, s.mapInitiateError(in, err)
	}

	switch res.Verdict {
	case idempotency.Created:
		s.logInitiated(created, in)
		return InitiateResult{Verdict: res.Verdict, Refund: created}, nil
	case idempotency.Replayed:
		var stored idempotencyRefundResponse
		if err := json.Unmarshal(res.Response, &stored); err != nil {
			return InitiateResult{}, fmt.Errorf("refund: decode idempotent response: %w", err)
		}
		id, err := uuid.Parse(stored.RefundID)
		if err != nil {
			return InitiateResult{}, fmt.Errorf("refund: bad stored refund id: %w", err)
		}
		rf, err := s.refunds.GetByID(ctx, id)
		if err != nil {
			return InitiateResult{}, fmt.Errorf("refund: reload idempotent refund %s: %w", id, err)
		}
		return InitiateResult{Verdict: res.Verdict, Refund: rf}, nil
	default:
		return InitiateResult{Verdict: res.Verdict}, nil
	}
}

func (s *Service) insertRefund(ctx context.Context, in InitiateInput, parent *transaction.Txn) (*refund.Refund, error) {
	if err := s.refunds.LockParentTransaction(ctx, in.TransactionID); err != nil {
		return nil, err
	}

	alreadyRefunded, err := s.refunds.SumActiveRefunds(ctx, in.TransactionID)
	if err != nil {
		return nil, err
	}

	r, err := refund.New(in.TransactionID, in.Amount, parent.Amount, alreadyRefunded, in.Reason, in.InitiatedBy)
	if err != nil {
		return nil, err
	}
	r.AttemptedGateway = parent.GatewayID

	if err := s.refunds.Insert(ctx, r); err != nil {
		return nil, fmt.Errorf("insert refund: %w", err)
	}

	payload, err := json.Marshal(refundInitiatedPayload{
		RefundID:         r.ID.String(),
		TransactionID:    in.TransactionID.String(),
		TenantID:         parent.TenantID.String(),
		UserID:           parent.UserID.String(),
		Amount:           r.Amount,
		Reason:           r.Reason,
		AggregateVersion: r.Version,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal refund event: %w", err)
	}
	if err := s.outbox.Write(ctx, ports.OutboxEvent{
		AggregateID:      r.ID,
		AggregateType:    "refund",
		EventType:        ports.EventTypeRefundInitiated,
		Payload:          payload,
		EventVersion:     1,
		AggregateVersion: r.Version,
	}); err != nil {
		return nil, fmt.Errorf("write refund event: %w", err)
	}
	if s.audit != nil {
		_ = s.audit.WriteEntry(ctx, &ports.AuditEntry{
			TransactionID: &r.TransactionID,
			EventType:     ports.AuditEventTypeRefundInitiated,
			Actor:         in.InitiatedBy,
			NewState:      string(r.Status),
			Reason:        in.Reason,
		})
	}
	return r, nil
}

func (s *Service) mapInitiateError(in InitiateInput, err error) error {
	var over refund.ErrOverRefund
	if errors.As(err, &over) {
		s.metrics.Increment(ports.MetricRefundDuplicationBlocked, map[string]string{"reason": "over_refund"})
		s.log.Warn(ports.LogEventRefundOverRefundBlocked, map[string]any{
			ports.FieldTransactionID: in.TransactionID.String(),
		})
		return over
	}
	return fmt.Errorf("refund: initiate for %s: %w", in.TransactionID, err)
}

func (s *Service) logInitiated(rf *refund.Refund, in InitiateInput) {
	s.log.Info(ports.LogEventRefundInitiated, map[string]any{
		ports.FieldRefundID:      rf.ID.String(),
		ports.FieldTransactionID: in.TransactionID.String(),
	})
	s.metrics.Increment(ports.MetricRefundInitiated, map[string]string{"gateway_id": rf.AttemptedGateway})
}
