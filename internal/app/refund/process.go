package refund

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/domain/notification"
	"github.com/crownroutes/payment-service/internal/domain/refund"
	"github.com/crownroutes/payment-service/internal/domain/transaction"
	"github.com/crownroutes/payment-service/internal/ports"
)

type refundTerminalPayload struct {
	RefundID         string `json:"refund_id"`
	TransactionID    string `json:"transaction_id"`
	Status           string `json:"status"`
	GatewayRefundID  string `json:"gateway_refund_id"`
	Amount           int64  `json:"amount"`
	AggregateVersion int    `json:"aggregate_version"`
}

type refundOutcome struct {
	terminal      bool
	newStatus     refund.Status
	failureReason *refund.FailureReason
}

// ProcessRefund drives a newly initiated refund through the gateway. It is
// called by the relay on REFUND_INITIATED events and is safe against event
// redelivery: a refund already past REFUND_INITIATED is returned untouched.
func (s *Service) ProcessRefund(ctx context.Context, refundID uuid.UUID) (*refund.Refund, error) {
	rf, err := s.refunds.GetByID(ctx, refundID)
	if err != nil {
		return nil, fmt.Errorf("refund: load refund %s: %w", refundID, err)
	}
	if rf.Status != refund.StatusInitiated {
		return rf, nil
	}
	return s.runRefund(ctx, rf)
}

// RetryStaleRefund re-drives a refund the relay left in REFUND_PROCESSING
// (ambiguous or timed-out gateway response). Called by the refund reaper.
func (s *Service) RetryStaleRefund(ctx context.Context, refundID uuid.UUID) (*refund.Refund, error) {
	rf, err := s.refunds.GetByID(ctx, refundID)
	if err != nil {
		return nil, fmt.Errorf("refund: load refund %s: %w", refundID, err)
	}
	if rf.Status != refund.StatusProcessing {
		return rf, nil
	}
	return s.runRefund(ctx, rf)
}

func (s *Service) runRefund(ctx context.Context, rf *refund.Refund) (*refund.Refund, error) {
	parent, err := s.txns.GetByID(ctx, rf.TransactionID)
	if err != nil {
		return nil, fmt.Errorf("refund: load parent %s: %w", rf.TransactionID, err)
	}

	adapter, err := s.gateways.Get(rf.AttemptedGateway)
	if err != nil {
		return nil, fmt.Errorf("refund: resolve adapter for %s: %w", rf.AttemptedGateway, err)
	}

	// Atomically claim the refund. Only one caller (relay or reaper) wins; the
	// loser reloads and returns the current state without touching the gateway.
	claimed, err := s.refunds.ClaimProcessing(ctx, rf)
	if err != nil {
		return nil, fmt.Errorf("refund: claim %s: %w", rf.ID, err)
	}
	if !claimed {
		rf, err := s.refunds.GetByID(ctx, rf.ID)
		if err != nil {
			return nil, fmt.Errorf("refund: reload claimed refund %s: %w", rf.ID, err)
		}
		return rf, nil
	}

	resp, gwErr := s.callGateway(ctx, adapter, rf, parent)
	outcome := resolveRefundOutcome(resp, gwErr)

	if resp != nil {
		rf.GatewayRefundID = resp.GatewayRefundID
	}
	rf.ActualGateway = rf.AttemptedGateway

	if !outcome.terminal {
		if err := s.tx.WithinTx(ctx, func(ctx context.Context) error {
			return s.refunds.UpdateStatus(ctx, rf)
		}); err != nil {
			return nil, fmt.Errorf("refund: persist in-flight %s: %w", rf.ID, err)
		}
		return rf, nil
	}

	if err := s.tx.WithinTx(ctx, func(ctx context.Context) error {
		if err := rf.Transition(outcome.newStatus); err != nil {
			return err
		}
		rf.FailureReason = outcome.failureReason
		if err := s.refunds.UpdateStatus(ctx, rf); err != nil {
			return err
		}

		if err := s.transitionParentOnRefundResult(ctx, parent, outcome); err != nil {
			return err
		}
		if err := s.txns.UpdateStatus(ctx, parent); err != nil {
			return err
		}

		event, err := s.buildTerminalEvent(rf, outcome.newStatus)
		if err != nil {
			return err
		}
		if err := s.outbox.Write(ctx, event); err != nil {
			return err
		}

		s.dispatchTerminalNotification(ctx, parent, rf, outcome)
		s.dispatchTenantWebhook(ctx, parent, rf, outcome)

		if s.bus != nil {
			var txnStatus transaction.Status
			switch outcome.newStatus {
			case refund.StatusRefunded:
				txnStatus = transaction.StatusRefunded
			case refund.StatusFailed:
				txnStatus = transaction.StatusRefundFailed
			}
			s.bus.Publish(ctx, ports.StatusEvent{
				TransactionID: parent.ID,
				Status:        txnStatus,
			})
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("refund: finalize %s: %w", rf.ID, err)
	}

	if outcome.newStatus == refund.StatusRefunded {
		s.log.Info(ports.LogEventRefundSucceeded, map[string]any{ports.FieldRefundID: rf.ID.String()})
		s.metrics.Increment(ports.MetricRefundSucceeded, map[string]string{"gateway_id": rf.ActualGateway})
	} else {
		s.log.Info(ports.LogEventRefundFailed, map[string]any{ports.FieldRefundID: rf.ID.String()})
		s.metrics.Increment(ports.MetricRefundFailed, map[string]string{"gateway_id": rf.ActualGateway})
	}

	if s.audit != nil {
		_ = s.audit.WriteEntry(ctx, &ports.AuditEntry{
			TransactionID: &rf.TransactionID,
			EventType:     ports.AuditEventTypeStateChange,
			Actor:         string(transaction.ActorSystem),
			PreviousState: string(refund.StatusProcessing),
			NewState:      string(outcome.newStatus),
			Reason:        "refund_processed",
		})
	}
	return rf, nil
}

func (s *Service) transitionParentOnRefundResult(ctx context.Context, parent *transaction.Txn, outcome refundOutcome) error {
	if outcome.newStatus == refund.StatusRefunded {
		alreadyRefunded, err := s.refunds.SumActiveRefunds(ctx, parent.ID)
		if err != nil {
			return fmt.Errorf("refund: sum active refunds for parent %s: %w", parent.ID, err)
		}
		if alreadyRefunded >= parent.Amount {
			return transaction.TransitionState(parent, transaction.StatusRefunded, transaction.ActorSystem)
		}
		return transaction.TransitionState(parent, transaction.StatusPartiallyRefunded, transaction.ActorSystem)
	}

	// Failure: only move the parent to REFUND_FAILED when no other refund
	// activity is outstanding; otherwise it must stay in-flight so the other
	// refunds can still resolve.
	alreadyRefunded, err := s.refunds.SumActiveRefunds(ctx, parent.ID)
	if err != nil {
		return fmt.Errorf("refund: sum active refunds for parent %s: %w", parent.ID, err)
	}
	if alreadyRefunded > 0 {
		return nil
	}
	return transaction.TransitionState(parent, transaction.StatusRefundFailed, transaction.ActorSystem)
}

func (s *Service) ResolveCancelRefund(ctx context.Context, transactionID uuid.UUID, amount int64) error {
	exists, err := s.refunds.ExistsByReason(ctx, transactionID, ReasonCancelResolution)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}

	res, err := s.InitiateRefund(ctx, InitiateInput{
		TransactionID:  transactionID,
		Amount:         amount,
		Reason:         ReasonCancelResolution,
		InitiatedBy:    "system:cancel_resolution",
		IdempotencyKey: "cancel-resolution:" + transactionID.String(),
	})
	if err != nil {
		var over refund.ErrOverRefund
		if errors.As(err, &over) {
			return nil
		}
		return fmt.Errorf("refund: cancel resolution for %s: %w", transactionID, err)
	}
	if res.Refund == nil {
		return nil
	}

	if _, err := s.ProcessRefund(ctx, res.Refund.ID); err != nil {
		return fmt.Errorf("refund: process cancel resolution %s: %w", res.Refund.ID, err)
	}
	return nil
}

func (s *Service) callGateway(ctx context.Context, adapter ports.GatewayAdapter, rf *refund.Refund, parent *transaction.Txn) (*ports.GatewayRefundResponse, *ports.GatewayError) {
	// Refunds must be issued in the same currency/amount the gateway actually
	// charged (fee-inclusive gateway_amount), not the nominal transaction
	// amount, or gateways reject the refund for exceeding the capture.
	// Partial refunds are prorated: the gateway charge includes fees on top of
	// the nominal amount, so refunding rf.Amount of a captured gateway_amount
	// requires the proportionate share of the fee-inclusive amount.
	gwAmount := rf.Amount
	gwCurrency := parent.Currency
	if parent.GatewayAmount != nil {
		gwAmount = (*parent.GatewayAmount * rf.Amount) / parent.Amount
		gwCurrency = parent.GatewayCurrency
	}
	resp, err := adapter.Refund(ctx, ports.GatewayRefundRequest{
		RefundID:           rf.ID,
		TransactionID:      rf.TransactionID,
		TenantID:           parent.TenantID,
		GatewayReferenceID: parent.GatewayReferenceID,
		Amount:             gwAmount,
		Currency:           gwCurrency,
		Reason:             rf.Reason,
	})
	if err != nil {
		var gwErr *ports.GatewayError
		if errors.As(err, &gwErr) {
			return nil, gwErr
		}
		return nil, &ports.GatewayError{
			Category:       ports.ErrorCategoryGatewayError,
			Code:           "refund_call_failed",
			GatewayMessage: err.Error(),
			Underlying:     err,
		}
	}
	return resp, nil
}

func resolveRefundOutcome(resp *ports.GatewayRefundResponse, gwErr *ports.GatewayError) refundOutcome {
	if gwErr != nil {
		switch gwErr.Category {
		case ports.ErrorCategoryAmbiguous, ports.ErrorCategoryNetworkTimeout, ports.ErrorCategoryGatewayError:
			return refundOutcome{terminal: false}
		}
		return refundOutcome{
			terminal:  true,
			newStatus: refund.StatusFailed,
			failureReason: &refund.FailureReason{
				Category:       string(gwErr.Category),
				Code:           gwErr.Code,
				GatewayCode:    gwErr.GatewayCode,
				GatewayMessage: gwErr.GatewayMessage,
				Source:         refund.FailureReasonSourceGateway,
			},
		}
	}

	switch resp.Status {
	case ports.GatewayRefundStatusCompleted:
		return refundOutcome{terminal: true, newStatus: refund.StatusRefunded}
	case ports.GatewayRefundStatusFailed:
		return refundOutcome{
			terminal:  true,
			newStatus: refund.StatusFailed,
			failureReason: &refund.FailureReason{
				Category: "gateway_refund_failed",
				Source:   refund.FailureReasonSourceGateway,
			},
		}
	default:
		return refundOutcome{terminal: false}
	}
}

func (s *Service) buildTerminalEvent(rf *refund.Refund, status refund.Status) (ports.OutboxEvent, error) {
	eventType := ports.EventTypeRefundFailed
	if status == refund.StatusRefunded {
		eventType = ports.EventTypeRefundSucceeded
	}

	payload, err := json.Marshal(refundTerminalPayload{
		RefundID:         rf.ID.String(),
		TransactionID:    rf.TransactionID.String(),
		Status:           string(status),
		GatewayRefundID:  rf.GatewayRefundID,
		Amount:           rf.Amount,
		AggregateVersion: rf.Version,
	})
	if err != nil {
		return ports.OutboxEvent{}, fmt.Errorf("refund: marshal terminal event: %w", err)
	}

	return ports.OutboxEvent{
		AggregateID:      rf.ID,
		AggregateType:    "refund",
		EventType:        eventType,
		Payload:          payload,
		EventVersion:     1,
		AggregateVersion: rf.Version,
	}, nil
}

func (s *Service) dispatchTerminalNotification(ctx context.Context, parent *transaction.Txn, rf *refund.Refund, outcome refundOutcome) {
	if s.notif == nil || parent.CustomerEmail == "" {
		return
	}

	data := map[string]any{
		"amount":         rf.Amount,
		"currency":       parent.Currency,
		"transaction_id": parent.ID.String(),
	}

	switch outcome.newStatus {
	case refund.StatusRefunded:
		s.notif.Dispatch(ctx, &notification.Notification{
			TenantID:     parent.TenantID,
			UserID:       &parent.UserID,
			Type:         notification.RefundCompleted,
			Channel:      notification.ChannelEmail,
			Recipient:    parent.CustomerEmail,
			TemplateName: "REFUND_COMPLETED",
			TemplateData: data,
		})
	case refund.StatusFailed:
		reason := ""
		if outcome.failureReason != nil {
			reason = outcome.failureReason.GatewayMessage
		}
		data["reason"] = reason
		s.notif.Dispatch(ctx, &notification.Notification{
			TenantID:     parent.TenantID,
			UserID:       &parent.UserID,
			Type:         notification.RefundFailed,
			Channel:      notification.ChannelEmail,
			Recipient:    parent.CustomerEmail,
			TemplateName: "REFUND_FAILED",
			TemplateData: data,
		})
	}
}

func (s *Service) dispatchTenantWebhook(ctx context.Context, parent *transaction.Txn, rf *refund.Refund, outcome refundOutcome) {
	if s.twDispatcher == nil {
		return
	}

	var eventType string
	switch outcome.newStatus {
	case refund.StatusRefunded:
		eventType = ports.EventTypeRefundSucceeded
	case refund.StatusFailed:
		eventType = ports.EventTypeRefundFailed
	default:
		return
	}

	payload, err := json.Marshal(map[string]any{
		"refund_id":      rf.ID.String(),
		"transaction_id": parent.ID.String(),
		"status":         string(outcome.newStatus),
		"amount":         rf.Amount,
	})
	if err != nil {
		return
	}

	_ = s.twDispatcher.Dispatch(ctx, parent.TenantID, parent.ID, eventType, payload)
}
