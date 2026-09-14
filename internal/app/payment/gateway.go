package payment

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/domain/notification"
	"github.com/crownroutes/payment-service/internal/domain/transaction"
	"github.com/crownroutes/payment-service/internal/ports"
)

type transactionTerminalPayload struct {
	TransactionID      string `json:"transaction_id"`
	Status             string `json:"status"`
	Gateway            string `json:"gateway"`
	GatewayReferenceID string `json:"gateway_reference_id"`
	Amount             int64  `json:"amount"`
	AggregateVersion   int    `json:"aggregate_version"`
}

type cachedResponse struct {
	Status             string `json:"status"`
	GatewayReferenceID string `json:"gateway_reference_id"`
}

type outcome struct {
	terminal      bool
	newStatus     transaction.Status
	failureReason *transaction.FailureReason
}

func (s *Service) checkStatus(ctx context.Context, adapter ports.GatewayAdapter, txn *transaction.Txn) (*ports.GatewayPaymentResponse, *ports.GatewayError) {
	resp, err := adapter.CheckStatus(ctx, ports.GatewayStatusRequest{
		TransactionID:      txn.ID,
		GatewayReferenceID: txn.GatewayReferenceID,
		IdempotencyKey:     txn.GatewayIdempotencyKey,
	})
	if err != nil {
		var gwErr *ports.GatewayError
		if errors.As(err, &gwErr) {
			return nil, gwErr
		}
		return nil, &ports.GatewayError{
			Category:       ports.ErrorCategoryGatewayError,
			Code:           "status_check_failed",
			GatewayMessage: err.Error(),
			Underlying:     err,
		}
	}
	return resp, nil
}

func (s *Service) finalize(ctx context.Context, txn *transaction.Txn, resp *ports.GatewayPaymentResponse, result outcome) (*transaction.Txn, error) {
	if resp != nil {
		txn.GatewayReferenceID = resp.GatewayReferenceID
		if md := toMethodDetails(resp.MethodResponse); md != nil {
			txn.MethodDetails = md
		}
	}
	txn.ActualGateway = txn.GatewayID

	cached, err := json.Marshal(cachedResponse{
		Status:             string(result.newStatusOr(txn.Status)),
		GatewayReferenceID: txn.GatewayReferenceID,
	})
	if err != nil {
		return nil, fmt.Errorf("payment: marshal cached response: %w", err)
	}

	err = s.tx.WithinTx(ctx, func(ctx context.Context) error {
		if result.terminal {
			if err := transaction.TransitionState(txn, result.newStatus, transaction.ActorGateway); err != nil {
				return err
			}
			txn.FailureReason = result.failureReason
			if s.audit != nil {
				_ = s.audit.WriteEntry(ctx, &ports.AuditEntry{
					TransactionID: &txn.ID,
					EventType:     ports.AuditEventTypeStateChange,
					Actor:         string(transaction.ActorGateway),
					PreviousState: string(transaction.StatusProcessing),
					NewState:      string(result.newStatus),
					Reason:        "gateway_finalize",
				})
			}
		}
		if err := s.repo.UpdateStatus(ctx, txn); err != nil {
			return err
		}
		if result.terminal {
			event, err := s.buildTerminalEvent(txn, result.newStatus)
			if err != nil {
				return err
			}
			if err := s.outbox.Write(ctx, *event); err != nil {
				return err
			}
			if txn.CallbackURL != "" {
				cbPayload, err := json.Marshal(map[string]any{
					"transaction_id": txn.ID.String(),
					"status":         string(result.newStatus),
					"callback_url":   txn.CallbackURL,
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
			s.dispatchTerminalNotification(ctx, txn, result.newStatus, result.failureReason)
			s.dispatchTenantWebhook(ctx, txn, result.newStatus)
		}
		return s.lease.WriteCachedResponse(ctx, txn.ID, cached)
	})
	if err != nil {
		s.log.Error(ports.LogEventGatewayResponse, map[string]any{
			ports.FieldErrorCode:     "payment_finalize_failed",
			ports.FieldTransactionID: txn.ID.String(),
			ports.FieldGatewayID:     txn.GatewayID,
		}, err)
		return nil, fmt.Errorf("payment: finalize %s: %w", txn.ID, err)
	}

	s.recordOutcome(txn, result)
	if result.terminal {
		if s.bus != nil {
			s.bus.Publish(ctx, ports.StatusEvent{
				TransactionID: txn.ID,
				Status:        result.newStatus,
			})
		}
		s.exitIntent(ctx, txn.GatewayID, txn.ID)
	}
	s.resolveCancelIfRequested(ctx, txn, result)
	return txn, nil
}

func (s *Service) resolveCancelIfRequested(ctx context.Context, txn *transaction.Txn, result outcome) {
	if !result.terminal || result.newStatus != transaction.StatusCaptured {
		return
	}
	if !txn.CancelIntent || s.cancelResolver == nil {
		return
	}
	if err := s.cancelResolver.ResolveCancelRefund(ctx, txn.ID, txn.Amount); err != nil {
		s.log.Error(ports.LogEventCancelResolution, map[string]any{
			ports.FieldErrorCode:     "cancel_resolution_failed",
			ports.FieldTraceID:       "",
			ports.FieldTransactionID: txn.ID.String(),
		}, err)
		return
	}
	s.log.Info(ports.LogEventCancelResolution, map[string]any{ports.FieldTransactionID: txn.ID.String()})
}

func (s *Service) attemptGateways(ctx context.Context, adapter ports.GatewayAdapter, txn *transaction.Txn) (*ports.GatewayPaymentResponse, outcome) {
	var gwErr *ports.GatewayError
	resp, gwErr := s.callGateway(ctx, adapter, txn)
	result := resolveOutcome(resp, gwErr)
	s.recordBreaker(ctx, txn.GatewayID, gwErr)
	return resp, result
}

func (s *Service) recordBreaker(ctx context.Context, gatewayID string, gwErr *ports.GatewayError) {
	if s.breaker == nil {
		return
	}
	var err error
	if isGatewayHealthFailure(gwErr) {
		err = s.breaker.RecordFailure(ctx, gatewayID)
	} else {
		err = s.breaker.RecordSuccess(ctx, gatewayID)
	}
	if err != nil {
		s.log.Warn(ports.LogEventGatewayCircuitOpen, map[string]any{
			ports.FieldGatewayID: gatewayID,
			"error":              err.Error(),
		})
	}
}

func (s *Service) enterIntent(ctx context.Context, gatewayID string, txnID uuid.UUID, ttl time.Duration) {
	if s.intents == nil {
		return
	}
	if err := s.intents.EnterProcessing(ctx, gatewayID, txnID, ttl); err != nil {
		s.log.Warn(ports.LogEventGatewayResponse, map[string]any{
			ports.FieldTransactionID: txnID.String(),
			ports.FieldGatewayID:     gatewayID,
			"error":                  err.Error(),
			"stage":                  "intent_enter",
		})
	}
}

func (s *Service) exitIntent(ctx context.Context, gatewayID string, txnID uuid.UUID) {
	if s.intents == nil {
		return
	}
	if err := s.intents.ExitProcessing(ctx, gatewayID, txnID); err != nil {
		s.log.Warn(ports.LogEventGatewayResponse, map[string]any{
			ports.FieldTransactionID: txnID.String(),
			ports.FieldGatewayID:     gatewayID,
			"error":                  err.Error(),
			"stage":                  "intent_exit",
		})
	}
}

func isGatewayHealthFailure(gwErr *ports.GatewayError) bool {
	if gwErr == nil {
		return false
	}
	switch gwErr.Category {
	case ports.ErrorCategoryNetworkTimeout, ports.ErrorCategoryGatewayError, ports.ErrorCategoryAmbiguous:
		return true
	default:
		return false
	}
}

func (s *Service) callGateway(ctx context.Context, adapter ports.GatewayAdapter, txn *transaction.Txn) (*ports.GatewayPaymentResponse, *ports.GatewayError) {
	resp, err := adapter.InitiatePayment(ctx, ports.GatewayPaymentRequest{
		TransactionID:  txn.ID,
		TenantID:       txn.TenantID,
		Amount:         txn.Amount,
		Currency:       txn.Currency,
		PaymentMethod:  txn.PaymentMethod,
		IdempotencyKey: txn.GatewayIdempotencyKey,
		Metadata:       txn.Metadata,
		CustomerEmail:  txn.CustomerEmail,
		Description:    txn.Description,
		AttemptNumber:  1,
	})
	if err != nil {
		var gwErr *ports.GatewayError
		if errors.As(err, &gwErr) {
			return nil, gwErr
		}
		return nil, &ports.GatewayError{
			Category:       ports.ErrorCategoryGatewayError,
			Code:           "gateway_call_failed",
			GatewayMessage: err.Error(),
			Underlying:     err,
		}
	}
	return resp, nil
}

func resolveOutcome(resp *ports.GatewayPaymentResponse, gwErr *ports.GatewayError) outcome {
	if gwErr != nil {
		if gwErr.Category == ports.ErrorCategoryAmbiguous || gwErr.Category == ports.ErrorCategoryNetworkTimeout {
			return outcome{terminal: false}
		}
		return outcome{
			terminal:  true,
			newStatus: transaction.StatusFailed,
			failureReason: &transaction.FailureReason{
				Category:       string(gwErr.Category),
				Code:           gwErr.Code,
				GatewayCode:    gwErr.GatewayCode,
				GatewayMessage: gwErr.GatewayMessage,
				Source:         transaction.FailureReasonSourceGateway,
			},
		}
	}

	switch resp.Status {
	case ports.GatewayPaymentStatusSucceeded:
		return outcome{terminal: true, newStatus: transaction.StatusCaptured}
	case ports.GatewayPaymentStatusAuthorized:
		return outcome{terminal: true, newStatus: transaction.StatusAuthorized}
	case ports.GatewayPaymentStatusFailed:
		return outcome{
			terminal:  true,
			newStatus: transaction.StatusFailed,
			failureReason: &transaction.FailureReason{
				Category:       "gateway_declined",
				Code:           resp.ErrorCode,
				GatewayCode:    resp.ErrorCode,
				GatewayMessage: resp.ErrorMessage,
				Source:         transaction.FailureReasonSourceGateway,
			},
		}
	default:
		return outcome{terminal: false}
	}
}

func (o outcome) newStatusOr(current transaction.Status) transaction.Status {
	if o.terminal {
		return o.newStatus
	}
	return current
}

func (s *Service) buildTerminalEvent(txn *transaction.Txn, status transaction.Status) (*ports.OutboxEvent, error) {
	eventType := ports.EventTypeTransactionFailed
	switch status {
	case transaction.StatusCaptured:
		eventType = ports.EventTypeTransactionCaptured
	case transaction.StatusCancelled:
		eventType = ports.EventTypeTransactionCancelled
	}

	payload, err := json.Marshal(transactionTerminalPayload{
		TransactionID:      txn.ID.String(),
		Status:             string(status),
		Gateway:            txn.ActualGateway,
		GatewayReferenceID: txn.GatewayReferenceID,
		Amount:             txn.Amount,
		AggregateVersion:   txn.Version,
	})
	if err != nil {
		return nil, fmt.Errorf("payment: marshal terminal event: %w", err)
	}

	return &ports.OutboxEvent{
		AggregateID:      txn.ID,
		AggregateType:    "transaction",
		EventType:        eventType,
		Payload:          payload,
		EventVersion:     1,
		AggregateVersion: txn.Version,
	}, nil
}

func (s *Service) recordOutcome(txn *transaction.Txn, result outcome) {
	tags := map[string]string{
		"gateway_id":     txn.ActualGateway,
		"payment_method": string(txn.PaymentMethod),
	}
	switch {
	case !result.terminal:
		s.log.Info(ports.LogEventGatewayResponse, map[string]any{
			ports.FieldTransactionID: txn.ID.String(),
			ports.FieldGatewayID:     txn.ActualGateway,
			ports.FieldNewState:      string(txn.Status),
		})
	case result.newStatus == transaction.StatusCaptured:
		s.log.Info(ports.LogEventTransactionTransition, map[string]any{
			ports.FieldTransactionID: txn.ID.String(),
			ports.FieldNewState:      string(transaction.StatusCaptured),
		})
		s.metrics.Increment(ports.MetricTransactionSucceeded, tags)
	default:
		s.log.Info(ports.LogEventTransactionTransition, map[string]any{
			ports.FieldTransactionID: txn.ID.String(),
			ports.FieldNewState:      string(transaction.StatusFailed),
		})
		s.metrics.Increment(ports.MetricTransactionFailed, tags)
	}
}

func toMethodDetails(mr ports.GatewayMethodResponse) *transaction.MethodDetails {
	switch v := mr.(type) {
	case *ports.GatewayCardResponse:
		return &transaction.MethodDetails{Card: &transaction.CardDetails{
			CardBrand: v.CardBrand,
			Last4:     v.Last4,
			Network:   v.Network,
			RiskScore: v.RiskScore,
			AuthCode:  v.AuthCode,
		}}
	case *ports.GatewayUPIResponse:
		return &transaction.MethodDetails{UPI: &transaction.UPIDetails{
			VPA:              v.VPA,
			UPITransactionID: v.UPITransactionID,
			PayerBank:        v.PayerBank,
		}}
	case *ports.GatewayNetbankingResponse:
		return &transaction.MethodDetails{Netbanking: &transaction.NetbankingDetails{
			BankCode:        v.BankCode,
			BankReferenceID: v.BankReferenceID,
		}}
	case *ports.GatewayWalletResponse:
		return &transaction.MethodDetails{Wallet: &transaction.WalletDetails{
			WalletProvider:      v.WalletProvider,
			WalletTransactionID: v.WalletTransactionID,
		}}
	default:
		return nil
	}
}

func (s *Service) dispatchTerminalNotification(ctx context.Context, txn *transaction.Txn, status transaction.Status, failureReason *transaction.FailureReason) {
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
		if failureReason != nil {
			data["reason"] = failureReason.GatewayMessage
		}
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
