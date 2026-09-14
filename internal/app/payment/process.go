package payment

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/domain/transaction"
	"github.com/crownroutes/payment-service/internal/ports"
)

// ProcessPayment drives a pending transaction through gateway initiation using
// a transaction-scoped lease. Note: the production trigger for gateway calls is
// the outbox relay routing GATEWAY_INITIATE events into ProcessGatewayInitiate
// (which uses TryAcquireDirect). This method is exercised directly by the
// integration suite as the equivalent driver and is kept as the canonical
// Acquire-in-transaction path.
func (s *Service) ProcessPayment(ctx context.Context, transactionID uuid.UUID) (*transaction.Txn, error) {
	txn, err := s.repo.GetByID(ctx, transactionID)
	if err != nil {
		return nil, fmt.Errorf("payment: load transaction %s: %w", transactionID, err)
	}
	if txn.Status != transaction.StatusPending {
		return txn, nil
	}

	adapter, err := s.gateways.Get(txn.GatewayID)
	if err != nil {
		return nil, fmt.Errorf("payment: resolve adapter for %s: %w", txn.GatewayID, err)
	}

	ttlSec := txn.EstimatedTimeoutSeconds
	timeout := time.Duration(ttlSec) * time.Second

	var acquired bool
	err = s.tx.WithinTx(ctx, func(ctx context.Context) error {
		ok, _, err := s.lease.Acquire(ctx, txn.ID, txn.ID, ttlSec)
		if err != nil {
			return err
		}
		acquired = ok
		if !ok {
			// Re-read inside the same transaction for a consistent view
			// after the other instance's lease acquire committed.
			current, err := s.repo.GetByID(ctx, txn.ID)
			if err != nil {
				return err
			}
			*txn = *current
			return nil
		}

		if err := transaction.TransitionState(txn, transaction.StatusProcessing, transaction.ActorSystem); err != nil {
			return err
		}
		now := time.Now().UTC()
		txn.ProcessingStartedAt = &now
		txn.ProcessingTimeout = &timeout
		return s.repo.UpdateStatus(ctx, txn)
	})
	if err != nil {
		return nil, fmt.Errorf("payment: begin processing %s: %w", txn.ID, err)
	}
	if !acquired {
		return txn, nil
	}

	s.enterIntent(ctx, txn.GatewayID, txn.ID, timeout)

	resp, result := s.attemptGateways(ctx, adapter, txn)
	return s.finalize(ctx, txn, resp, result)
}

func (s *Service) RecoverExpiredLease(ctx context.Context, transactionID uuid.UUID) (*transaction.Txn, error) {
	txn, err := s.repo.GetByID(ctx, transactionID)
	if err != nil {
		return nil, fmt.Errorf("payment: load stuck transaction %s: %w", transactionID, err)
	}
	if txn.Status != transaction.StatusProcessing || !txn.IsLeaseExpired() {
		return txn, nil
	}

	gateway := txn.ActualGateway
	if gateway == "" {
		gateway = txn.GatewayID
	}
	adapter, err := s.gateways.Get(gateway)
	if err != nil {
		return nil, fmt.Errorf("payment: resolve adapter for %s: %w", gateway, err)
	}
	txn.GatewayID = gateway

	resp, gwErr := s.checkStatus(ctx, adapter, txn)
	result := resolveOutcome(resp, gwErr)
	s.recordBreaker(ctx, gateway, gwErr)

	if !result.terminal {
		s.log.Info(ports.LogEventTransactionLeaseExpired, map[string]any{
			ports.FieldTransactionID: txn.ID.String(),
			ports.FieldGatewayID:     gateway,
			ports.FieldNewState:      string(txn.Status),
		})
		return txn, nil
	}

	s.log.Info(ports.LogEventTransactionLeaseExpired, map[string]any{
		ports.FieldTransactionID: txn.ID.String(),
		ports.FieldGatewayID:     gateway,
		ports.FieldNewState:      string(result.newStatus),
	})
	return s.finalize(ctx, txn, resp, result)
}
