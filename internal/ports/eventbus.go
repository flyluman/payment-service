package ports

import (
	"context"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/domain/transaction"
)

type StatusEvent struct {
	TransactionID uuid.UUID
	Status        transaction.Status
}

type EventBus interface {
	Publish(ctx context.Context, event StatusEvent)
	Subscribe(transactionID uuid.UUID) chan StatusEvent
	Unsubscribe(transactionID uuid.UUID, ch chan StatusEvent)
}
