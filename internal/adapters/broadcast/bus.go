package broadcast

import (
	"context"
	"sync"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/ports"
)

type InMemoryBus struct {
	mu   sync.RWMutex
	subs map[uuid.UUID]map[chan ports.StatusEvent]struct{}
}

func NewInMemoryBus() *InMemoryBus {
	return &InMemoryBus{
		subs: make(map[uuid.UUID]map[chan ports.StatusEvent]struct{}),
	}
}

func (b *InMemoryBus) Publish(_ context.Context, event ports.StatusEvent) {
	b.mu.RLock()
	chans := b.subs[event.TransactionID]
	b.mu.RUnlock()
	for ch := range chans {
		select {
		case ch <- event:
		default:
		}
	}
}

func (b *InMemoryBus) Subscribe(transactionID uuid.UUID) chan ports.StatusEvent {
	ch := make(chan ports.StatusEvent, 1)
	b.mu.Lock()
	if b.subs[transactionID] == nil {
		b.subs[transactionID] = make(map[chan ports.StatusEvent]struct{})
	}
	b.subs[transactionID][ch] = struct{}{}
	b.mu.Unlock()
	return ch
}

func (b *InMemoryBus) Unsubscribe(transactionID uuid.UUID, ch chan ports.StatusEvent) {
	b.mu.Lock()
	delete(b.subs[transactionID], ch)
	if len(b.subs[transactionID]) == 0 {
		delete(b.subs, transactionID)
	}
	b.mu.Unlock()
}
