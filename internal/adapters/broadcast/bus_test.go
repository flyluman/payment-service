package broadcast

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/domain/transaction"
	"github.com/crownroutes/payment-service/internal/ports"
)

func TestPublish_DeliversToSubscriber(t *testing.T) {
	bus := NewInMemoryBus()
	txnID := uuid.New()

	ch := bus.Subscribe(txnID)
	defer bus.Unsubscribe(txnID, ch)

	ev := ports.StatusEvent{TransactionID: txnID, Status: transaction.StatusSucceeded}
	bus.Publish(context.Background(), ev)

	select {
	case got := <-ch:
		if got.Status != transaction.StatusSucceeded {
			t.Fatalf("expected status %s, got %s", transaction.StatusSucceeded, got.Status)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event")
	}
}

func TestPublish_IgnoresDifferentTransaction(t *testing.T) {
	bus := NewInMemoryBus()
	txnID := uuid.New()
	otherID := uuid.New()

	ch := bus.Subscribe(txnID)
	defer bus.Unsubscribe(txnID, ch)

	bus.Publish(context.Background(), ports.StatusEvent{TransactionID: otherID, Status: transaction.StatusSucceeded})

	select {
	case <-ch:
		t.Fatal("should not have received event for different transaction")
	case <-time.After(50 * time.Millisecond):
		// expected
	}
}

func TestPublish_NonBlockingWhenFull(t *testing.T) {
	bus := NewInMemoryBus()
	txnID := uuid.New()

	ch := bus.Subscribe(txnID)
	defer bus.Unsubscribe(txnID, ch)

	bus.Publish(context.Background(), ports.StatusEvent{TransactionID: txnID, Status: transaction.StatusPending})
	bus.Publish(context.Background(), ports.StatusEvent{TransactionID: txnID, Status: transaction.StatusSucceeded})

	select {
	case got := <-ch:
		if got.Status != transaction.StatusPending {
			t.Fatalf("expected first event (PENDING), got %s", got.Status)
		}
	default:
		t.Fatal("first event should be available")
	}
}

func TestUnsubscribe_StopsDelivery(t *testing.T) {
	bus := NewInMemoryBus()
	txnID := uuid.New()

	ch := bus.Subscribe(txnID)
	bus.Unsubscribe(txnID, ch)

	bus.Publish(context.Background(), ports.StatusEvent{TransactionID: txnID, Status: transaction.StatusSucceeded})

	select {
	case <-ch:
		t.Fatal("should not receive after unsubscribe")
	case <-time.After(50 * time.Millisecond):
		// expected
	}
}

func TestUnsubscribe_CleansUpEmptyMap(t *testing.T) {
	bus := NewInMemoryBus()
	txnID := uuid.New()

	ch := bus.Subscribe(txnID)
	bus.Unsubscribe(txnID, ch)

	bus.mu.RLock()
	_, exists := bus.subs[txnID]
	bus.mu.RUnlock()

	if exists {
		t.Fatal("expected transaction key to be cleaned up")
	}
}

func TestMultipleSubscribers(t *testing.T) {
	bus := NewInMemoryBus()
	txnID := uuid.New()

	ch1 := bus.Subscribe(txnID)
	ch2 := bus.Subscribe(txnID)
	defer bus.Unsubscribe(txnID, ch1)
	defer bus.Unsubscribe(txnID, ch2)

	ev := ports.StatusEvent{TransactionID: txnID, Status: transaction.StatusProcessing}
	bus.Publish(context.Background(), ev)

	for i, ch := range []chan ports.StatusEvent{ch1, ch2} {
		select {
		case got := <-ch:
			if got.Status != transaction.StatusProcessing {
				t.Fatalf("subscriber %d: expected PROCESSING, got %s", i, got.Status)
			}
		case <-time.After(time.Second):
			t.Fatalf("subscriber %d: timed out", i)
		}
	}
}

func TestConcurrentAccess(t *testing.T) {
	bus := NewInMemoryBus()
	txnID := uuid.New()
	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ch := bus.Subscribe(txnID)
			bus.Publish(context.Background(), ports.StatusEvent{TransactionID: txnID, Status: transaction.StatusPending})
			<-ch
			bus.Unsubscribe(txnID, ch)
		}()
	}
	wg.Wait()
}
