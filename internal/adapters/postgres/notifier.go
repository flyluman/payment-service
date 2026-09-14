package postgres

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

const reconnectDelay = 3 * time.Second

// OutboxNotifier listens for PostgreSQL NOTIFY events on the outbox_insert
// channel and sends wakeup signals when new outbox events arrive.
// Uses a dedicated connection (not pooled) so the LISTEN subscription persists.
type OutboxNotifier struct {
	dsn    string
	wakeup chan struct{}
	close  chan struct{}
	wg     sync.WaitGroup
}

func NewOutboxNotifier(dsn string) *OutboxNotifier {
	return &OutboxNotifier{
		dsn:    dsn,
		wakeup: make(chan struct{}, 64),
		close:  make(chan struct{}),
	}
}

// Wakeup returns a channel that receives a signal when a new outbox event is
// inserted on any shard.
func (n *OutboxNotifier) Wakeup() <-chan struct{} {
	return n.wakeup
}

// Start begins listening for notifications in a background goroutine.
// It reconnects automatically on connection drops until Close is called.
func (n *OutboxNotifier) Start(ctx context.Context) {
	n.wg.Add(1)
	go n.loop(ctx)
}

// Close stops the listener and waits for the goroutine to exit.
func (n *OutboxNotifier) Close() {
	close(n.close)
	n.wg.Wait()
}

func (n *OutboxNotifier) loop(ctx context.Context) {
	defer n.wg.Done()

	for {
		select {
		case <-n.close:
			return
		default:
		}

		if err := n.listen(ctx); err != nil {
			select {
			case <-n.close:
				return
			case <-time.After(reconnectDelay):
			}
		}
	}
}

func (n *OutboxNotifier) listen(ctx context.Context) error {
	conn, err := pgx.Connect(ctx, n.dsn)
	if err != nil {
		return fmt.Errorf("notifier: connect: %w", err)
	}
	defer conn.Close(ctx)

	if _, err := conn.Exec(ctx, "LISTEN outbox_insert"); err != nil {
		return fmt.Errorf("notifier: listen: %w", err)
	}

	for {
		notif, err := conn.WaitForNotification(ctx)
		if err != nil {
			return fmt.Errorf("notifier: wait: %w", err)
		}

		select {
		case n.wakeup <- struct{}{}:
		default:
		}

		_ = notif
	}
}
