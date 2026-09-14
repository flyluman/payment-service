package relay

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/ports"
)

func BenchmarkWorkerProcessEvent(b *testing.B) {
	tests := []struct {
		name   string
		errPub error
	}{
		{"publish_success", nil},
		{"publish_retry", errors.New("timeout")},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			b.ReportAllocs()
			outbox := &fakeOutbox{}
			pub := &fakePublisher{err: tt.errPub}
			w := newWorker(outbox, pub, Config{
				MaxAttempts: 5, BaseBackoff: time.Second, MaxBackoff: time.Minute,
			})

			for i := 0; i < b.N; i++ {
				w.processEvent(context.Background(), ports.PendingEvent{
					ID:          uuid.Must(uuid.NewV7()),
					AggregateID: uuid.Must(uuid.NewV7()),
					EventType:   "TEST",
					Attempts:    0,
				})
			}
		})
	}
}

func BenchmarkWorkerRunOnce(b *testing.B) {
	outbox := &fakeOutbox{
		pending: make([]ports.PendingEvent, 50),
	}
	for i := range outbox.pending {
		outbox.pending[i] = ports.PendingEvent{
			ID:          uuid.Must(uuid.NewV7()),
			AggregateID: uuid.Must(uuid.NewV7()),
			EventType:   "TEST",
		}
	}
	pub := &fakePublisher{}
	w := newWorker(outbox, pub, Config{MaxAttempts: 5})

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		outbox.polls = 0
		_, _, err := w.RunOnce(context.Background())
		if err != nil {
			b.Fatal(err)
		}
	}
}
