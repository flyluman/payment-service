package ports

import (
	"testing"

	"github.com/crownroutes/payment-service/internal/domain/transaction"
)

func TestEventTypeForTransactionStatus(t *testing.T) {
	tests := []struct {
		name   string
		status transaction.Status
		want   string
		wantOK bool
	}{
		{name: "captured", status: transaction.StatusCaptured, want: EventTypeTransactionCaptured, wantOK: true},
		{name: "cancelled", status: transaction.StatusCancelled, want: EventTypeTransactionCancelled, wantOK: true},
		{name: "authorized", status: transaction.StatusAuthorized, want: EventTypeTransactionAuthorized, wantOK: true},
		{name: "failed", status: transaction.StatusFailed, want: EventTypeTransactionFailed, wantOK: true},
		{name: "settled", status: transaction.StatusSettled, want: EventTypeTransactionSettled, wantOK: true},
		{name: "refunded", status: transaction.StatusRefunded, want: EventTypeRefundSucceeded, wantOK: true},
		{name: "pending", status: transaction.StatusPending, wantOK: false},
		{name: "processing", status: transaction.StatusProcessing, wantOK: false},
		{name: "refund_pending", status: transaction.StatusRefundPending, wantOK: false},
		{name: "partially_refunded", status: transaction.StatusPartiallyRefunded, wantOK: false},
		{name: "refund_failed", status: transaction.StatusRefundFailed, wantOK: false},
		{name: "disputed", status: transaction.StatusDisputed, wantOK: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := EventTypeForTransactionStatus(tt.status)
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tt.wantOK)
			}
			if got != tt.want {
				t.Errorf("event type = %q, want %q", got, tt.want)
			}
		})
	}
}
