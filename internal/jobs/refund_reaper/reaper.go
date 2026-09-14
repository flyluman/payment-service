package refundreaper

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/domain/refund"
	"github.com/crownroutes/payment-service/internal/ports"
)

type StaleRefundLister interface {
	ListStaleRefunds(ctx context.Context, olderThan time.Duration, maxAttempts, limit int) ([]uuid.UUID, error)
}

type RefundRetrier interface {
	RetryStaleRefund(ctx context.Context, refundID uuid.UUID) (*refund.Refund, error)
}

type Config struct {
	StaleAfter  time.Duration
	MaxAttempts int
	BatchLimit  int
}

type Reaper struct {
	refunds StaleRefundLister
	retry   RefundRetrier
	log     ports.Logger
	cfg     Config
}

func New(refunds StaleRefundLister, retry RefundRetrier, log ports.Logger, cfg Config) *Reaper {
	if cfg.StaleAfter <= 0 {
		cfg.StaleAfter = 5 * time.Minute
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 5
	}
	if cfg.BatchLimit <= 0 {
		cfg.BatchLimit = 100
	}
	return &Reaper{refunds: refunds, retry: retry, log: log, cfg: cfg}
}

// RunOnce re-drives refunds stuck in a non-terminal state. Ambiguous or
// timed-out gateway responses leave a refund in REFUND_PROCESSING with no
// scheduled follow-up; the relay only runs on REFUND_INITIATED events, so
// without this job those refunds would never resolve.
//
// Note on at-least-once semantics: re-driving a refund whose outcome was
// ambiguous can double-call the gateway. The claim gate in RetryStaleRefund
// serializes concurrent reapers, and gateways that key refunds by their own id
// (Stripe/Razorpay/FIB) treat a repeat call for the same refund as a lookup.
func (r *Reaper) RunOnce(ctx context.Context) error {
	ids, err := r.refunds.ListStaleRefunds(ctx, r.cfg.StaleAfter, r.cfg.MaxAttempts, r.cfg.BatchLimit)
	if err != nil {
		r.log.Error(ports.LogEventRefundRetry, map[string]any{
			ports.FieldErrorCode: "list_stale_refunds_failed",
		}, err)
		return nil
	}

	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return nil
		}
		rf, err := r.retry.RetryStaleRefund(ctx, id)
		if err != nil {
			r.log.Error(ports.LogEventRefundRetry, map[string]any{
				ports.FieldErrorCode:     "refund_retry_failed",
				ports.FieldRefundID:      id.String(),
				ports.FieldTransactionID: "",
			}, err)
			continue
		}
		r.log.Info(ports.LogEventRefundRetry, map[string]any{
			ports.FieldRefundID:  id.String(),
			ports.FieldNewState:  string(rf.Status),
			ports.FieldErrorCode: "",
		})
	}
	return nil
}
