package payment

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/domain/transaction"
	"github.com/crownroutes/payment-service/internal/ports"
)

func BenchmarkCreatePayment(b *testing.B) {
	repo := &fakeRepo{}
	outbox := &fakeOutbox{}
	config := &fakeConfig{timeout: 30 * time.Second}
	tx := &fakeTransactor{}
	svc := newTestService(repo, outbox, config, tx, nil)

	input := CreateInput{
		TenantID:      uuid.New(),
		UserID:        uuid.New(),
		GatewayID:     "razorpay",
		Amount:        150000,
		Currency:      "BDT",
		PaymentMethod: transaction.PaymentMethodCard,
		CustomerID:    uuid.New(),
		CustomerEmail: "buyer@example.com",
		Description:   "benchmark order",
		CallbackURL:   "https://example.com/callback",
		RedirectURL:   "https://example.com/redirect",
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := svc.Create(context.Background(), input)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkProcessPayment(b *testing.B) {
	baseTxn := &transaction.Txn{
		ID:                      uuid.New(),
		TenantID:                uuid.New(),
		UserID:                  uuid.New(),
		Amount:                  150000,
		Currency:                "BDT",
		PaymentMethod:           transaction.PaymentMethodCard,
		Status:                  transaction.StatusPending,
		GatewayID:               "razorpay",
		EstimatedTimeoutSeconds: 30,
	}

	b.Run("lease_acquired", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			txn := *baseTxn
			txn.ID = uuid.Must(uuid.NewV7())
			repo := &fakeRepo{store: map[uuid.UUID]*transaction.Txn{txn.ID: &txn}}
			lease := &fakeLease{acquired: true}
			outbox := &fakeOutbox{}
			config := &fakeConfig{timeout: 30 * time.Second}
			tx := &fakeTransactor{}
			svc := NewService(repo, outbox, config, tx, lease,
				&fakeRegistry{adapter: &fakeAdapter{resp: &ports.GatewayPaymentResponse{
					GatewayReferenceID: "pi_bench",
					Status:             ports.GatewayPaymentStatusSucceeded,
				}}},
				noopLogger{}, noopMetrics{})

			_, err := svc.ProcessPayment(context.Background(), txn.ID)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("lease_miss_re_read", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			txn := *baseTxn
			txn.ID = uuid.Must(uuid.NewV7())
			// already succeeded by another instance
			succeeded := txn
			succeeded.Status = transaction.StatusCaptured
			succeeded.GatewayReferenceID = "pi_other"

			repo := &fakeRepo{store: map[uuid.UUID]*transaction.Txn{
				txn.ID: &succeeded,
			}}
			lease := &fakeLease{acquired: false, cached: nil}
			outbox := &fakeOutbox{}
			config := &fakeConfig{timeout: 30 * time.Second}
			tx := &fakeTransactor{}
			svc := NewService(repo, outbox, config, tx, lease,
				&fakeRegistry{}, noopLogger{}, noopMetrics{})

			_, err := svc.ProcessPayment(context.Background(), txn.ID)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
