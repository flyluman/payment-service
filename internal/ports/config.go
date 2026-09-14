package ports

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/domain/fees"
)

type ConfigStore interface {
	GetGatewayConfig(ctx context.Context, gatewayID string) (*GatewayConfig, error)
	ListActiveGateways(ctx context.Context) ([]*GatewayConfig, error)
	GetFeeModel(ctx context.Context, gatewayID, paymentMethod string) (*GatewayFeeModel, error)
	GetMetadataSchema(ctx context.Context, gatewayID string) (*GatewayMetadataSchema, error)
	GetProcessingTimeout(ctx context.Context, gatewayID, paymentMethod string) (time.Duration, error)
	GetCurrencyRates(ctx context.Context, tenantID uuid.UUID) ([]fees.CurrencyRate, error)
}

type GatewayConfig struct {
	GatewayID             string
	DisplayName           string
	IsActive              bool
	MinAmount             int64
	MaxAmount             int64
	SupportedCurrencies   []string
	SupportedMethods      []string
	IdempotencyCapable    bool
	SupportsCancel        bool
	SupportsPartialRefund bool
	EstimatedTimeouts     map[string]time.Duration
	CircuitBreaker        CircuitBreakerState
	Priority              int
	UpdatedAt             time.Time
}

type CircuitBreakerState struct {
	State         string
	CooldownUntil time.Time
}

func (cb CircuitBreakerState) IsOpen() bool {
	return cb.State == "OPEN" && time.Now().UTC().Before(cb.CooldownUntil)
}

type GatewayFeeModel struct {
	GatewayID                  string
	PaymentMethod              string
	FixedFee                   int64
	PercentageBPS              int64
	InterchangeCap             *int64
	DiscountVolumeThreshold    int64
	ChargesCurrency            string
}

func (f *GatewayFeeModel) CalculateFee(amountUnits, discountVolumeUnits int64) int64 {
	fee := (amountUnits*f.PercentageBPS)/10000 + f.FixedFee

	if f.InterchangeCap != nil && fee > *f.InterchangeCap {
		fee = *f.InterchangeCap
	}

	if f.DiscountVolumeThreshold > 0 &&
		discountVolumeUnits > f.DiscountVolumeThreshold {
		fee = (fee * 95) / 100
	}

	return fee
}

type GatewayMetadataSchema struct {
	GatewayID    string
	AllowedKeys  []string
	RequiredKeys []string
	MaxSizeBytes int
}

