package ports

import (
	"context"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/domain/gateway"
)

// TenantConfigStore provides CRUD for per-tenant gateway configurations.
type TenantConfigStore interface {
	Get(ctx context.Context, tenantID uuid.UUID, gatewayID string) (*gateway.TenantGatewayConfig, error)
	ListByTenant(ctx context.Context, tenantID uuid.UUID) ([]*gateway.TenantGatewayConfig, error)
	ListActiveByTenant(ctx context.Context, tenantID uuid.UUID) ([]*gateway.TenantGatewayConfig, error)
	ListByGateway(ctx context.Context, gatewayID string) ([]*gateway.TenantGatewayConfig, error)
	Upsert(ctx context.Context, cfg *gateway.TenantGatewayConfig) (int, error)
	SetActive(ctx context.Context, tenantID uuid.UUID, gatewayID string, active bool) error
	Delete(ctx context.Context, tenantID uuid.UUID, gatewayID string) error
}
