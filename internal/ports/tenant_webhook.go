package ports

import (
	"context"
	"time"

	"github.com/google/uuid"
)

type TenantWebhookDeliveryReader interface {
	ListPending(ctx context.Context, limit int) ([]TenantWebhookDelivery, error)
}

type TenantWebhookDeliveryUpdater interface {
	MarkDelivered(ctx context.Context, id uuid.UUID) error
	MarkFailed(ctx context.Context, id uuid.UUID, attempts int, lastError string, nextAttemptAt time.Time) error
}

type TenantWebhookConfigStore interface {
	Get(ctx context.Context, tenantID uuid.UUID) (*TenantWebhookConfig, error)
}

type TenantWebhookConfigWriter interface {
	Upsert(ctx context.Context, cfg *TenantWebhookConfig) error
	List(ctx context.Context) ([]*TenantWebhookConfig, error)
}

type TenantWebhookConfig struct {
	TenantID      uuid.UUID
	EndpointURL   string
	SigningSecret string
	IsActive      bool
}
