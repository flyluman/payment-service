package tenantwebhook

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/ports"
)

type Dispatcher struct {
	writer ports.TenantWebhookWriter
	config ports.TenantWebhookConfigStore
}

func NewDispatcher(writer ports.TenantWebhookWriter, config ports.TenantWebhookConfigStore) *Dispatcher {
	return &Dispatcher{writer: writer, config: config}
}

func (d *Dispatcher) Dispatch(ctx context.Context, tenantID uuid.UUID, txnID uuid.UUID, eventType string, payload []byte) error {
	if d.writer == nil || d.config == nil {
		return nil
	}

	cfg, err := d.config.Get(ctx, tenantID)
	if err != nil {
		return fmt.Errorf("tenant_webhook: load config: %w", err)
	}
	if cfg == nil || !cfg.IsActive || cfg.EndpointURL == "" {
		return nil
	}

	return d.writer.WriteDelivery(ctx, ports.TenantWebhookDelivery{
		TenantID:      tenantID,
		TransactionID: txnID,
		EventType:     eventType,
		Payload:       payload,
		EndpointURL:   cfg.EndpointURL,
	})
}
