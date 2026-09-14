package ports

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/domain/dispute"
)

type DisputeStore interface {
	Create(ctx context.Context, d *dispute.Dispute) error
	GetByID(ctx context.Context, id uuid.UUID) (*dispute.Dispute, error)
	GetByGatewayDisputeID(ctx context.Context, gatewayID, gatewayDisputeID string) (*dispute.Dispute, error)
	Update(ctx context.Context, d *dispute.Dispute) error
	List(ctx context.Context, filters DisputeFilters) ([]*dispute.Dispute, error)
}

type DisputeEvidenceStore interface {
	Add(ctx context.Context, e *dispute.Evidence) error
	ListByDisputeID(ctx context.Context, disputeID uuid.UUID) ([]*dispute.Evidence, error)
}

type DisputeFilters struct {
	TenantID  uuid.UUID
	Status    *dispute.Status
	GatewayID *string
	DateFrom  *time.Time
	DateTo    *time.Time
	Limit     int
	Offset    int
}
