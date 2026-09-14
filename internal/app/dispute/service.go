package dispute

import (
	"context"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/domain/dispute"
	"github.com/crownroutes/payment-service/internal/ports"
)

type Service struct {
	store    ports.DisputeStore
	evidence ports.DisputeEvidenceStore
}

func NewService(store ports.DisputeStore, evidence ports.DisputeEvidenceStore) *Service {
	return &Service{store: store, evidence: evidence}
}

func (s *Service) CreateDispute(ctx context.Context, d *dispute.Dispute) error {
	existing, err := s.store.GetByGatewayDisputeID(ctx, d.GatewayID, d.GatewayDisputeID)
	if err != nil && !isNotFound(err) {
		return err
	}
	if existing != nil {
		existing.Status = d.Status
		existing.Reason = d.Reason
		existing.EvidenceDueBy = d.EvidenceDueBy
		if d.Status == dispute.StatusWon || d.Status == dispute.StatusLost || d.Status == dispute.StatusAccepted || d.Status == dispute.StatusExpired {
			existing.ResolvedAt = d.ResolvedAt
		}
		return s.store.Update(ctx, existing)
	}
	return s.store.Create(ctx, d)
}

func (s *Service) GetDispute(ctx context.Context, id uuid.UUID) (*dispute.Dispute, error) {
	return s.store.GetByID(ctx, id)
}

func (s *Service) ListDisputes(ctx context.Context, filters ports.DisputeFilters) ([]*dispute.Dispute, error) {
	return s.store.List(ctx, filters)
}

func (s *Service) SubmitEvidence(ctx context.Context, disputeID uuid.UUID, ev *dispute.Evidence) error {
	d, err := s.store.GetByID(ctx, disputeID)
	if err != nil {
		return err
	}
	if !d.CanSubmitEvidence() {
		return dispute.ErrInvalidTransition
	}
	ev.DisputeID = disputeID
	if err := s.evidence.Add(ctx, ev); err != nil {
		return err
	}
	if d.EvidenceSubmittedAt == nil {
		now := ev.SubmittedAt
		d.EvidenceSubmittedAt = &now
		return s.store.Update(ctx, d)
	}
	return nil
}

func (s *Service) ListEvidence(ctx context.Context, disputeID uuid.UUID) ([]*dispute.Evidence, error) {
	return s.evidence.ListByDisputeID(ctx, disputeID)
}

func isNotFound(err error) bool {
	if de, ok := err.(*dispute.DisputeError); ok && de.Code == "NOT_FOUND" {
		return true
	}
	return false
}
