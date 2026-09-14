package dispute

import (
	"time"

	"github.com/google/uuid"
)

type EvidenceType string

const (
	EvidenceReceipt             EvidenceType = "RECEIPT"
	EvidenceInvoice             EvidenceType = "INVOICE"
	EvidenceCustomerComms       EvidenceType = "CUSTOMER_COMMS"
	EvidenceTermsOfService      EvidenceType = "TERMS_OF_SERVICE"
	EvidenceRefundPolicy        EvidenceType = "REFUND_POLICY"
	EvidenceCancellationPolicy  EvidenceType = "CANCELLATION_POLICY"
	EvidenceOther               EvidenceType = "OTHER"
)

type Evidence struct {
	ID          uuid.UUID    `json:"id"`
	DisputeID   uuid.UUID    `json:"dispute_id"`
	Type        EvidenceType `json:"evidence_type"`
	FileURL     string       `json:"file_url"`
	Notes       string       `json:"notes,omitempty"`
	SubmittedAt time.Time    `json:"submitted_at"`
}
