package ports

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/domain/transaction"
)

var (
	ErrWebhookSignature = errors.New("gateway: webhook signature verification failed")
	ErrWebhookParse     = errors.New("gateway: webhook payload could not be parsed")
)

type GatewayDisputeEvent struct {
	GatewayDisputeID  string
	GatewayReferenceID string // payment_intent ID (pi_xxx)
	Status            string // maps to dispute domain status
	Reason            string
	Amount            int64
	Currency          string
	EvidenceDueBy     *time.Time
	ResolvedAt        *time.Time
}

type GatewayWebhookEvent struct {
	EventID            string
	GatewayReferenceID string
	Status             GatewayPaymentStatus
	EventType          string
	GatewayMetadata    map[string]any
	Dispute            *GatewayDisputeEvent
}

type GatewayWebhookParser interface {
	ParseWebhook(body []byte, headers map[string]string, secret string) (*GatewayWebhookEvent, error)
}

type GatewayPaymentStatus string
type GatewayRefundStatus string
type GatewayCancelStatus string
type ErrorCategory string

const (
	GatewayPaymentStatusPending     GatewayPaymentStatus = "PENDING"
	GatewayPaymentStatusProcessing  GatewayPaymentStatus = "PROCESSING"
	GatewayPaymentStatusAuthorized  GatewayPaymentStatus = "AUTHORIZED"
	GatewayPaymentStatusSucceeded   GatewayPaymentStatus = "SUCCEEDED"
	GatewayPaymentStatusFailed      GatewayPaymentStatus = "FAILED"
	GatewayPaymentStatusCancelled   GatewayPaymentStatus = "CANCELLED"
	GatewayPaymentStatusAmbiguous   GatewayPaymentStatus = "AMBIGUOUS"
)
const (
	GatewayRefundStatusInitiated  GatewayRefundStatus = "INITIATED"
	GatewayRefundStatusProcessing GatewayRefundStatus = "PROCESSING"
	GatewayRefundStatusCompleted  GatewayRefundStatus = "COMPLETED"
	GatewayRefundStatusFailed     GatewayRefundStatus = "FAILED"
)
const (
	GatewayCancelStatusCancelled    GatewayCancelStatus = "CANCELLED"
	GatewayCancelStatusFailed       GatewayCancelStatus = "FAILED"
	GatewayCancelStatusNotSupported GatewayCancelStatus = "NOT_SUPPORTED"
)
const (
	ErrorCategoryHardDecline         ErrorCategory = "hard_decline"
	ErrorCategorySoftDecline         ErrorCategory = "soft_decline"
	ErrorCategoryNetworkTimeout      ErrorCategory = "network_timeout"
	ErrorCategoryGatewayError        ErrorCategory = "gateway_error"
	ErrorCategoryIdempotencyConflict ErrorCategory = "idempotency_conflict"
	ErrorCategoryAmbiguous           ErrorCategory = "ambiguous"
)

type GatewayCapabilities struct {
	SupportsCancel          bool
	SupportsPartialRefund   bool
	SupportsManualCapture   bool
	IdempotencyCapable      bool
	SupportedPaymentMethods []transaction.PaymentMethod
	SupportedCurrencies     []string
}
type GatewayPaymentRequest struct {
	TransactionID  uuid.UUID
	TenantID       uuid.UUID
	Amount         int64
	Currency       string
	PaymentMethod  transaction.PaymentMethod
	IdempotencyKey string
	Metadata       map[string]any
	CustomerEmail  string
	Description    string
	AttemptNumber  int
}
type GatewayStatusRequest struct {
	TransactionID      uuid.UUID
	GatewayReferenceID string
	IdempotencyKey     string
}
type GatewayRefundRequest struct {
	RefundID           uuid.UUID
	TransactionID      uuid.UUID
	GatewayReferenceID string
	Amount             int64
	Currency           string
	Reason             string
	IdempotencyKey     string
}
type GatewayCancelRequest struct {
	TransactionID      uuid.UUID
	GatewayReferenceID string
	IdempotencyKey     string
}
type GatewayCaptureRequest struct {
	TransactionID      uuid.UUID
	GatewayReferenceID string
	IdempotencyKey     string
	Amount             int64
	Currency           string
}
type GatewayCaptureResponse struct {
	GatewayReferenceID string
	Status             GatewayPaymentStatus
	Amount             int64
	Currency           string
	ErrorCode          string
	ErrorMessage       string
}
type GatewayCardResponse struct {
	CardBrand string
	Last4     string
	Network   string
	RiskScore float64
	AuthCode  string
}
type GatewayUPIResponse struct {
	VPA              string // plaintext in memory; never logged
	UPITransactionID string
	PayerBank        string
}
type GatewayNetbankingResponse struct {
	BankCode        string
	BankReferenceID string
}
type GatewayWalletResponse struct {
	WalletProvider      string
	WalletTransactionID string
}

type GatewayMethodResponse interface{ gatewayMethodResponse() }

func (*GatewayCardResponse) gatewayMethodResponse()       {}
func (*GatewayUPIResponse) gatewayMethodResponse()        {}
func (*GatewayNetbankingResponse) gatewayMethodResponse() {}
func (*GatewayWalletResponse) gatewayMethodResponse()     {}

type GatewayPaymentResponse struct {
	GatewayReferenceID string
	Status             GatewayPaymentStatus
	Amount             int64
	Currency           string
	ClientSecret       string
	ErrorCode          string
	ErrorMessage       string
	MethodResponse     GatewayMethodResponse
	GatewayMetadata    map[string]any
	GatewayFees        int64
	NextAction         *NextAction
}

type NextAction struct {
	Type         string `json:"type"`
	RedirectURL  string `json:"redirect_url,omitempty"`
	ClientSecret string `json:"client_secret,omitempty"`
}
type GatewayRefundResponse struct {
	GatewayRefundID string
	Status          GatewayRefundStatus
	Amount          int64
	Currency        string
}
type GatewayCancelResponse struct{ Status GatewayCancelStatus }
type GatewayAdapter interface {
	InitiatePayment(ctx context.Context, req GatewayPaymentRequest) (*GatewayPaymentResponse, error)
	CheckStatus(ctx context.Context, req GatewayStatusRequest) (*GatewayPaymentResponse, error)
	Refund(ctx context.Context, req GatewayRefundRequest) (*GatewayRefundResponse, error)
	Cancel(ctx context.Context, req GatewayCancelRequest) (*GatewayCancelResponse, error)
	CapturePayment(ctx context.Context, req GatewayCaptureRequest) (*GatewayCaptureResponse, error)
	Capabilities() GatewayCapabilities
}
type GatewayError struct {
	Category       ErrorCategory
	Code           string
	GatewayCode    string
	GatewayMessage string
	Retryable      bool
	Underlying     error
}

func (e *GatewayError) Error() string {
	if e.GatewayMessage != "" {
		return fmt.Sprintf("gateway error [%s/%s]: %s", e.Category, e.GatewayCode, e.GatewayMessage)
	}
	return fmt.Sprintf("gateway error [%s/%s]", e.Category, e.Code)
}

func (e *GatewayError) Unwrap() error { return e.Underlying }
