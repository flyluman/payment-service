package transaction

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/domain/fees"
)

type Status string
type PaymentMethod string
type Actor string
type CancelVia string
type FailureReasonSource string
type CaptureMode string

type FailureReason struct {
	Category       string              `json:"category"`
	Code           string              `json:"code"`
	GatewayCode    string              `json:"gateway_code"`
	GatewayMessage string              `json:"gateway_message"`
	Source         FailureReasonSource `json:"source"`
}

type CardDetails struct {
	CardBrand string  `json:"card_brand"`
	Last4     string  `json:"last4"`
	Network   string  `json:"network"`
	RiskScore float64 `json:"risk_score"`
	AuthCode  string  `json:"auth_code"`
}

type UPIDetails struct {
	VPA              string `json:"vpa"`
	UPITransactionID string `json:"upi_transaction_id"`
	PayerBank        string `json:"payer_bank"`
}

type NetbankingDetails struct {
	BankCode        string `json:"bank_code"`
	BankReferenceID string `json:"bank_reference_id"`
}

type WalletDetails struct {
	WalletProvider      string `json:"wallet_provider"`
	WalletTransactionID string `json:"wallet_transaction_id"`
}

type MethodDetails struct {
	Card       *CardDetails       `json:"card,omitempty"`
	UPI        *UPIDetails        `json:"upi,omitempty"`
	Netbanking *NetbankingDetails `json:"netbanking,omitempty"`
	Wallet     *WalletDetails     `json:"wallet,omitempty"`
}

type Txn struct {
	ID                      uuid.UUID
	TenantID                uuid.UUID
	UserID                  uuid.UUID
	Amount                  int64
	Currency                string
	PaymentMethod           PaymentMethod
	Status                  Status
	CaptureMode             CaptureMode
	Version                 int // optimistic locking — repo rejects writes where stored version != expected
	GatewayID               string
	GatewayReferenceID      string
	GatewayIdempotencyKey   string
	AttemptedGateway        string
	ActualGateway           string
	OriginalGateway         string
	EstimatedTimeoutSeconds int
	FailureReason           *FailureReason
	MethodDetails           *MethodDetails
	Metadata                map[string]any
	Description             string
	CustomerID              uuid.UUID
	CustomerEmail           string
	CancelIntent            bool
	CancelRequestedBy       Actor
	CancelRequestedAt       *time.Time
	CancelRequestedVia      CancelVia
	ProcessingStartedAt     *time.Time
	ProcessingTimeout       *time.Duration
	CallbackURL             string
	RedirectURL             string
	TokenHash               string
	GatewayFeeEstimate      *int64
	GatewayFeeCurrency      string
	GatewayFeeModelVersion  int
	FeeBreakdown            *fees.Breakdown
	GatewayAmount           *int64
	GatewayCurrency         string
	CreatedAt               time.Time
	UpdatedAt               time.Time
}

const (
	StatusPending            Status = "PENDING"
	StatusProcessing         Status = "PROCESSING"
	StatusAuthorized         Status = "AUTHORIZED"
	StatusCaptured           Status = "CAPTURED"
	StatusSettled            Status = "SETTLED"
	StatusFailed             Status = "FAILED"
	StatusCancelled          Status = "CANCELLED"
	StatusRefundPending      Status = "REFUND_PENDING"
	StatusPartiallyRefunded  Status = "PARTIALLY_REFUNDED"
	StatusRefunded           Status = "REFUNDED"
	StatusRefundFailed       Status = "REFUND_FAILED"
	StatusDisputed           Status = "DISPUTED"
)

const (
	PaymentMethodCard       PaymentMethod = "card"
	PaymentMethodUPI        PaymentMethod = "upi"
	PaymentMethodNetbanking PaymentMethod = "netbanking"
	PaymentMethodWallet     PaymentMethod = "wallet"
)

const (
	ActorSystem   Actor = "system"
	ActorTenant Actor = "tenant"
	ActorOps      Actor = "ops"
	ActorGateway  Actor = "gateway"
)

const (
	CancelViaAPI       CancelVia = "api"
	CancelViaDashboard CancelVia = "dashboard"
	CancelViaOpsTool   CancelVia = "ops-tool"
)

const (
	FailureReasonSourceGateway  FailureReasonSource = "gateway"
	FailureReasonSourceInternal FailureReasonSource = "internal"
	FailureReasonSourceTimeout  FailureReasonSource = "timeout"
)

const (
	CaptureModeAuto    CaptureMode = "auto"
	CaptureModeManual  CaptureMode = "manual"
)

var validCaptureModes = map[CaptureMode]struct{}{
	CaptureModeAuto:   {},
	CaptureModeManual: {},
}

var validPaymentMethods = map[PaymentMethod]struct{}{
	PaymentMethodCard:       {},
	PaymentMethodUPI:        {},
	PaymentMethodNetbanking: {},
	PaymentMethodWallet:     {},
}

func (s Status) IsTerminal() bool {
	switch s {
	case StatusCaptured, StatusCancelled, StatusRefunded, StatusRefundFailed:
		return true
	}
	return false
}

func New(
	tenantID uuid.UUID,
	userID uuid.UUID,
	amount int64,
	currency string,
	method PaymentMethod,
	gatewayID string,
	customerID uuid.UUID,
	customerEmail string,
	description string,
	metadata map[string]any,
	estimatedTimeoutSec int,
	captureMode CaptureMode,
) (*Txn, error) {
	if amount <= 0 {
		return nil, fmt.Errorf("amount must be positive, got %d", amount)
	}
	if len(currency) != 3 || currency != strings.ToUpper(currency) {
		return nil, fmt.Errorf("currency must be ISO 4217 uppercase 3-letter code, got %q", currency)
	}
	if _, ok := validPaymentMethods[method]; !ok {
		return nil, fmt.Errorf("invalid payment method %q", method)
	}
	if tenantID == uuid.Nil {
		return nil, fmt.Errorf("tenantID must not be nil")
	}
	if gatewayID == "" {
		return nil, fmt.Errorf("gatewayID must not be empty")
	}
	if estimatedTimeoutSec <= 0 {
		return nil, fmt.Errorf("estimatedTimeoutSec must be positive, got %d", estimatedTimeoutSec)
	}
	if captureMode == "" {
		captureMode = CaptureModeAuto
	}
	if _, ok := validCaptureModes[captureMode]; !ok {
		return nil, fmt.Errorf("invalid capture mode %q", captureMode)
	}

	now := time.Now().UTC()
	return &Txn{
		ID:                      uuid.Must(uuid.NewV7()),
		TenantID:                tenantID,
		UserID:                  userID,
		Amount:                  amount,
		Currency:                currency,
		PaymentMethod:           method,
		Status:                  StatusPending,
		CaptureMode:             captureMode,
		Version:                 1,
		GatewayID:               gatewayID,
		EstimatedTimeoutSeconds: estimatedTimeoutSec,
		CustomerID:              customerID,
		CustomerEmail:           customerEmail,
		Description:             description,
		Metadata:                metadata,
		CreatedAt:               now,
		UpdatedAt:               now,
	}, nil
}

func (t *Txn) SetCancelIntent(by Actor, via CancelVia) {
	now := time.Now().UTC()
	t.CancelIntent = true
	t.CancelRequestedBy = by
	t.CancelRequestedAt = &now
	t.CancelRequestedVia = via
	t.UpdatedAt = now
}

func (t *Txn) IsLeaseExpired() bool {
	if t.Status != StatusProcessing {
		return false
	}
	if t.ProcessingStartedAt == nil || t.ProcessingTimeout == nil {
		return false
	}
	return time.Now().UTC().After(t.ProcessingStartedAt.Add(*t.ProcessingTimeout))
}

func (t *Txn) HasGatewayDiscrepancy() bool {
	return t.ActualGateway != "" && t.AttemptedGateway != t.ActualGateway
}

func (t *Txn) Validate() error {
	if t.ID == uuid.Nil {
		return fmt.Errorf("transaction ID must not be nil")
	}
	if t.TenantID == uuid.Nil {
		return fmt.Errorf("tenantID must not be nil")
	}
	if t.UserID == uuid.Nil {
		return fmt.Errorf("userID must not be nil")
	}
	if t.Amount <= 0 {
		return fmt.Errorf("amount must be positive, got %d", t.Amount)
	}
	if len(t.Currency) != 3 || t.Currency != strings.ToUpper(t.Currency) {
		return fmt.Errorf("currency must be ISO 4217 uppercase 3-letter code, got %q", t.Currency)
	}
	if _, ok := validPaymentMethods[t.PaymentMethod]; !ok {
		return fmt.Errorf("invalid payment method %q", t.PaymentMethod)
	}
	if t.GatewayID == "" {
		return fmt.Errorf("gatewayID must not be empty")
	}
	if t.EstimatedTimeoutSeconds <= 0 {
		return fmt.Errorf("estimatedTimeoutSeconds must be positive, got %d", t.EstimatedTimeoutSeconds)
	}
	return nil
}
