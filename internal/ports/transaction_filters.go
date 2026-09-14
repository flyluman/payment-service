package ports

import (
	"time"

	"github.com/google/uuid"
)

type TransactionFilter struct {
	TenantID         *uuid.UUID
	UserID           *uuid.UUID
	Status           []string
	GatewayID        []string
	MinAmount        *int64
	MaxAmount        *int64
	MinGatewayAmount *int64
	MaxGatewayAmount *int64
	GatewayCurrency  *string
	Currency         *string
	PaymentMethod    []string
	DateFrom         *time.Time
	DateTo           *time.Time
	Cursor           *string
	Limit            int
}

type TransactionSummary struct {
	ID              uuid.UUID  `json:"id"`
	TenantID        uuid.UUID  `json:"tenant_id"`
	UserID          *uuid.UUID `json:"user_id,omitempty"`
	Amount          int64      `json:"amount"`
	Currency        string     `json:"currency"`
	Status          string     `json:"status"`
	GatewayID       string     `json:"gateway_id,omitempty"`
	PaymentMethod   string     `json:"payment_method,omitempty"`
	GatewayAmount   *int64     `json:"gateway_amount,omitempty"`
	GatewayCurrency string     `json:"gateway_currency,omitempty"`
	CreatedAt       time.Time  `json:"created_at"`
}

type TransactionListResult struct {
	Transactions []TransactionSummary `json:"transactions"`
	NextCursor   *string              `json:"next_cursor,omitempty"`
	HasMore      bool                 `json:"has_more"`
}
