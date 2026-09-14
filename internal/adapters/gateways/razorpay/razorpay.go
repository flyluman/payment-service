package razorpay

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/domain/transaction"
	"github.com/crownroutes/payment-service/internal/ports"
)

type TenantConfig struct {
	KeyID          string
	KeySecret      string
	BaseURL        string
	PublishableKey string
}

type Config struct {
	Timeout       time.Duration
	HTTPClient    *http.Client
	ResolveConfig func(ctx context.Context, tenantID uuid.UUID) (*TenantConfig, error)
}

const maxResponseBytes = 2 << 20
const defaultHTTPTimeout = 30 * time.Second

type Adapter struct {
	resolve func(ctx context.Context, tenantID uuid.UUID) (*TenantConfig, error)
	client  *http.Client

	txnTenants sync.Map
}

func New(cfg Config) *Adapter {
	client := cfg.HTTPClient
	if client == nil {
		timeout := cfg.Timeout
		if timeout <= 0 {
			timeout = defaultHTTPTimeout
		}
		client = &http.Client{Timeout: timeout}
	}
	return &Adapter{
		resolve: cfg.ResolveConfig,
		client:  client,
	}
}

func (a *Adapter) Capabilities() ports.GatewayCapabilities {
	return ports.GatewayCapabilities{
		SupportsCancel:        false,
		SupportsPartialRefund: true,
		IdempotencyCapable:    false,
		SupportedPaymentMethods: []transaction.PaymentMethod{
			transaction.PaymentMethodCard, transaction.PaymentMethodUPI,
			transaction.PaymentMethodNetbanking, transaction.PaymentMethodWallet,
		},
		SupportedCurrencies: []string{"BDT"},
	}
}

func (a *Adapter) InitiatePayment(ctx context.Context, req ports.GatewayPaymentRequest) (*ports.GatewayPaymentResponse, error) {
	tc, err := a.resolve(ctx, req.TenantID)
	if err != nil {
		return nil, &ports.GatewayError{
			Category: ports.ErrorCategoryGatewayError, Code: "config_resolve_error",
			GatewayMessage: err.Error(),
		}
	}

	a.txnTenants.Store(req.TransactionID, req.TenantID)

	receipt := req.IdempotencyKey
	if receipt == "" {
		receipt = deriveReceipt(req.TransactionID, req.AttemptNumber)
	}

	body := map[string]any{
		"amount":   req.Amount,
		"currency": strings.ToUpper(req.Currency),
		"receipt":  receipt,
		"notes":    map[string]string{"transaction_id": req.TransactionID.String()},
	}

	var order rzpOrder
	if err := a.do(ctx, tc, http.MethodPost, "/v1/orders", body, &order); err != nil {
		return nil, err
	}
	return toPaymentResponse(&order, tc), nil
}

func (a *Adapter) CheckStatus(ctx context.Context, req ports.GatewayStatusRequest) (*ports.GatewayPaymentResponse, error) {
	tc, err := a.resolveForTxn(ctx, req.TransactionID)
	if err != nil {
		return nil, err
	}

	var order rzpOrder
	if err := a.do(ctx, tc, http.MethodGet, "/v1/orders/"+url.PathEscape(req.GatewayReferenceID), nil, &order); err != nil {
		return nil, err
	}
	return toPaymentResponse(&order, tc), nil
}

func (a *Adapter) Refund(ctx context.Context, req ports.GatewayRefundRequest) (*ports.GatewayRefundResponse, error) {
	tc, err := a.resolveForTxn(ctx, req.TransactionID)
	if err != nil {
		return nil, err
	}

	body := map[string]any{}
	if req.Amount > 0 {
		body["amount"] = req.Amount
	}
	if req.Reason != "" {
		body["notes"] = map[string]string{"reason": req.Reason}
	}

	var refund rzpRefund
	if err := a.do(ctx, tc, http.MethodPost, "/v1/payments/"+url.PathEscape(req.GatewayReferenceID)+"/refund", body, &refund); err != nil {
		return nil, err
	}
	return &ports.GatewayRefundResponse{
		GatewayRefundID: refund.ID,
		Status:          mapRefundStatus(refund.Status),
		Amount:          refund.Amount,
		Currency:        strings.ToUpper(refund.Currency),
	}, nil
}

func (a *Adapter) Cancel(ctx context.Context, req ports.GatewayCancelRequest) (*ports.GatewayCancelResponse, error) {
	return &ports.GatewayCancelResponse{Status: ports.GatewayCancelStatusNotSupported}, nil
}

func (a *Adapter) CapturePayment(ctx context.Context, req ports.GatewayCaptureRequest) (*ports.GatewayCaptureResponse, error) {
	return nil, &ports.GatewayError{
		Category: ports.ErrorCategoryGatewayError, Code: "not_implemented",
		GatewayMessage: "manual capture not yet implemented",
	}
}

func (a *Adapter) resolveForTxn(ctx context.Context, txnID uuid.UUID) (*TenantConfig, error) {
	v, ok := a.txnTenants.Load(txnID)
	if !ok {
		return nil, &ports.GatewayError{
			Category: ports.ErrorCategoryGatewayError, Code: "tenant_not_cached",
			GatewayMessage: fmt.Sprintf("tenant for transaction %s not found", txnID),
		}
	}
	return a.resolve(ctx, v.(uuid.UUID))
}

func (a *Adapter) do(ctx context.Context, tc *TenantConfig, method, path string, body any, out any) error {
	baseURL := tc.BaseURL
	if baseURL == "" {
		baseURL = "https://api.razorpay.com"
	}

	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return &ports.GatewayError{Category: ports.ErrorCategoryGatewayError, Code: "marshal_error", GatewayMessage: err.Error()}
		}
		reader = bytes.NewReader(b)
	}

	httpReq, err := http.NewRequestWithContext(ctx, method, baseURL+path, reader)
	if err != nil {
		return &ports.GatewayError{Category: ports.ErrorCategoryGatewayError, Code: "request_build_error", GatewayMessage: err.Error()}
	}
	httpReq.SetBasicAuth(tc.KeyID, tc.KeySecret)
	if body != nil {
		httpReq.Header.Set("Content-Type", "application/json")
	}

	resp, err := a.client.Do(httpReq)
	if err != nil {
		return &ports.GatewayError{Category: ports.ErrorCategoryNetworkTimeout, Code: "network_error", GatewayMessage: err.Error(), Retryable: true, Underlying: err}
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return &ports.GatewayError{Category: ports.ErrorCategoryNetworkTimeout, Code: "read_error", GatewayMessage: err.Error(), Retryable: true, Underlying: err}
	}
	if resp.StatusCode >= 400 {
		var env rzpErrorEnvelope
		_ = json.Unmarshal(data, &env)
		e := env.Error
		if e == nil {
			e = &rzpError{Code: "SERVER_ERROR", Description: "unknown gateway error"}
		}
		cat := classifyError(e.Code, e.Reason)
		return &ports.GatewayError{
			Category:       cat,
			Code:           string(cat),
			GatewayCode:    firstNonEmpty(e.Reason, e.Code),
			GatewayMessage: e.Description,
			Retryable:      cat == ports.ErrorCategoryNetworkTimeout || cat == ports.ErrorCategorySoftDecline,
		}
	}

	if out != nil {
		if err := json.Unmarshal(data, out); err != nil {
			return &ports.GatewayError{Category: ports.ErrorCategoryGatewayError, Code: "decode_error", GatewayMessage: err.Error()}
		}
	}
	return nil
}

func deriveReceipt(id uuid.UUID, attemptNumber int) string {
	sum := crc32.ChecksumIEEE([]byte(id.String() + strconv.Itoa(attemptNumber)))
	enc := base62(sum)
	if len(enc) >= 40 {
		return enc[:40]
	}
	return strings.Repeat("0", 40-len(enc)) + enc
}

const base62chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

func base62(n uint32) string {
	if n == 0 {
		return "0"
	}
	var buf []byte
	for n > 0 {
		buf = append([]byte{base62chars[n%62]}, buf...)
		n /= 62
	}
	return string(buf)
}

func toPaymentResponse(o *rzpOrder, tc *TenantConfig) *ports.GatewayPaymentResponse {
	resp := &ports.GatewayPaymentResponse{
		GatewayReferenceID: o.ID,
		Status:             mapOrderStatus(o.Status),
		Amount:             o.Amount,
		Currency:           strings.ToUpper(o.Currency),
		GatewayMetadata:    map[string]any{},
	}
	for k, v := range o.Notes {
		resp.GatewayMetadata[k] = v
	}
	resp.GatewayMetadata["order_id"] = o.ID
	resp.GatewayMetadata["key_id"] = tc.KeyID
	if tc.PublishableKey != "" {
		resp.GatewayMetadata["publishable_key"] = tc.PublishableKey
	}
	return resp
}

func mapOrderStatus(s string) ports.GatewayPaymentStatus {
	switch s {
	case "paid":
		return ports.GatewayPaymentStatusSucceeded
	case "attempted":
		return ports.GatewayPaymentStatusProcessing
	case "created":
		return ports.GatewayPaymentStatusPending
	default:
		return ports.GatewayPaymentStatusPending
	}
}

func mapRefundStatus(s string) ports.GatewayRefundStatus {
	switch s {
	case "processed":
		return ports.GatewayRefundStatusCompleted
	case "pending":
		return ports.GatewayRefundStatusProcessing
	case "failed":
		return ports.GatewayRefundStatusFailed
	default:
		return ports.GatewayRefundStatusProcessing
	}
}

func classifyError(code, reason string) ports.ErrorCategory {
	switch code {
	case "GATEWAY_ERROR":
		return ports.ErrorCategoryGatewayError
	case "SERVER_ERROR":
		return ports.ErrorCategoryNetworkTimeout
	case "BAD_REQUEST_ERROR":
		switch reason {
		case "payment_failed", "payment_declined", "card_declined":
			return ports.ErrorCategoryHardDecline
		case "insufficient_funds", "payment_pending":
			return ports.ErrorCategorySoftDecline
		default:
			return ports.ErrorCategoryGatewayError
		}
	default:
		return ports.ErrorCategoryGatewayError
	}
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

var _ ports.GatewayAdapter = (*Adapter)(nil)
