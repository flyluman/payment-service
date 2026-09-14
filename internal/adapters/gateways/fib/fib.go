package fib

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sync/singleflight"

	"github.com/crownroutes/payment-service/internal/ports"
)

type TenantConfig struct {
	ClientID     string
	ClientSecret string
	BaseURL      string
	CallbackURL  string
}

type Config struct {
	Timeout       time.Duration
	HTTPClient    *http.Client
	ResolveConfig func(ctx context.Context, tenantID uuid.UUID) (*TenantConfig, error)
}

type tenantAuth struct {
	accessToken string
	expiry      time.Time
	skew        time.Duration
	mu          sync.RWMutex
	group       singleflight.Group
}

type Adapter struct {
	client  *http.Client
	resolve func(ctx context.Context, tenantID uuid.UUID) (*TenantConfig, error)

	mu     sync.RWMutex
	tokens map[string]*tenantAuth

	txnTenants sync.Map
}

const maxResponseBytes = 2 << 20
const defaultHTTPTimeout = 30 * time.Second

var _ ports.GatewayAdapter = (*Adapter)(nil)

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
		client:  client,
		resolve: cfg.ResolveConfig,
		tokens:  make(map[string]*tenantAuth),
	}
}

func (a *Adapter) Capabilities() ports.GatewayCapabilities {
	return ports.GatewayCapabilities{
		SupportsCancel:          true,
		SupportsPartialRefund:   false,
		IdempotencyCapable:      false,
		SupportedPaymentMethods: nil,
		SupportedCurrencies:     []string{"IQD"},
	}
}

func (a *Adapter) InitiatePayment(ctx context.Context, req ports.GatewayPaymentRequest) (*ports.GatewayPaymentResponse, error) {
	if req.TenantID == uuid.Nil {
		return nil, &ports.GatewayError{
			Category: ports.ErrorCategoryGatewayError, Code: "missing_tenant",
			GatewayMessage: "tenant_id is required",
		}
	}
	tc, err := a.resolve(ctx, req.TenantID)
	if err != nil {
		return nil, &ports.GatewayError{
			Category: ports.ErrorCategoryGatewayError, Code: "config_resolve_error",
			GatewayMessage: err.Error(),
		}
	}

	a.txnTenants.Store(req.TransactionID, req.TenantID)

	redirectURI := ""
	if req.Metadata != nil {
		if v, ok := req.Metadata["redirect_uri"].(string); ok {
			redirectURI = v
		}
	}

	fibReq := createPaymentRequest{
		MonetaryValue: monetaryValueObj{
			Amount:   formatAmount(req.Amount),
			Currency: strings.ToUpper(req.Currency),
		},
		StatusCallbackURL: tc.CallbackURL + "?transaction_id=" + req.TransactionID.String(),
		RedirectURI:       redirectURI,
		Description:       req.Description,
		Category:          "ECOMMERCE",
	}

	var resp createPaymentResponse
	if err := a.do(ctx, tc, http.MethodPost, "/protected/v1/payments", &fibReq, &resp); err != nil {
		return nil, err
	}

	paymentID := resp.ID
	if paymentID == "" {
		paymentID = resp.PaymentID
	}

	rawMeta := map[string]any{
		"qr_code":            resp.QRCode,
		"readable_code":      resp.ReadableCode,
		"personal_app_link":  resp.PersonalAppLink,
		"business_app_link":  resp.BusinessAppLink,
		"corporate_app_link": resp.CorporateAppLink,
		"valid_until":        resp.ValidUntil,
	}

	return &ports.GatewayPaymentResponse{
		GatewayReferenceID: paymentID,
		Status:             ports.GatewayPaymentStatusPending,
		Amount:             req.Amount,
		Currency:           strings.ToUpper(req.Currency),
		GatewayMetadata: rawMeta,
	}, nil
}

func (a *Adapter) CheckStatus(ctx context.Context, req ports.GatewayStatusRequest) (*ports.GatewayPaymentResponse, error) {
	tc, err := a.resolveForTxn(ctx, req.TransactionID)
	if err != nil {
		return nil, err
	}

	var resp paymentStatusResponse
	path := "/protected/v1/payments/" + req.GatewayReferenceID + "/status"
	if err := a.do(ctx, tc, http.MethodGet, path, nil, &resp); err != nil {
		return nil, err
	}

	paymentID := resp.ID
	if paymentID == "" {
		paymentID = resp.PaymentID
	}

	currency := ""
	if resp.Amount != nil {
		currency = resp.Amount.Currency
	}

	return &ports.GatewayPaymentResponse{
		GatewayReferenceID: paymentID,
		Status:             mapPaymentStatus(resp.Status),
		Currency:           currency,
		GatewayMetadata: map[string]any{},
	}, nil
}

func (a *Adapter) Refund(ctx context.Context, req ports.GatewayRefundRequest) (*ports.GatewayRefundResponse, error) {
	tc, err := a.resolveForTxn(ctx, req.TransactionID)
	if err != nil {
		return nil, err
	}

	var resp refundResponse
	path := "/protected/v1/payments/" + req.GatewayReferenceID + "/refund"
	if err := a.do(ctx, tc, http.MethodPost, path, nil, &resp); err != nil {
		return nil, err
	}

	st := ports.GatewayRefundStatusProcessing
	if resp.Status == "REFUNDED" {
		st = ports.GatewayRefundStatusCompleted
	}

	return &ports.GatewayRefundResponse{
		GatewayRefundID: resp.RefundID,
		Status:          st,
		Amount:          req.Amount,
		Currency:        strings.ToUpper(req.Currency),
	}, nil
}

func (a *Adapter) Cancel(ctx context.Context, req ports.GatewayCancelRequest) (*ports.GatewayCancelResponse, error) {
	tc, err := a.resolveForTxn(ctx, req.TransactionID)
	if err != nil {
		return nil, err
	}

	var resp actionResponse
	path := "/protected/v1/payments/" + req.GatewayReferenceID + "/cancel"
	if err := a.do(ctx, tc, http.MethodPost, path, nil, &resp); err != nil {
		return nil, err
	}

	status := ports.GatewayCancelStatusCancelled
	if resp.Status != "CANCELLED" {
		status = ports.GatewayCancelStatusFailed
	}

	return &ports.GatewayCancelResponse{Status: status}, nil
}

func (a *Adapter) resolveForTxn(ctx context.Context, txnID uuid.UUID) (*TenantConfig, error) {
	v, ok := a.txnTenants.Load(txnID)
	if !ok {
		return nil, &ports.GatewayError{
			Category: ports.ErrorCategoryGatewayError, Code: "tenant_not_cached",
			GatewayMessage: fmt.Sprintf("tenant for transaction %s not found; InitiatePayment must be called first", txnID),
		}
	}
	return a.resolve(ctx, v.(uuid.UUID))
}

func (a *Adapter) authenticate(ctx context.Context, tc *TenantConfig) (string, error) {
	key := tc.ClientID + ":" + tc.ClientSecret
	a.mu.RLock()
	ta, exists := a.tokens[key]
	a.mu.RUnlock()

	if exists {
		ta.mu.RLock()
		if ta.accessToken != "" && time.Now().Before(ta.expiry) {
			tok := ta.accessToken
			ta.mu.RUnlock()
			return tok, nil
		}
		ta.mu.RUnlock()
	}

	ch := a.authGroup(key).DoChan(key, func() (any, error) {
		return a.requestAccessToken(ctx, tc)
	})

	select {
	case result := <-ch:
		if result.Err != nil {
			return "", result.Err
		}
		return result.Val.(string), nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func (a *Adapter) authGroup(key string) *singleflight.Group {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.tokens[key] == nil {
		a.tokens[key] = &tenantAuth{}
	}
	return &a.tokens[key].group
}

func (a *Adapter) requestAccessToken(ctx context.Context, tc *TenantConfig) (string, error) {
	form := "grant_type=client_credentials"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		tc.BaseURL+"/auth/realms/fib-online-shop/protocol/openid-connect/token",
		strings.NewReader(form))
	if err != nil {
		return "", &ports.GatewayError{
			Category: ports.ErrorCategoryGatewayError, Code: "token_request_build_error",
			GatewayMessage: err.Error(),
		}
	}
	req.SetBasicAuth(tc.ClientID, tc.ClientSecret)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := a.client.Do(req)
	if err != nil {
		return "", &ports.GatewayError{
			Category: ports.ErrorCategoryNetworkTimeout, Code: "token_network_error",
			GatewayMessage: err.Error(), Retryable: true,
		}
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return "", &ports.GatewayError{
			Category: ports.ErrorCategoryNetworkTimeout, Code: "token_read_error",
			GatewayMessage: err.Error(), Retryable: true,
		}
	}

	if resp.StatusCode >= 400 {
		var env fibErrorEnvelope
		_ = json.Unmarshal(data, &env)
		return "", &ports.GatewayError{
			Category: ports.ErrorCategoryGatewayError, Code: "token_error",
			GatewayMessage: extractFibError(env),
		}
	}

	var tr tokenResponse
	if err := json.Unmarshal(data, &tr); err != nil {
		return "", &ports.GatewayError{
			Category: ports.ErrorCategoryGatewayError, Code: "token_decode_error",
			GatewayMessage: err.Error(),
		}
	}

	skew := accessTokenRefreshSkew(tr.ExpiresIn)
	key := tc.ClientID + ":" + tc.ClientSecret

	a.mu.Lock()
	a.tokens[key] = &tenantAuth{
		accessToken: tr.AccessToken,
		expiry:      time.Now().Add(time.Duration(tr.ExpiresIn) * time.Second).Add(-skew),
		skew:        skew,
	}
	a.mu.Unlock()

	return tr.AccessToken, nil
}

func accessTokenRefreshSkew(ttl int) time.Duration {
	if ttl <= 0 {
		return 0
	}
	d := time.Duration(float64(ttl) * 0.1 * float64(time.Second))
	if d > 30*time.Second {
		d = 30 * time.Second
	}
	return d
}

func (a *Adapter) do(ctx context.Context, tc *TenantConfig, method, path string, body, out any) error {
	token, err := a.authenticate(ctx, tc)
	if err != nil {
		return err
	}

	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return &ports.GatewayError{
				Category: ports.ErrorCategoryGatewayError, Code: "marshal_error",
				GatewayMessage: err.Error(),
			}
		}
		reader = bytes.NewReader(b)
	}

	httpReq, err := http.NewRequestWithContext(ctx, method, tc.BaseURL+path, reader)
	if err != nil {
		return &ports.GatewayError{
			Category: ports.ErrorCategoryGatewayError, Code: "request_build_error",
			GatewayMessage: err.Error(),
		}
	}
	httpReq.Header.Set("Authorization", "Bearer "+token)
	if body != nil {
		httpReq.Header.Set("Content-Type", "application/json")
	}

	resp, err := a.client.Do(httpReq)
	if err != nil {
		return &ports.GatewayError{
			Category: ports.ErrorCategoryNetworkTimeout, Code: "network_error",
			GatewayMessage: err.Error(), Retryable: true, Underlying: err,
		}
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return &ports.GatewayError{
			Category: ports.ErrorCategoryNetworkTimeout, Code: "read_error",
			GatewayMessage: err.Error(), Retryable: true, Underlying: err,
		}
	}

	if resp.StatusCode >= 400 {
		cat, code, msg := classifyFibError(resp.StatusCode, data)
		return &ports.GatewayError{
			Category:       cat,
			Code:           code,
			GatewayCode:    fmt.Sprintf("HTTP_%d", resp.StatusCode),
			GatewayMessage: msg,
			Retryable:      cat == ports.ErrorCategoryNetworkTimeout || cat == ports.ErrorCategorySoftDecline,
		}
	}

	if out != nil {
		if err := json.Unmarshal(data, out); err != nil {
			return &ports.GatewayError{
				Category: ports.ErrorCategoryGatewayError, Code: "decode_error",
				GatewayMessage: err.Error(),
			}
		}
	}
	return nil
}

func mapPaymentStatus(s string) ports.GatewayPaymentStatus {
	switch s {
	case "UNPAID":
		return ports.GatewayPaymentStatusPending
	case "PAID":
		return ports.GatewayPaymentStatusSucceeded
	case "DECLINED":
		return ports.GatewayPaymentStatusFailed
	case "CANCELLED":
		return ports.GatewayPaymentStatusCancelled
	case "REFUNDED":
		return ports.GatewayPaymentStatusFailed
	default:
		return ports.GatewayPaymentStatusProcessing
	}
}

func classifyFibError(statusCode int, body []byte) (ports.ErrorCategory, string, string) {
	var env fibErrorEnvelope
	_ = json.Unmarshal(body, &env)
	msg := extractFibError(env)

	switch {
	case statusCode >= 500:
		return ports.ErrorCategoryGatewayError, "server_error", msg
	case statusCode == 401 || statusCode == 403:
		return ports.ErrorCategoryGatewayError, "auth_error", msg
	case statusCode == 404:
		return ports.ErrorCategoryGatewayError, "not_found", msg
	case statusCode == 409:
		return ports.ErrorCategorySoftDecline, "conflict", msg
	case statusCode == 422:
		return ports.ErrorCategoryHardDecline, "validation_error", msg
	case statusCode == 429:
		return ports.ErrorCategoryNetworkTimeout, "rate_limited", msg
	default:
		return ports.ErrorCategoryGatewayError, "unknown_error", msg
	}
}

func extractFibError(env fibErrorEnvelope) string {
	if env.Error != nil && env.Error.Error != "" {
		return env.Error.Error
	}
	return "unknown gateway error"
}

func formatAmount(amount int64) string {
	return fmt.Sprintf("%d", amount)
}
