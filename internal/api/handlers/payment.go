package handlers

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/api/middleware"
	"github.com/crownroutes/payment-service/internal/app/idempotency"
	"github.com/crownroutes/payment-service/internal/app/payment"
	"github.com/crownroutes/payment-service/internal/domain/transaction"
	"github.com/crownroutes/payment-service/internal/ports"
)

type PaymentService interface {
	Create(ctx context.Context, in payment.CreateInput) (payment.CreateResult, error)
	ProcessPayment(ctx context.Context, transactionID uuid.UUID) (*transaction.Txn, error)
	GetPayment(ctx context.Context, id uuid.UUID) (*transaction.Txn, error)
	GetGatewayMetadata(ctx context.Context, transactionID uuid.UUID) (map[string]any, error)
	ListTransactions(ctx context.Context, filter ports.TransactionFilter) (*ports.TransactionListResult, error)
	Authorize(ctx context.Context, transactionID uuid.UUID) (*transaction.Txn, error)
	Capture(ctx context.Context, transactionID uuid.UUID) (*transaction.Txn, error)
	Settle(ctx context.Context, transactionID uuid.UUID) (*transaction.Txn, error)
}

type PaymentHandler struct {
	svc PaymentService
}

func NewPaymentHandler(svc PaymentService) *PaymentHandler {
	return &PaymentHandler{svc: svc}
}

type createPaymentRequest struct {
	GatewayID     string         `json:"gateway_id"`
	Amount        int64          `json:"amount"`
	Currency      string         `json:"currency"`
	PaymentMethod string         `json:"payment_method"`
	CaptureMode   string         `json:"capture_mode,omitempty"`
	CustomerID    string         `json:"customer_id,omitempty"`
	CustomerEmail string         `json:"customer_email,omitempty"`
	Description   string         `json:"description,omitempty"`
	Metadata      map[string]any `json:"metadata,omitempty"`
	CallbackURL   string         `json:"callback_url"`
	RedirectURL   string         `json:"redirect_url"`
}

type createPaymentResponse struct {
	TransactionID   string         `json:"transaction_id"`
	Token           string         `json:"token"`
	Status          string         `json:"status"`
	GatewayMetadata map[string]any `json:"gateway_metadata,omitempty"`
	FeeBreakdown    any            `json:"fee_breakdown,omitempty"`
}

type paymentResponse struct {
	TransactionID      string         `json:"transaction_id"`
	Status             string         `json:"status"`
	Amount             int64          `json:"amount"`
	Currency           string         `json:"currency"`
	PaymentMethod      string         `json:"payment_method"`
	GatewayReferenceID string         `json:"gateway_reference_id,omitempty"`
	CustomerID         string         `json:"customer_id,omitempty"`
	CustomerEmail      string         `json:"customer_email,omitempty"`
	Description        string         `json:"description,omitempty"`
	Metadata           map[string]any `json:"metadata,omitempty"`
	GatewayMetadata    map[string]any `json:"gateway_metadata,omitempty"`
	GatewayAmount      *int64         `json:"gateway_amount,omitempty"`
	GatewayCurrency    string         `json:"gateway_currency,omitempty"`
	FeeBreakdown       any            `json:"fee_breakdown,omitempty"`
	CreatedAt          string         `json:"created_at"`
}

func (h *PaymentHandler) Create(w http.ResponseWriter, r *http.Request) {
	tenantIDStr := middleware.TenantIDFromContext(r.Context())
	if tenantIDStr == "" {
		writeError(w, r, http.StatusUnauthorized, "unauthorized", "missing tenant")
		return
	}
	tenantID, err := uuid.Parse(tenantIDStr)
	if err != nil {
		writeError(w, r, http.StatusUnauthorized, "unauthorized", "invalid tenant")
		return
	}

	userIDStr := middleware.UserIDFromContext(r.Context())
	var userID uuid.UUID
	if userIDStr != "" {
		userID, err = uuid.Parse(userIDStr)
		if err != nil {
			writeError(w, r, http.StatusUnauthorized, "unauthorized", "invalid user")
			return
		}
	}

	var req createPaymentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, r, http.StatusBadRequest, "invalid_request_body", "request body is not valid JSON")
		return
	}

	if req.GatewayID == "" {
		writeError(w, r, http.StatusBadRequest, "missing_gateway_id", "gateway_id is required")
		return
	}
	if req.Amount <= 0 {
		writeError(w, r, http.StatusBadRequest, "invalid_amount", "amount must be positive")
		return
	}
	if req.Currency == "" || req.PaymentMethod == "" {
		writeError(w, r, http.StatusBadRequest, "missing_fields", "currency and payment_method are required")
		return
	}
	if req.CallbackURL == "" {
		writeError(w, r, http.StatusBadRequest, "missing_callback_url", "callback_url is required")
		return
	}
	if req.RedirectURL == "" {
		writeError(w, r, http.StatusBadRequest, "missing_redirect_url", "redirect_url is required")
		return
	}

	idemKey := r.Header.Get("Idempotency-Key")
	if idemKey == "" {
		writeError(w, r, http.StatusBadRequest, "missing_idempotency_key", "Idempotency-Key header is required")
		return
	}

	var customerID uuid.UUID
	if req.CustomerID != "" {
		customerID, err = uuid.Parse(req.CustomerID)
		if err != nil {
			writeError(w, r, http.StatusBadRequest, "invalid_customer_id", "customer_id must be a valid UUID")
			return
		}
	}

	result, err := h.svc.Create(r.Context(), payment.CreateInput{
		TenantID:       tenantID,
		UserID:         userID,
		GatewayID:      req.GatewayID,
		Amount:         req.Amount,
		Currency:       req.Currency,
		PaymentMethod:  transaction.PaymentMethod(req.PaymentMethod),
		CaptureMode:    transaction.CaptureMode(req.CaptureMode),
		CustomerID:     customerID,
		CustomerEmail:  req.CustomerEmail,
		Description:    req.Description,
		Metadata:       req.Metadata,
		CallbackURL:    req.CallbackURL,
		RedirectURL:    req.RedirectURL,
		IdempotencyKey: idemKey,
	})
	if err != nil {
		switch {
		case errors.Is(err, payment.ErrNoGateway):
			writeError(w, r, http.StatusUnprocessableEntity, "no_eligible_gateway", "no gateway can process this payment")
		case errors.Is(err, idempotency.ErrKeyRequired):
			writeError(w, r, http.StatusBadRequest, "missing_idempotency_key", "Idempotency-Key header is required")
		default:
			writeError(w, r, http.StatusInternalServerError, "create_failed", "could not create payment")
		}
		return
	}

	switch result.Verdict {
	case idempotency.Created:
		statusCode := http.StatusCreated
		resp := createPaymentResponse{
			TransactionID: result.Transaction.ID.String(),
			Token:         result.Token,
			Status:        string(result.Transaction.Status),
			FeeBreakdown:  result.Transaction.FeeBreakdown,
		}
		meta, _ := h.svc.GetGatewayMetadata(r.Context(), result.Transaction.ID)
		if meta != nil {
			resp.GatewayMetadata = meta
		}
		writeJSON(w, r, statusCode, resp)
	case idempotency.Replayed:
		resp := createPaymentResponse{
			TransactionID: result.Transaction.ID.String(),
			Token:         result.Token,
			Status:        string(result.Transaction.Status),
			FeeBreakdown:  result.Transaction.FeeBreakdown,
		}
		meta, _ := h.svc.GetGatewayMetadata(r.Context(), result.Transaction.ID)
		if meta != nil {
			resp.GatewayMetadata = meta
		}
		writeJSON(w, r, http.StatusOK, resp)
	case idempotency.InProgress:
		writeError(w, r, http.StatusConflict, "idempotency_in_progress", "a request with this idempotency key is already in progress")
	case idempotency.KeyReused:
		writeError(w, r, http.StatusConflict, "idempotency_key_reused", "idempotency key reused with a different request body")
	default:
		writeError(w, r, http.StatusInternalServerError, "create_failed", "could not create payment")
	}
}

func (h *PaymentHandler) Get(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "invalid_id", "transaction id must be a valid UUID")
		return
	}

	txn, err := h.svc.GetPayment(r.Context(), id)
	if err != nil {
		writeError(w, r, http.StatusNotFound, "not_found", "payment not found")
		return
	}

	tenantID := middleware.TenantIDFromContext(r.Context())
	if tenantID != "" {
		if txn.TenantID.String() != tenantID {
			writeError(w, r, http.StatusNotFound, "not_found", "payment not found")
			return
		}
	} else {
		token := r.URL.Query().Get("token")
		if token == "" || !payment.VerifyToken(token, txn.TokenHash) {
			writeError(w, r, http.StatusUnauthorized, "unauthorized", "invalid or missing token")
			return
		}
	}

	resp := toPaymentResponse(txn)
	meta, _ := h.svc.GetGatewayMetadata(r.Context(), id)
	if meta != nil {
		resp.GatewayMetadata = meta
	}
	writeJSON(w, r, http.StatusOK, resp)
}

func toPaymentResponse(t *transaction.Txn) paymentResponse {
	resp := paymentResponse{
		TransactionID:      t.ID.String(),
		Status:             string(t.Status),
		Amount:             t.Amount,
		Currency:           t.Currency,
		PaymentMethod:      string(t.PaymentMethod),
		GatewayReferenceID: t.GatewayReferenceID,
		CustomerEmail:      t.CustomerEmail,
		Description:        t.Description,
		Metadata:           t.Metadata,
		GatewayAmount:      t.GatewayAmount,
		GatewayCurrency:    t.GatewayCurrency,
		CreatedAt:          t.CreatedAt.Format(time.RFC3339Nano),
	}
	if t.CustomerID != uuid.Nil {
		resp.CustomerID = t.CustomerID.String()
	}
	if t.FeeBreakdown != nil {
		resp.FeeBreakdown = t.FeeBreakdown
	}
	return resp
}

func (h *PaymentHandler) List(w http.ResponseWriter, r *http.Request) {
	principal, ok := middleware.PrincipalFromContext(r.Context())
	if !ok {
		writeError(w, r, http.StatusUnauthorized, "unauthorized", "missing authentication")
		return
	}

	filter := ports.TransactionFilter{
		Limit: 50,
	}
	if tid := r.URL.Query().Get("tenant_id"); tid != "" {
		u, err := uuid.Parse(tid)
		if err != nil {
			writeError(w, r, http.StatusBadRequest, "invalid_tenant_id", "tenant_id must be a valid UUID")
			return
		}
		filter.TenantID = &u
	}

	// Tenant isolation: service tokens can only query their own tenant.
	if principal.Role == middleware.RoleService {
		authTenantID, _ := uuid.Parse(principal.TenantID)
		if filter.TenantID != nil && *filter.TenantID != authTenantID {
			writeError(w, r, http.StatusForbidden, "tenant_mismatch", "service token cannot query other tenants")
			return
		}
		filter.TenantID = &authTenantID
	}
	if uid := r.URL.Query().Get("user_id"); uid != "" {
		u, err := uuid.Parse(uid)
		if err != nil {
			writeError(w, r, http.StatusBadRequest, "invalid_user_id", "user_id must be a valid UUID")
			return
		}
		filter.UserID = &u
	}
	if s := r.URL.Query().Get("status"); s != "" {
		filter.Status = strings.Split(s, ",")
	}
	if g := r.URL.Query().Get("gateway_id"); g != "" {
		filter.GatewayID = strings.Split(g, ",")
	}
	if minStr := r.URL.Query().Get("min_amount"); minStr != "" {
		v, err := strconv.ParseInt(minStr, 10, 64)
		if err != nil {
			writeError(w, r, http.StatusBadRequest, "invalid_min_amount", "min_amount must be an integer")
			return
		}
		filter.MinAmount = &v
	}
	if maxStr := r.URL.Query().Get("max_amount"); maxStr != "" {
		v, err := strconv.ParseInt(maxStr, 10, 64)
		if err != nil {
			writeError(w, r, http.StatusBadRequest, "invalid_max_amount", "max_amount must be an integer")
			return
		}
		filter.MaxAmount = &v
	}
	if c := r.URL.Query().Get("currency"); c != "" {
		filter.Currency = &c
	}
	if pm := r.URL.Query().Get("payment_method"); pm != "" {
		filter.PaymentMethod = strings.Split(pm, ",")
	}
	if from := r.URL.Query().Get("date_from"); from != "" {
		t, err := time.Parse(time.RFC3339, from)
		if err != nil {
			writeError(w, r, http.StatusBadRequest, "invalid_date_from", "date_from must be RFC3339")
			return
		}
		filter.DateFrom = &t
	}
	if to := r.URL.Query().Get("date_to"); to != "" {
		t, err := time.Parse(time.RFC3339, to)
		if err != nil {
			writeError(w, r, http.StatusBadRequest, "invalid_date_to", "date_to must be RFC3339")
			return
		}
		filter.DateTo = &t
	}
	if cursor := r.URL.Query().Get("cursor"); cursor != "" {
		filter.Cursor = &cursor
	}
	if limStr := r.URL.Query().Get("limit"); limStr != "" {
		v, err := strconv.Atoi(limStr)
		if err != nil || v <= 0 || v > 100 {
			writeError(w, r, http.StatusBadRequest, "invalid_limit", "limit must be between 1 and 100")
			return
		}
		filter.Limit = v
	}

	result, err := h.svc.ListTransactions(r.Context(), filter)
	if err != nil {
		writeError(w, r, http.StatusInternalServerError, "list_failed", err.Error())
		return
	}

	if strings.Contains(r.Header.Get("Accept"), "text/csv") {
		h.writeCSV(w, r, result)
		return
	}

	type listItem struct {
		TransactionID string `json:"transaction_id"`
		UserID        string `json:"user_id,omitempty"`
		Amount        int64  `json:"amount"`
		Currency      string `json:"currency"`
		Status        string `json:"status"`
		GatewayID     string `json:"gateway_id,omitempty"`
		PaymentMethod string `json:"payment_method,omitempty"`
		CreatedAt     string `json:"created_at"`
	}
	data := make([]listItem, 0, len(result.Transactions))
	for _, t := range result.Transactions {
		item := listItem{
			TransactionID: t.ID.String(),
			Amount:        t.Amount,
			Currency:      t.Currency,
			Status:        t.Status,
			GatewayID:     t.GatewayID,
			PaymentMethod: t.PaymentMethod,
			CreatedAt:     t.CreatedAt.Format(time.RFC3339Nano),
		}
		if t.UserID != nil {
			item.UserID = t.UserID.String()
		}
		data = append(data, item)
	}
	writeJSON(w, r, http.StatusOK, map[string]any{
		"data":        data,
		"has_more":    result.HasMore,
		"next_cursor": result.NextCursor,
	})
}

func (h *PaymentHandler) writeCSV(w http.ResponseWriter, r *http.Request, result *ports.TransactionListResult) {
	w.Header().Set("Content-Type", "text/csv")
	w.Header().Set("Content-Disposition", "attachment;filename=transactions.csv")
	writer := csv.NewWriter(w)
	defer writer.Flush()
	writer.Write([]string{"id", "user_id", "amount", "currency", "status", "gateway_id", "payment_method", "created_at"})
	for _, t := range result.Transactions {
		userID := ""
		if t.UserID != nil {
			userID = t.UserID.String()
		}
		writer.Write([]string{
			t.ID.String(), userID,
			strconv.FormatInt(t.Amount, 10), t.Currency, t.Status,
			t.GatewayID, t.PaymentMethod, t.CreatedAt.Format(time.RFC3339Nano),
		})
	}
}

func (h *PaymentHandler) Authorize(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "invalid_id", "transaction id must be a valid UUID")
		return
	}

	txn, err := h.svc.Authorize(r.Context(), id)
	if err != nil {
		switch {
		case errors.Is(err, payment.ErrInvalidTransition):
			writeError(w, r, http.StatusConflict, "invalid_transition", err.Error())
		default:
			writeError(w, r, http.StatusInternalServerError, "authorize_failed", "could not authorize payment")
		}
		return
	}

	writeJSON(w, r, http.StatusOK, toPaymentResponse(txn))
}

func (h *PaymentHandler) Capture(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "invalid_id", "transaction id must be a valid UUID")
		return
	}

	txn, err := h.svc.Capture(r.Context(), id)
	if err != nil {
		switch {
		case errors.Is(err, payment.ErrInvalidTransition):
			writeError(w, r, http.StatusConflict, "invalid_transition", err.Error())
		default:
			writeError(w, r, http.StatusInternalServerError, "capture_failed", "could not capture payment")
		}
		return
	}

	writeJSON(w, r, http.StatusOK, toPaymentResponse(txn))
}

func (h *PaymentHandler) Settle(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, r, http.StatusBadRequest, "invalid_id", "transaction id must be a valid UUID")
		return
	}

	txn, err := h.svc.Settle(r.Context(), id)
	if err != nil {
		switch {
		case errors.Is(err, payment.ErrInvalidTransition):
			writeError(w, r, http.StatusConflict, "invalid_transition", err.Error())
		default:
			writeError(w, r, http.StatusInternalServerError, "settle_failed", "could not settle payment")
		}
		return
	}

	writeJSON(w, r, http.StatusOK, toPaymentResponse(txn))
}
