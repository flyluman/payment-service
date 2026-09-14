package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/app/payment"
	"github.com/crownroutes/payment-service/internal/domain/transaction"
	"github.com/crownroutes/payment-service/internal/ports"
)

type TransactionByIDGetter interface {
	GetByID(ctx context.Context, id uuid.UUID) (*transaction.Txn, error)
}

type successTemplateData struct {
	TransactionID string
	ReturnURL     string
	Amount        int64
	Currency      string
}

type failureTemplateData struct {
	TransactionID string
	ReturnURL     string
	Amount        int64
	Currency      string
	Message       string
}

type GatewayMetadataReader interface {
	GetGatewayMetadata(ctx context.Context, transactionID uuid.UUID) (map[string]any, error)
}

type PayHandler struct {
	txnByID           TransactionByIDGetter
	templates         *template.Template
	log               ports.Logger
	gatewayMetaReader GatewayMetadataReader
}

func NewPayHandler(txnByID TransactionByIDGetter, templates *template.Template, log ports.Logger, gatewayMetaReader GatewayMetadataReader) *PayHandler {
	return &PayHandler{
		txnByID:           txnByID,
		templates:         templates,
		log:               log,
		gatewayMetaReader: gatewayMetaReader,
	}
}

func (h *PayHandler) auth(r *http.Request) (*transaction.Txn, error) {
	txnIDStr := r.PathValue("transaction_id")
	if txnIDStr == "" {
		txnIDStr = r.URL.Query().Get("transaction_id")
	}
	if txnIDStr == "" {
		return nil, fmt.Errorf("missing transaction_id")
	}
	txnID, err := uuid.Parse(txnIDStr)
	if err != nil {
		return nil, fmt.Errorf("invalid transaction_id")
	}

	token := r.URL.Query().Get("token")
	if token == "" {
		return nil, fmt.Errorf("missing token")
	}

	txn, err := h.txnByID.GetByID(r.Context(), txnID)
	if err != nil {
		return nil, fmt.Errorf("transaction not found")
	}
	if !payment.VerifyToken(token, txn.TokenHash) {
		return nil, fmt.Errorf("invalid token")
	}
	return txn, nil
}

func (h *PayHandler) ServeCheckout(w http.ResponseWriter, r *http.Request) {
	txn, err := h.auth(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	pageData := map[string]any{
		"transactionId": txn.ID.String(),
		"redirectUrl":   txn.RedirectURL,
		"token":         r.URL.Query().Get("token"),
	}
	if txn.FeeBreakdown != nil {
		pageData["fee_breakdown"] = txn.FeeBreakdown
	}
	if h.gatewayMetaReader != nil {
		if meta, err := h.gatewayMetaReader.GetGatewayMetadata(r.Context(), txn.ID); err == nil && meta != nil {
			pageData["rawMetadata"] = meta
		}
	}

	pd, _ := json.Marshal(pageData)
	data := struct{ PageData template.JS }{PageData: template.JS(pd)}
	if err := h.templates.ExecuteTemplate(w, "checkout.html", data); err != nil {
		http.Error(w, "template error", http.StatusInternalServerError)
	}
}

func (h *PayHandler) Success(w http.ResponseWriter, r *http.Request) {
	txn, err := h.auth(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	data := successTemplateData{
		TransactionID: txn.ID.String(),
		ReturnURL:     txn.RedirectURL,
		Amount:        txn.Amount,
		Currency:      txn.Currency,
	}
	if err := h.templates.ExecuteTemplate(w, "success.html", data); err != nil {
		http.Error(w, "template error", http.StatusInternalServerError)
	}
}

func (h *PayHandler) Failure(w http.ResponseWriter, r *http.Request) {
	txn, err := h.auth(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	msg := ""
	if txn.FailureReason != nil {
		msg = txn.FailureReason.GatewayMessage
	}
	if msg == "" {
		msg = "Your payment could not be processed."
	}
	data := failureTemplateData{
		TransactionID: txn.ID.String(),
		ReturnURL:     txn.RedirectURL,
		Amount:        txn.Amount,
		Currency:      txn.Currency,
		Message:       msg,
	}
	if err := h.templates.ExecuteTemplate(w, "failure.html", data); err != nil {
		http.Error(w, "template error", http.StatusInternalServerError)
	}
}
