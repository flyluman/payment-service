package handlers

import (
	"net/http"

	"github.com/crownroutes/payment-service/internal/api/middleware"
	"github.com/crownroutes/payment-service/internal/api/response"
)

func writeJSON(w http.ResponseWriter, r *http.Request, status int, body any) {
	response.WriteJSON(w, status, body, middleware.RequestIDFromContext(r.Context()))
}

func writeError(w http.ResponseWriter, r *http.Request, status int, code, message string) {
	response.WriteError(w, status, code, message, middleware.RequestIDFromContext(r.Context()))
}
