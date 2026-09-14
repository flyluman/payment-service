package callback

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/ports"
)

type noopLog struct{}

func (noopLog) Info(string, map[string]any)  {}
func (noopLog) Warn(string, map[string]any)  {}
func (noopLog) Error(string, map[string]any, error) {}
func (noopLog) Debug(string, map[string]any) {}
func (noopLog) Trace(string, map[string]any) {}
func (noopLog) With(map[string]any) ports.Logger { return noopLog{} }

func TestHandle_EmptyCallbackURL(t *testing.T) {
	d := NewDispatcher(&http.Client{}, noopLog{})
	payload, _ := json.Marshal(eventPayload{
		TransactionID: uuid.NewString(),
		Status:        "SUCCEEDED",
		CallbackURL:   "",
	})

	err := d.Handle(context.Background(), ports.PendingEvent{Payload: payload})
	if err != nil {
		t.Fatalf("empty callback URL should return nil, got %v", err)
	}
}

func TestHandle_InvalidJSON(t *testing.T) {
	d := NewDispatcher(&http.Client{}, noopLog{})
	err := d.Handle(context.Background(), ports.PendingEvent{Payload: []byte("bad")})
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestHandle_Success(t *testing.T) {
	var receivedBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedBody, _ = io.ReadAll(r.Body)
		if r.Method != http.MethodPost {
			t.Fatalf("expected POST, got %s", r.Method)
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Fatalf("expected application/json, got %s", r.Header.Get("Content-Type"))
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	d := NewDispatcher(srv.Client(), noopLog{})
	payload, _ := json.Marshal(eventPayload{
		TransactionID: uuid.NewString(),
		Status:        "SUCCEEDED",
		CallbackURL:   srv.URL,
	})

	err := d.Handle(context.Background(), ports.PendingEvent{Payload: payload})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var body map[string]string
	json.Unmarshal(receivedBody, &body)
	if body["transaction_id"] == "" {
		t.Fatal("expected transaction_id in body")
	}
	if body["status"] != "SUCCEEDED" {
		t.Fatalf("expected status SUCCEEDED, got %s", body["status"])
	}
}

func TestHandle_Non2xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	d := NewDispatcher(srv.Client(), noopLog{})
	payload, _ := json.Marshal(eventPayload{
		TransactionID: uuid.NewString(),
		Status:        "FAILED",
		CallbackURL:   srv.URL,
	})

	err := d.Handle(context.Background(), ports.PendingEvent{Payload: payload})
	if err == nil || !strings.Contains(err.Error(), "500") {
		t.Fatalf("expected 500 error, got %v", err)
	}
}

func TestHandle_ConnectionRefused(t *testing.T) {
	d := NewDispatcher(&http.Client{}, noopLog{})
	payload, _ := json.Marshal(eventPayload{
		TransactionID: uuid.NewString(),
		Status:        "SUCCEEDED",
		CallbackURL:   "http://127.0.0.1:1",
	})

	err := d.Handle(context.Background(), ports.PendingEvent{Payload: payload})
	if err == nil {
		t.Fatal("expected error for connection refused")
	}
}
