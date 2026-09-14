package callback

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"github.com/crownroutes/payment-service/internal/ports"
)

type eventPayload struct {
	TransactionID string `json:"transaction_id"`
	Status        string `json:"status"`
	CallbackURL   string `json:"callback_url"`
}

// Dispatcher handles TRANSACTION_CALLBACK events by POSTing to the booking engine.
// Implements outbox.Handler.
type Dispatcher struct {
	client *http.Client
	log    ports.Logger
}

func NewDispatcher(client *http.Client, log ports.Logger) *Dispatcher {
	return &Dispatcher{client: client, log: log}
}

func (d *Dispatcher) Handle(ctx context.Context, event ports.PendingEvent) error {
	var payload eventPayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return fmt.Errorf("callback: unmarshal payload: %w", err)
	}

	if payload.CallbackURL == "" {
		d.log.Warn("callback: empty url, skipping", map[string]any{
			"event_id":       event.ID.String(),
			"transaction_id": payload.TransactionID,
		})
		return nil
	}

	body, err := json.Marshal(map[string]string{
		"transaction_id": payload.TransactionID,
		"status":         payload.Status,
	})
	if err != nil {
		return fmt.Errorf("callback: marshal body: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, payload.CallbackURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("callback: create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := d.client.Do(req)
	if err != nil {
		return fmt.Errorf("callback: http call: %w", err)
	}
	resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("callback: non-2xx status %d", resp.StatusCode)
	}

	d.log.Info("callback.sent", map[string]any{
		"transaction_id": payload.TransactionID,
		"status":         payload.Status,
		"callback_url":   redactCallbackURL(payload.CallbackURL),
	})
	return nil
}

func redactCallbackURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	return u.Scheme + "://" + u.Host + u.Path
}
