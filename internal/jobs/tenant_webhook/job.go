package tenant_webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/crownroutes/payment-service/internal/ports"
)

type Worker struct {
	reader      ports.TenantWebhookDeliveryReader
	updater     ports.TenantWebhookDeliveryUpdater
	config      ports.TenantWebhookConfigStore
	client      *http.Client
	logger      logger
	maxAttempts int
	maxBackoff  time.Duration
}

type logger interface {
	Warn(msg string, args map[string]any)
}

type Config struct {
	MaxAttempts    int
	MaxBackoffSec  int
	HTTPTimeoutSec int
}

func NewWorker(
	reader ports.TenantWebhookDeliveryReader,
	updater ports.TenantWebhookDeliveryUpdater,
	config ports.TenantWebhookConfigStore,
	logger logger,
	cfg Config,
) *Worker {
	timeout := time.Duration(cfg.HTTPTimeoutSec) * time.Second
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	maxAttempts := cfg.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 10
	}
	maxBackoff := time.Duration(cfg.MaxBackoffSec) * time.Second
	if maxBackoff <= 0 {
		maxBackoff = 30 * time.Second
	}
	return &Worker{
		reader:      reader,
		updater:     updater,
		config:      config,
		client:      &http.Client{Timeout: timeout},
		logger:      logger,
		maxAttempts: maxAttempts,
		maxBackoff:  maxBackoff,
	}
}

func (w *Worker) RunOnce(ctx context.Context) error {
	deliveries, err := w.reader.ListPending(ctx, 20)
	if err != nil {
		return fmt.Errorf("list pending deliveries: %w", err)
	}

	for _, d := range deliveries {
		if err := w.deliver(ctx, d); err != nil {
			w.logger.Warn("tenant_webhook.delivery_failed", map[string]any{
				"delivery_id": d.ID.String(),
				"tenant_id":   d.TenantID.String(),
				"error":       err.Error(),
			})
		}
	}
	return nil
}

func (w *Worker) deliver(ctx context.Context, d ports.TenantWebhookDelivery) error {
	cfg, err := w.config.Get(ctx, d.TenantID)
	if err != nil {
		return w.updater.MarkFailed(ctx, d.ID, d.Attempts+1, fmt.Sprintf("get config: %v", err), time.Now().Add(w.maxBackoff))
	}
	if cfg == nil || !cfg.IsActive {
		return w.updater.MarkFailed(ctx, d.ID, d.Attempts+1, "webhook config not found or inactive", time.Now().Add(w.maxBackoff))
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.EndpointURL, bytes.NewReader(d.Payload))
	if err != nil {
		return w.updater.MarkFailed(ctx, d.ID, d.Attempts+1, fmt.Sprintf("build request: %v", err), time.Now().Add(w.maxBackoff))
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Webhook-Event", d.EventType)
	req.Header.Set("X-Delivery-ID", d.ID.String())

	if cfg.SigningSecret != "" {
		sig := signPayload(d.Payload, cfg.SigningSecret)
		req.Header.Set("X-Webhook-Signature", sig)
	}

	resp, err := w.client.Do(req)
	if err != nil {
		return w.updater.MarkFailed(ctx, d.ID, d.Attempts+1, fmt.Sprintf("http request: %v", err), time.Now().Add(w.maxBackoff))
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return w.updater.MarkDelivered(ctx, d.ID)
	}

	return w.updater.MarkFailed(ctx, d.ID, d.Attempts+1, fmt.Sprintf("HTTP %d", resp.StatusCode), time.Now().Add(w.maxBackoff))
}

func signPayload(payload []byte, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(payload)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}
