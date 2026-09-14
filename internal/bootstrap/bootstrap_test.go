package bootstrap

import (
	"context"
	"log/slog"
	"testing"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/config"
	"github.com/crownroutes/payment-service/internal/domain/gateway"
	"github.com/crownroutes/payment-service/internal/ports"
)

func TestParseLogLevel(t *testing.T) {
	tests := []struct {
		input    string
		expected slog.Level
	}{
		{"error", slog.LevelError},
		{"warn", slog.LevelWarn},
		{"debug", slog.LevelDebug},
		{"trace", slog.Level(-8)},
		{"info", slog.LevelInfo},
		{"", slog.LevelInfo},
		{"random", slog.LevelInfo},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := ParseLogLevel(tt.input)
			if got != tt.expected {
				t.Fatalf("ParseLogLevel(%q) = %v, want %v", tt.input, got, tt.expected)
			}
		})
	}
}

func TestGatewayRegistry_PanicsOnNilStore(t *testing.T) {
	defer func() {
		if rec := recover(); rec == nil {
			t.Fatal("expected panic for nil TenantConfigStore")
		}
	}()

	cfg := &config.Config{}
	GatewayRegistry(cfg, nil)
}

func TestGatewayRegistry_RegistersThreeGateways(t *testing.T) {
	cfg := &config.Config{}
	cfg.Gateway.HTTPTimeout = 30

	tcs := &fakeTenantConfigStore{}
	registry := GatewayRegistry(cfg, tcs)

	for _, id := range []string{"stripe", "razorpay", "fib"} {
		_, err := registry.Get(id)
		if err != nil {
			t.Fatalf("gateway %q not registered: %v", id, err)
		}
	}
}

func TestGatewayRegistry_GetUnknown(t *testing.T) {
	cfg := &config.Config{}
	tcs := &fakeTenantConfigStore{}
	registry := GatewayRegistry(cfg, tcs)

	_, err := registry.Get("unknown")
	if err == nil {
		t.Fatal("expected error for unknown gateway")
	}
}

var _ ports.TenantConfigStore = (*fakeTenantConfigStore)(nil)

type fakeTenantConfigStore struct{}

func (f *fakeTenantConfigStore) Get(_ context.Context, _ uuid.UUID, _ string) (*gateway.TenantGatewayConfig, error) {
	return nil, nil
}
func (f *fakeTenantConfigStore) ListByTenant(_ context.Context, _ uuid.UUID) ([]*gateway.TenantGatewayConfig, error) {
	return nil, nil
}
func (f *fakeTenantConfigStore) ListActiveByTenant(_ context.Context, _ uuid.UUID) ([]*gateway.TenantGatewayConfig, error) {
	return nil, nil
}
func (f *fakeTenantConfigStore) Upsert(_ context.Context, _ *gateway.TenantGatewayConfig) (int, error) {
	return 0, nil
}
func (f *fakeTenantConfigStore) SetActive(_ context.Context, _ uuid.UUID, _ string, _ bool) error {
	return nil
}
func (f *fakeTenantConfigStore) Delete(_ context.Context, _ uuid.UUID, _ string) error {
	return nil
}
