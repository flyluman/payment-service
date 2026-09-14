package bootstrap

import (
	"context"
	"log/slog"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/config"
	"github.com/crownroutes/payment-service/internal/adapters/gateways"
	"github.com/crownroutes/payment-service/internal/adapters/gateways/fib"
	"github.com/crownroutes/payment-service/internal/adapters/gateways/razorpay"
	"github.com/crownroutes/payment-service/internal/adapters/gateways/stripe"
	"github.com/crownroutes/payment-service/internal/domain/gateway"
	"github.com/crownroutes/payment-service/internal/ports"
)

func GatewayRegistry(cfg *config.Config, tcs ports.TenantConfigStore) *gateways.Registry {
	registry := gateways.NewRegistry()

	if tcs == nil {
		panic("bootstrap: TenantConfigStore is required — all gateways need per-tenant config")
	}

	registry.Register("stripe", stripe.New(stripe.Config{
		Timeout: cfg.Gateway.HTTPTimeout,
		ResolveConfig: func(ctx context.Context, tenantID uuid.UUID) (*stripe.TenantConfig, error) {
			raw, err := tcs.Get(ctx, tenantID, "stripe")
			if err != nil {
				return nil, err
			}
			parsed, err := gateway.ParseProviderConfig(gateway.ProviderStripe, raw.Config)
			if err != nil {
				return nil, err
			}
			sc := parsed.(gateway.StripeConfig)
			return &stripe.TenantConfig{
				APIKey:         sc.APIKey,
				BaseURL:        sc.BaseURL,
				PublishableKey: sc.PublishableKey,
			}, nil
		},
	}))
	registry.Register("razorpay", razorpay.New(razorpay.Config{
		Timeout: cfg.Gateway.HTTPTimeout,
		ResolveConfig: func(ctx context.Context, tenantID uuid.UUID) (*razorpay.TenantConfig, error) {
			raw, err := tcs.Get(ctx, tenantID, "razorpay")
			if err != nil {
				return nil, err
			}
			parsed, err := gateway.ParseProviderConfig(gateway.ProviderRazorpay, raw.Config)
			if err != nil {
				return nil, err
			}
			rc := parsed.(gateway.RazorpayConfig)
			return &razorpay.TenantConfig{
				KeyID:          rc.KeyID,
				KeySecret:      rc.KeySecret,
				BaseURL:        rc.BaseURL,
				PublishableKey: rc.PublishableKey,
			}, nil
		},
	}))
	registry.Register("fib", fib.New(fib.Config{
		Timeout: cfg.Gateway.HTTPTimeout,
		ResolveConfig: func(ctx context.Context, tenantID uuid.UUID) (*fib.TenantConfig, error) {
			raw, err := tcs.Get(ctx, tenantID, "fib")
			if err != nil {
				return nil, err
			}
			parsed, err := gateway.ParseProviderConfig(gateway.ProviderFIB, raw.Config)
			if err != nil {
				return nil, err
			}
			fc := parsed.(gateway.FIBConfig)
			return &fib.TenantConfig{
				ClientID:     fc.ClientID,
				ClientSecret: fc.ClientSecret,
				BaseURL:      fc.BaseURL,
				CallbackURL:  fc.CallbackURL,
			}, nil
		},
	}))

	// Register settlement fetchers — each adapter also implements ports.SettlementReportFetcher.
	stripeAdapter, _ := registry.Get("stripe")
	registry.RegisterSettlementFetcher("stripe", stripeAdapter.(ports.SettlementReportFetcher))
	razorpayAdapter, _ := registry.Get("razorpay")
	registry.RegisterSettlementFetcher("razorpay", razorpayAdapter.(ports.SettlementReportFetcher))
	fibAdapter, _ := registry.Get("fib")
	registry.RegisterSettlementFetcher("fib", fibAdapter.(ports.SettlementReportFetcher))

	return registry
}

func ParseLogLevel(level string) slog.Level {
	switch level {
	case "error":
		return slog.LevelError
	case "warn":
		return slog.LevelWarn
	case "debug":
		return slog.LevelDebug
	case "trace":
		return slog.Level(-8)
	default:
		return slog.LevelInfo
	}
}
