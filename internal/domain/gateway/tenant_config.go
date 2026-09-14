package gateway

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type Provider string

const (
	ProviderStripe   Provider = "stripe"
	ProviderRazorpay Provider = "razorpay"
	ProviderFIB      Provider = "fib"
)

var ErrNotFound = errors.New("tenant gateway config not found")

var validProviders = map[Provider]bool{
	ProviderStripe:   true,
	ProviderRazorpay: true,
	ProviderFIB:      true,
}

func ValidProvider(p Provider) bool { return validProviders[p] }

// TenantGatewayConfig holds a tenant's gateway-specific credentials and settings.
// The config payload is stored encrypted; the in-memory form uses the
// provider-specific struct for validation and the raw JSON for persistence.
type TenantGatewayConfig struct {
	TenantID      uuid.UUID
	GatewayID     string
	Provider      Provider
	Config        json.RawMessage // provider-specific JSON payload
	ConfigVersion int
	IsActive      bool
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// ProviderConfig is the common interface for provider-specific config shapes.
type ProviderConfig interface {
	Provider() Provider
	Validate() error
}

// StripeConfig holds Stripe-specific credentials.
type StripeConfig struct {
	APIKey         string `json:"api_key"`
	BaseURL        string `json:"base_url,omitempty"`
	WebhookSecret  string `json:"webhook_secret,omitempty"`
	PublishableKey string `json:"publishable_key,omitempty"`
}

func (c StripeConfig) Provider() Provider { return ProviderStripe }
func (c StripeConfig) Validate() error {
	if c.APIKey == "" {
		return errors.New("stripe: api_key is required")
	}
	if len(c.APIKey) < 10 {
		return errors.New("stripe: api_key looks too short")
	}
	return nil
}

// RazorpayConfig holds Razorpay-specific credentials.
type RazorpayConfig struct {
	KeyID          string `json:"key_id"`
	KeySecret      string `json:"key_secret"`
	BaseURL        string `json:"base_url,omitempty"`
	PublishableKey string `json:"publishable_key,omitempty"`
}

func (c RazorpayConfig) Provider() Provider { return ProviderRazorpay }
func (c RazorpayConfig) Validate() error {
	if c.KeyID == "" {
		return errors.New("razorpay: key_id is required")
	}
	if c.KeySecret == "" {
		return errors.New("razorpay: key_secret is required")
	}
	return nil
}

// FIBConfig holds FIB-specific credentials.
type FIBConfig struct {
	ClientID     string `json:"client_id"`
	ClientSecret string `json:"client_secret"`
	BaseURL      string `json:"base_url"`
	CallbackURL  string `json:"callback_url,omitempty"`
}

func (c FIBConfig) Provider() Provider { return ProviderFIB }
func (c FIBConfig) Validate() error {
	if c.ClientID == "" {
		return errors.New("fib: client_id is required")
	}
	if c.ClientSecret == "" {
		return errors.New("fib: client_secret is required")
	}
	if c.BaseURL == "" {
		return errors.New("fib: base_url is required")
	}
	return nil
}

func ExtractWebhookSecret(provider Provider, raw json.RawMessage) (string, error) {
	cfg, err := ParseProviderConfig(provider, raw)
	if err != nil {
		return "", err
	}
	switch v := cfg.(type) {
	case StripeConfig:
		return v.WebhookSecret, nil
	case RazorpayConfig:
		return v.KeySecret, nil
	case FIBConfig:
		return "", nil
	default:
		return "", fmt.Errorf("unsupported provider: %s", provider)
	}
}



// ParseProviderConfig validates and returns the provider-specific config from raw JSON.
func ParseProviderConfig(provider Provider, raw json.RawMessage) (ProviderConfig, error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("provider %s: config payload is empty", provider)
	}

	switch provider {
	case ProviderStripe:
		var cfg StripeConfig
		if err := json.Unmarshal(raw, &cfg); err != nil {
			return nil, fmt.Errorf("stripe: unmarshal config: %w", err)
		}
		if err := cfg.Validate(); err != nil {
			return nil, err
		}
		return cfg, nil

	case ProviderRazorpay:
		var cfg RazorpayConfig
		if err := json.Unmarshal(raw, &cfg); err != nil {
			return nil, fmt.Errorf("razorpay: unmarshal config: %w", err)
		}
		if err := cfg.Validate(); err != nil {
			return nil, err
		}
		return cfg, nil

	case ProviderFIB:
		var cfg FIBConfig
		if err := json.Unmarshal(raw, &cfg); err != nil {
			return nil, fmt.Errorf("fib: unmarshal config: %w", err)
		}
		if err := cfg.Validate(); err != nil {
			return nil, err
		}
		return cfg, nil

	default:
		return nil, fmt.Errorf("unsupported provider: %s", provider)
	}
}
