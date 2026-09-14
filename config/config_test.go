package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

var (
	stringType   = reflect.TypeFor[string]()
	durationType = reflect.TypeFor[time.Duration]()
)

// validatableConfig returns a Config that passes every Validate rule, so a test
// can flip a single field and isolate the rule under test.
func validatableConfig() *Config {
	c := &Config{}
	c.App.Environment = "dev"
	c.Observability.LogLevel = "info"
	c.Outbox.WALLagAlertThresholdMB = 2000
	c.Outbox.WALLagCriticalThresholdMB = 5000
	c.Outbox.PollIntervalSec = 10
	c.Outbox.ClaimTTLSec = 60
	c.Outbox.WorkerCount = 1
	c.RateLimit.FallbackMultiplier = 0.5
	return c
}

func TestValidate_BaselineIsValid(t *testing.T) {
	if err := Validate(validatableConfig()); err != nil {
		t.Fatalf("baseline config should validate, got: %v", err)
	}
}

func TestValidate_SNSPublisherRequiresTopic(t *testing.T) {
	c := validatableConfig()

	// Selecting the sns publisher without a domain-events topic must fail closed
	// rather than boot a relay that can't deliver anything.
	c.Outbox.Publisher = "sns"
	err := Validate(c)
	if err == nil || !strings.Contains(err.Error(), "sns.payment_events_topic") {
		t.Fatalf("expected missing-topic error for sns publisher, got: %v", err)
	}

	c.SNS.PaymentEventsTopic = "arn:aws:sns:us-east-1:123:payment-events"
	if err := Validate(c); err != nil {
		t.Fatalf("sns publisher with a topic should validate, got: %v", err)
	}
}

func TestValidate_RejectsUnknownPublisher(t *testing.T) {
	c := validatableConfig()
	c.Outbox.Publisher = "kafka"
	if err := Validate(c); err == nil || !strings.Contains(err.Error(), "outbox.publisher") {
		t.Fatalf("expected unknown-publisher error, got: %v", err)
	}
}

func TestIntSecondsToDurationHook_StringSeconds(t *testing.T) {
	hook := intSecondsToDurationHook()
	got, err := hook(stringType, durationType, "1800")
	if err != nil {
		t.Fatalf("hook errored on bare number string: %v", err)
	}
	if d, ok := got.(time.Duration); !ok || d != 1800*time.Second {
		t.Fatalf("hook returned %v, want 1800s", got)
	}

	// Non-numeric strings must pass through for StringToTimeDurationHookFunc.
	got, err = hook(stringType, durationType, "30s")
	if err != nil {
		t.Fatalf("hook errored on suffixed string: %v", err)
	}
	if s, ok := got.(string); !ok || s != "30s" {
		t.Fatalf("hook returned %v, want passthrough of \"30s\"", got)
	}
}

const minimalConfigYAML = `app:
  environment: dev
observability:
  log_level: info
outbox:
  wal_lag_alert_threshold_mb: 100
  wal_lag_critical_threshold_mb: 200
  poll_interval_sec: 5
  claim_ttl_sec: 30
  worker_count: 1
rate_limit:
  fallback_multiplier: 0.5
security:
  service_tokens: test-token=11111111-1111-1111-1111-111111111111:22222222-2222-2222-2222-222222222222
`

func TestLoadConfig_CONFIG_PATHCustomFile(t *testing.T) {
	dir := t.TempDir()
	defaultPath := filepath.Join(dir, "config.yaml")
	customPath := filepath.Join(dir, "custom.yaml")
	if err := os.WriteFile(defaultPath, []byte(minimalConfigYAML), 0o600); err != nil {
		t.Fatal(err)
	}
	custom := strings.Replace(minimalConfigYAML, "environment: dev", "environment: dev\n  service_name: from-custom", 1)
	if err := os.WriteFile(customPath, []byte(custom), 0o600); err != nil {
		t.Fatal(err)
	}

	t.Chdir(dir)
	t.Setenv("CONFIG_PATH", customPath)

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.App.ServiceName != "from-custom" {
		t.Fatalf("service_name = %q, want from-custom", cfg.App.ServiceName)
	}
}

func TestLoadConfig_CONFIG_PATHMissingFallsBackToDefault(t *testing.T) {
	dir := t.TempDir()
	defaultPath := filepath.Join(dir, "config.yaml")
	defaultYAML := strings.Replace(minimalConfigYAML, "environment: dev", "environment: dev\n  service_name: from-default", 1)
	if err := os.WriteFile(defaultPath, []byte(defaultYAML), 0o600); err != nil {
		t.Fatal(err)
	}

	t.Chdir(dir)
	t.Setenv("CONFIG_PATH", filepath.Join(dir, "does-not-exist.yaml"))

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.App.ServiceName != "from-default" {
		t.Fatalf("service_name = %q, want from-default", cfg.App.ServiceName)
	}
}
