package config

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

var (
	stringType   = reflect.TypeOf("")
	durationType = reflect.TypeOf(time.Duration(0))
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


