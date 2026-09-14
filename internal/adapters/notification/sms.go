package notification

import (
	"context"
	"fmt"
)

type StubSMSSender struct {
	provider string
	apiKey   string
	from     string
}

type SMSConfig struct {
	Provider string
	APIKey   string
	From     string
}

func NewStubSMSSender(cfg SMSConfig) *StubSMSSender {
	return &StubSMSSender{
		provider: cfg.Provider,
		apiKey:   cfg.APIKey,
		from:     cfg.From,
	}
}

func (s *StubSMSSender) SendSMS(ctx context.Context, to, text string) error {
	if s.apiKey == "" {
		return fmt.Errorf("SMS provider not configured")
	}
	return nil
}
