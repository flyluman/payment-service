package notification

import (
	"context"
)

type Processor struct {
	svc    ProcessorService
	logger Logger
}

type ProcessorService interface {
	ProcessQueue(ctx context.Context) error
}

type Logger interface {
	Warn(msg string, args map[string]any)
}

func NewProcessor(svc ProcessorService, logger Logger) *Processor {
	return &Processor{svc: svc, logger: logger}
}

func (p *Processor) RunOnce(ctx context.Context) error {
	return p.svc.ProcessQueue(ctx)
}
