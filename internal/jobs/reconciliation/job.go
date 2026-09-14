package reconciliation

import (
	"context"
	"time"

	reconapp "github.com/crownroutes/payment-service/internal/app/reconciliation"
	"github.com/crownroutes/payment-service/internal/ports"
)

type Scheduler struct {
	store  ports.ReconciliationStore
	svc    *reconapp.Service
	log    ports.Logger
	config Config
}

type Config struct {
	Interval  time.Duration
	BatchSize int
}

func NewScheduler(store ports.ReconciliationStore, svc *reconapp.Service, log ports.Logger, cfg Config) *Scheduler {
	if cfg.Interval <= 0 {
		cfg.Interval = time.Hour
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 5
	}
	return &Scheduler{
		store:  store,
		svc:    svc,
		log:    log,
		config: cfg,
	}
}

func (s *Scheduler) RunOnce(ctx context.Context) error {
	jobs, err := s.store.GetPendingJobs(ctx, s.config.BatchSize)
	if err != nil {
		return err
	}

	for _, job := range jobs {
		if err := s.svc.RunJob(ctx, "ops", job.ID); err != nil {
			s.log.Warn("reconciliation.job_failed", map[string]any{
				"job_id": job.ID.String(),
				"error":  err.Error(),
			})
		}
	}

	return nil
}

func (s *Scheduler) Interval() time.Duration {
	return s.config.Interval
}
