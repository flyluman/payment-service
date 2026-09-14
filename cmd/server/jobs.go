package main

import (
	"context"
	"time"

	"github.com/crownroutes/payment-service/internal/adapters/postgres"
	leaseexpiry "github.com/crownroutes/payment-service/internal/jobs/lease_expiry"
	partitionmanager "github.com/crownroutes/payment-service/internal/jobs/partition_manager"
	gatewaymetrics "github.com/crownroutes/payment-service/internal/jobs/gateway_metrics"
	tenantwebhook "github.com/crownroutes/payment-service/internal/jobs/tenant_webhook"
	notifprocessor "github.com/crownroutes/payment-service/internal/jobs/notification"
	reconjob "github.com/crownroutes/payment-service/internal/jobs/reconciliation"
)

const defaultPartitionInterval = time.Hour
const defaultGatewayMetricsInterval = 5 * time.Minute

// startJobs runs the lease_expiry, partition_manager, and gateway_metrics jobs on tickers until
// ctx is cancelled. It runs each job once immediately, then on its configured
// interval.
func startJobs(ctx context.Context, d *deps) error {
	cfg := d.cfg
	logger := d.logger

	partStore := postgres.NewPartitionStore(d.db)
	partMgr := partitionmanager.New(partStore, logger, partitionmanager.Config{
		WeeksAhead:     cfg.Jobs.PartitionWeeksAhead,
		RetentionWeeks: cfg.Jobs.PartitionRetentionWeeks,
		DropAfter:      time.Duration(cfg.Jobs.PartitionDropAfterDays) * 24 * time.Hour,
	})

	reaper := buildReaper(d)

	gwMetricsStore := postgres.NewGatewayMetricsStore(d.db)
	gwMetricsJob := gatewaymetrics.NewJob(d.cbStore, gwMetricsStore, d.registry, logger)

	twhReader := postgres.NewTenantWebhookDeliveryReader(d.db)
	twhUpdater := postgres.NewTenantWebhookDeliveryUpdater(d.db)
	twhConfig := postgres.NewTenantWebhookConfigStore(d.db)
	twhWorker := tenantwebhook.NewWorker(twhReader, twhUpdater, twhConfig, logger, tenantwebhook.Config{
		MaxAttempts:   10,
		MaxBackoffSec: 3600,
	})

	notifProcessor := notifprocessor.NewProcessor(d.notifSvc, logger)

	leaseInterval := time.Duration(cfg.Jobs.LeaseExpiryIntervalSec) * time.Second
	if leaseInterval <= 0 {
		leaseInterval = 60 * time.Second
	}
	partInterval := defaultPartitionInterval
	gwMetricsInterval := defaultGatewayMetricsInterval
	twhInterval := 5 * time.Second
	notifInterval := 2 * time.Second

	reconInterval := time.Duration(cfg.Jobs.ReconciliationIntervalSec) * time.Second
	if reconInterval <= 0 {
		reconInterval = 5 * time.Minute
	}
	reconStore := postgres.NewReconciliationStore(d.db)
	reconScheduler := reconjob.NewScheduler(reconStore, d.reconSvc, logger, reconjob.Config{
		Interval: reconInterval,
	})

	// Run once at startup.
	runJobOnce(ctx, logger, "partition_manager", func(c context.Context) error { return partMgr.RunOnce(c) })
	runJobOnce(ctx, logger, "lease_expiry", func(c context.Context) error { return reaper.RunOnce(c) })
	runJobOnce(ctx, logger, "gateway_metrics", func(c context.Context) error { return gwMetricsJob.RunOnce(c) })
	runJobOnce(ctx, logger, "tenant_webhook", func(c context.Context) error { return twhWorker.RunOnce(c) })
	runJobOnce(ctx, logger, "notification", func(c context.Context) error { return notifProcessor.RunOnce(c) })
	runJobOnce(ctx, logger, "reconciliation", func(c context.Context) error { return reconScheduler.RunOnce(c) })

	leaseTicker := time.NewTicker(leaseInterval)
	defer leaseTicker.Stop()
	partTicker := time.NewTicker(partInterval)
	defer partTicker.Stop()
	gwMetricsTicker := time.NewTicker(gwMetricsInterval)
	defer gwMetricsTicker.Stop()
	twhTicker := time.NewTicker(twhInterval)
	defer twhTicker.Stop()
	notifTicker := time.NewTicker(notifInterval)
	defer notifTicker.Stop()
	reconTicker := time.NewTicker(reconInterval)
	defer reconTicker.Stop()

	logger.Info("jobs.scheduled", map[string]any{
		"lease_expiry_interval":        leaseInterval.String(),
		"partition_manager_interval":   partInterval.String(),
		"gateway_metrics_interval":     gwMetricsInterval.String(),
		"tenant_webhook_interval":      twhInterval.String(),
		"notification_interval":        notifInterval.String(),
		"reconciliation_interval":      reconInterval.String(),
	})

	for {
		select {
		case <-ctx.Done():
			logger.Info("jobs.stopped", nil)
			return nil
		case <-leaseTicker.C:
			runJobOnce(ctx, logger, "lease_expiry", func(c context.Context) error { return reaper.RunOnce(c) })
		case <-partTicker.C:
			runJobOnce(ctx, logger, "partition_manager", func(c context.Context) error { return partMgr.RunOnce(c) })
		case <-gwMetricsTicker.C:
			runJobOnce(ctx, logger, "gateway_metrics", func(c context.Context) error { return gwMetricsJob.RunOnce(c) })
		case <-twhTicker.C:
			runJobOnce(ctx, logger, "tenant_webhook", func(c context.Context) error { return twhWorker.RunOnce(c) })
		case <-notifTicker.C:
			runJobOnce(ctx, logger, "notification", func(c context.Context) error { return notifProcessor.RunOnce(c) })
		case <-reconTicker.C:
			runJobOnce(ctx, logger, "reconciliation", func(c context.Context) error { return reconScheduler.RunOnce(c) })
		}
	}
}

func runJobOnce(ctx context.Context, logger interface {
	Warn(string, map[string]any)
}, name string, fn func(context.Context) error) {
	if err := fn(ctx); err != nil {
		logger.Warn("job.failed", map[string]any{"job": name, "error": err.Error()})
	}
}

func buildReaper(d *deps) *leaseexpiry.Reaper {
	return leaseexpiry.New(d.txnRepo, d.paymentSvc, d.idempotencyRepo, d.logger, leaseexpiry.Config{
		IdempotencyProcessingTimeout: time.Duration(d.cfg.Jobs.IdempotencyProcessingTimeoutSec) * time.Second,
	})
}
