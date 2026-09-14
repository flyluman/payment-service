package main

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/crownroutes/payment-service/config"
	"github.com/crownroutes/payment-service/internal/adapters/callback"
	"github.com/crownroutes/payment-service/internal/adapters/initiator"
	"github.com/crownroutes/payment-service/internal/adapters/postgres"
	"github.com/crownroutes/payment-service/internal/adapters/observability"
	"github.com/crownroutes/payment-service/internal/adapters/outbox"
	"github.com/crownroutes/payment-service/internal/adapters/sns"
	"github.com/crownroutes/payment-service/internal/app/payment"
	"github.com/crownroutes/payment-service/internal/ports"
	"github.com/crownroutes/payment-service/internal/relay"
	"github.com/crownroutes/payment-service/internal/relay/publisher"
	"github.com/crownroutes/payment-service/internal/relay/ring"
)

// startRelay blocks until ctx is cancelled or the relay worker exits.
func startRelay(ctx context.Context, d *deps) error {
	cfg := d.cfg
	logger := d.logger

	d.outboxWriter.SetClaimTTL(time.Duration(cfg.Outbox.ClaimTTLSec) * time.Second)
	d.outboxWriter.SetShardCount(cfg.Outbox.ShardCount)
	d.outboxWriter.SetLogger(logger)

	notifier := postgres.NewOutboxNotifier(cfg.Database.DSN())
	notifier.Start(ctx)
	defer notifier.Close()

	pub, err := buildPublisher(ctx, cfg, logger, d.paymentSvc)
	if err != nil {
		return err
	}

	shards := ring.OwnedShards(cfg.Outbox.WorkerIndex, cfg.Outbox.WorkerCount, cfg.Outbox.ShardCount)
	logger.Info("relay.shards_assigned", map[string]any{
		"worker_index": cfg.Outbox.WorkerIndex,
		"worker_count": cfg.Outbox.WorkerCount,
		"shard_count":  len(shards),
	})
	if len(shards) == 0 {
		logger.Warn("relay.no_shards", map[string]any{
			"worker_index": cfg.Outbox.WorkerIndex,
			"worker_count": cfg.Outbox.WorkerCount,
			"warning":      "this worker owns no shards (more workers than shards); it will publish nothing",
		})
	}

	worker := relay.NewWorker(d.outboxWriter, pub, logger, d.metrics, relay.Config{
		Shards:       shards,
		BatchSize:    cfg.Outbox.BatchSize,
		MaxAttempts:  cfg.Outbox.MaxAttempts,
		PollInterval: time.Duration(cfg.Outbox.PollIntervalSec) * time.Second,
		Wakeup:       notifier.Wakeup(),
	})

	if err := worker.Run(ctx); err != nil && err != context.Canceled {
		return fmt.Errorf("relay worker: %w", err)
	}
	logger.Info("relay.stopped", nil)
	return nil
}

func buildPublisher(ctx context.Context, cfg *config.Config, logger *observability.SlogLogger, paymentSvc *payment.Service) (relay.Publisher, error) {
	var basePub relay.Publisher
	switch cfg.Outbox.Publisher {
	case "sns":
		pub, err := sns.NewPublisherFromConfig(ctx, cfg.AWS.Region, cfg.SNS.PaymentEventsTopic, cfg.Outbox.SNSAggregateVersionAttr, logger)
		if err != nil {
			return nil, fmt.Errorf("build sns publisher: %w", err)
		}
		logger.Info("relay.publisher_selected", map[string]any{"publisher": "sns", "topic": cfg.SNS.PaymentEventsTopic})
		basePub = pub
	default:
		if cfg.App.Environment == "dev" {
			logger.Info("relay.publisher_selected", map[string]any{"publisher": "log"})
		} else {
			logger.Warn("relay.publisher_selected", map[string]any{
				"publisher":   "log",
				"warning":     "OUTBOX_PUBLISHER=log in a non-dev environment: the relay is up but delivers nothing (events are only logged). Set OUTBOX_PUBLISHER=sns for real delivery.",
				"environment": cfg.App.Environment,
			})
		}
		basePub = publisher.NewLogPublisher(logger)
	}

	cbDispatcher := callback.NewDispatcher(
		&http.Client{Timeout: 10 * time.Second},
		logger,
	)
	initiatorHandler := initiator.NewHandler(paymentSvc)
	baseHandler := outbox.HandlerFunc(func(ctx context.Context, event ports.PendingEvent) error {
		return basePub.Publish(ctx, event)
	})

	return outbox.NewRouter(
		outbox.Route{EventType: "*", Handler: baseHandler},
		outbox.Route{EventType: ports.EventTypeTransactionCallback, Handler: cbDispatcher},
		outbox.Route{EventType: ports.EventTypeGatewayInitiate, Handler: initiatorHandler},
	), nil
}
