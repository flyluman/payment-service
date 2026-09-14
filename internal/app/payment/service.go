package payment

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/app/idempotency"
	"github.com/crownroutes/payment-service/internal/domain/fees"
	"github.com/crownroutes/payment-service/internal/domain/transaction"
	"github.com/crownroutes/payment-service/internal/ports"
)

var ErrNoGateway = errors.New("payment: no eligible gateway for transaction")

type TransactionRepository interface {
	Insert(ctx context.Context, t *transaction.Txn) error
	GetByID(ctx context.Context, id uuid.UUID) (*transaction.Txn, error)
	UpdateStatus(ctx context.Context, t *transaction.Txn) error
	UpdateGatewayReference(ctx context.Context, id uuid.UUID, gatewayRefID string, expectedVersion int) error
	List(ctx context.Context, filter ports.TransactionFilter) (*ports.TransactionListResult, error)
}
type EventWriter interface {
	Write(ctx context.Context, event ports.OutboxEvent) error
}
type Transactor interface {
	WithinTx(ctx context.Context, fn func(ctx context.Context) error) error
}
type ConfigReader interface {
	GetGatewayConfig(ctx context.Context, gatewayID string) (*ports.GatewayConfig, error)
	GetProcessingTimeout(ctx context.Context, gatewayID, paymentMethod string) (time.Duration, error)
	GetFeeModel(ctx context.Context, gatewayID, paymentMethod string) (*ports.GatewayFeeModel, error)
	GetCurrencyRates(ctx context.Context, tenantID uuid.UUID) ([]fees.CurrencyRate, error)
}
type LeaseStore interface {
	Acquire(ctx context.Context, leaseKey, transactionID uuid.UUID, ttlSec int) (bool, []byte, error)
	WriteCachedResponse(ctx context.Context, leaseKey uuid.UUID, response []byte) error
	TryAcquireDirect(ctx context.Context, leaseKey, transactionID uuid.UUID, ttlSec int) (bool, error)
}
type GatewayRegistry interface {
	Get(gatewayID string) (ports.GatewayAdapter, error)
}
type CancelResolver interface {
	ResolveCancelRefund(ctx context.Context, transactionID uuid.UUID, amount int64) error
}
type CircuitBreaker interface {
	RecordFailure(ctx context.Context, gatewayID string) error
	RecordSuccess(ctx context.Context, gatewayID string) error
}
type IntentTracker interface {
	EnterProcessing(ctx context.Context, gatewayID string, txnID uuid.UUID, ttl time.Duration) error
	ExitProcessing(ctx context.Context, gatewayID string, txnID uuid.UUID) error
}
type Service struct {
	repo           TransactionRepository
	outbox         EventWriter
	config         ConfigReader
	tx             Transactor
	lease          LeaseStore
	gateways       GatewayRegistry
	cancelResolver CancelResolver
	breaker        CircuitBreaker
	intents        IntentTracker
	idem           *idempotency.Guard
	gatewayMetaStore  GatewayMetadataStore
	audit          ports.AuditLogStore
	notif          ports.NotificationDispatcher
	twDispatcher   ports.TenantWebhookDispatcher
	log            ports.Logger
	metrics        ports.MetricRecorder
	bus            ports.EventBus
}

func (s *Service) SetCancelResolver(r CancelResolver)   { s.cancelResolver = r }
func (s *Service) SetCircuitBreaker(b CircuitBreaker)   { s.breaker = b }
func (s *Service) SetIntentTracker(t IntentTracker)     { s.intents = t }
func (s *Service) SetIdempotency(g *idempotency.Guard)  { s.idem = g }
func (s *Service) SetGatewayMetadataStore(w GatewayMetadataStore) { s.gatewayMetaStore = w }
func (s *Service) SetEventBus(bus ports.EventBus)          { s.bus = bus }
func (s *Service) SetAuditLogStore(a ports.AuditLogStore)  { s.audit = a }
func (s *Service) SetNotificationService(n ports.NotificationDispatcher) { s.notif = n }
func (s *Service) SetTenantWebhookDispatcher(d ports.TenantWebhookDispatcher) { s.twDispatcher = d }

func NewService(
	repo TransactionRepository,
	outbox EventWriter,
	config ConfigReader,
	tx Transactor,
	lease LeaseStore,
	gateways GatewayRegistry,
	log ports.Logger,
	metrics ports.MetricRecorder,
) *Service {
	return &Service{
		repo:    repo,
		outbox:  outbox,
		config:  config,
		tx:      tx,
		lease:   lease,
		gateways: gateways,
		log:     log,
		metrics: metrics,
	}
}

type transactionCreatedPayload struct {
	TransactionID    string `json:"transaction_id"`
	TenantID         string `json:"tenant_id"`
	UserID           string `json:"user_id"`

	Amount           int64  `json:"amount"`
	Currency         string `json:"currency"`
	PaymentMethod    string `json:"payment_method"`
	Gateway          string `json:"gateway"`
	Status           string `json:"status"`
	CreatedAt        string `json:"created_at"`
	AggregateVersion int    `json:"aggregate_version"`
}


type GatewayMetadataStore interface {
	InsertGatewayMetadata(ctx context.Context, transactionID uuid.UUID, gatewayID string, payload []byte) error
	GetGatewayMetadata(ctx context.Context, transactionID uuid.UUID) ([]byte, error)
}

func (s *Service) GetPayment(ctx context.Context, id uuid.UUID) (*transaction.Txn, error) {
	return s.repo.GetByID(ctx, id)
}

type gatewayInitiatePayload struct {
	TransactionID string `json:"transaction_id"`
	TenantID      string `json:"tenant_id"`
	GatewayID     string `json:"gateway_id"`
}

func (s *Service) ProcessGatewayInitiate(ctx context.Context, transactionID uuid.UUID) error {
	txn, err := s.repo.GetByID(ctx, transactionID)
	if err != nil {
		return fmt.Errorf("gateway-initiate: load transaction %s: %w", transactionID, err)
	}
	if txn.GatewayReferenceID != "" {
		return nil
	}
	if txn.Status != transaction.StatusPending {
		return nil
	}

	ttlSec := txn.EstimatedTimeoutSeconds
	if ttlSec <= 0 {
		ttlSec = 30
	}
	acquired, err := s.lease.TryAcquireDirect(ctx, txn.ID, txn.ID, ttlSec)
	if err != nil {
		return fmt.Errorf("gateway-initiate: acquire lease: %w", err)
	}
	if !acquired {
		return nil
	}

	adapter, err := s.gateways.Get(txn.GatewayID)
	if err != nil {
		return fmt.Errorf("gateway-initiate: resolve adapter for %s: %w", txn.GatewayID, err)
	}

	// Calculate two-layer fees before calling the gateway.
	gatewayAmount := txn.Amount
	gatewayCurrency := txn.Currency

	feeModel, _ := s.config.GetFeeModel(ctx, txn.GatewayID, string(txn.PaymentMethod))
	if feeModel != nil {
		rates, _ := s.config.GetCurrencyRates(ctx, txn.TenantID)
		chargesCurrency := feeModel.ChargesCurrency
		if chargesCurrency == "" {
			chargesCurrency = txn.Currency
		}

		// Convert fee_model fixed_fee from BPS to actual amount: fixedFee is in charges currency units.
		var fixedFee float64
		if feeModel.FixedFee > 0 {
			fixedFee = float64(feeModel.FixedFee)
		}

		// serviceFeeRatio: percentage_bps is in basis points (1 BPS = 0.01%).
		var ratio float64
		if feeModel.PercentageBPS > 0 {
			ratio = float64(feeModel.PercentageBPS) / 100.0
		}

		breakdown := fees.Calculate(txn.Amount, txn.Currency, chargesCurrency, fixedFee, ratio, rates)
		if err := fees.Validate(breakdown); err != nil {
			s.log.Warn("payment.fee_calculation_invalid", map[string]any{
				"transaction_id": txn.ID.String(),
				"error":          err.Error(),
			})
		} else {
			txn.FeeBreakdown = breakdown
			if breakdown.Gateway != nil {
				gatewayAmount = breakdown.Gateway.Amount
				gatewayCurrency = breakdown.Gateway.Currency
				gwAmt := breakdown.Gateway.Amount
				txn.GatewayAmount = &gwAmt
				txn.GatewayCurrency = breakdown.Gateway.Currency
			}
		}
	}

	resp, err := adapter.InitiatePayment(ctx, ports.GatewayPaymentRequest{
		TransactionID: txn.ID,
		TenantID:      txn.TenantID,
		Amount:        gatewayAmount,
		Currency:      gatewayCurrency,
		PaymentMethod: txn.PaymentMethod,
		Metadata:      txn.Metadata,
		CustomerEmail: txn.CustomerEmail,
		Description:   txn.Description,
		AttemptNumber: 1,
	})
	if err != nil {
		s.recordGatewayError(ctx, txn, err)
		return fmt.Errorf("gateway-initiate: gateway call failed: %w", err)
	}

	return s.tx.WithinTx(ctx, func(ctx context.Context) error {
		if err := s.repo.UpdateGatewayReference(ctx, txn.ID, resp.GatewayReferenceID, txn.Version); err != nil {
			return fmt.Errorf("update gateway reference: %w", err)
		}
		txn.GatewayReferenceID = resp.GatewayReferenceID
		if s.gatewayMetaStore != nil && len(resp.GatewayMetadata) > 0 {
			metaPayload, err := json.Marshal(resp.GatewayMetadata)
			if err != nil {
				return fmt.Errorf("marshal raw metadata: %w", err)
			}
			if err := s.gatewayMetaStore.InsertGatewayMetadata(ctx, txn.ID, txn.GatewayID, metaPayload); err != nil {
				return fmt.Errorf("insert raw metadata: %w", err)
			}
		}

		if err := transaction.TransitionState(txn, transaction.StatusProcessing, transaction.ActorSystem); err != nil {
			return fmt.Errorf("transition to processing: %w", err)
		}
		if s.audit != nil {
			_ = s.audit.WriteEntry(ctx, &ports.AuditEntry{
				TransactionID: &txn.ID,
				EventType:     ports.AuditEventTypeStateChange,
				Actor:         string(transaction.ActorSystem),
				PreviousState: string(transaction.StatusPending),
				NewState:      string(txn.Status),
				Reason:        "gateway_initiate",
			})
		}
		if err := s.repo.UpdateStatus(ctx, txn); err != nil {
			return fmt.Errorf("update status: %w", err)
		}

		if s.bus != nil {
			s.bus.Publish(ctx, ports.StatusEvent{
				TransactionID: txn.ID,
				Status:        transaction.StatusProcessing,
			})
		}
		return nil
	})
}

func (s *Service) recordGatewayError(ctx context.Context, txn *transaction.Txn, gatewayErr error) {
	if s.breaker != nil {
		_ = s.breaker.RecordFailure(ctx, txn.GatewayID)
	}
}

func (s *Service) GetGatewayMetadata(ctx context.Context, transactionID uuid.UUID) (map[string]any, error) {
	if s.gatewayMetaStore == nil {
		return nil, nil
	}
	raw, err := s.gatewayMetaStore.GetGatewayMetadata(ctx, transactionID)
	if err != nil {
		return nil, nil
	}
	if len(raw) == 0 {
		return nil, nil
	}
	var meta map[string]any
	if err := json.Unmarshal(raw, &meta); err != nil {
		return nil, nil
	}
	return meta, nil
}

type CreateInput struct {
	TenantID       uuid.UUID
	UserID         uuid.UUID
	GatewayID      string

	Amount         int64
	Currency       string
	PaymentMethod  transaction.PaymentMethod
	CaptureMode    transaction.CaptureMode
	CustomerID     uuid.UUID
	CustomerEmail  string
	Description    string
	Metadata       map[string]any

	CallbackURL    string
	RedirectURL    string
	IdempotencyKey string
}

type CreateResult struct {
	Verdict     idempotency.Verdict
	Transaction *transaction.Txn
	Token       string
}

type idempotencyTransactionResponse struct {
	TransactionID string `json:"transaction_id"`
	Token         string `json:"token,omitempty"`
}

func (s *Service) Create(ctx context.Context, in CreateInput) (CreateResult, error) {
	gateway := in.GatewayID
	if gateway == "" {
		return CreateResult{}, ErrNoGateway
	}
	cfg, err := s.config.GetGatewayConfig(ctx, gateway)
	if err != nil {
		return CreateResult{}, fmt.Errorf("payment: gateway config for %s: %w", gateway, err)
	}
	if !cfg.IsActive {
		return CreateResult{}, ErrNoGateway
	}

	timeout, err := s.config.GetProcessingTimeout(ctx, gateway, string(in.PaymentMethod))
	if err != nil {
		return CreateResult{}, fmt.Errorf("payment: processing timeout for %s/%s: %w", gateway, in.PaymentMethod, err)
	}
	timeoutSec := int(timeout.Seconds())
	if timeoutSec <= 0 {
		return CreateResult{}, fmt.Errorf("payment: invalid processing timeout %s for %s/%s", timeout, gateway, in.PaymentMethod)
	}

	identity := in.TenantID
	if identity == uuid.Nil {
		return CreateResult{}, fmt.Errorf("payment: tenant_id is required")
	}

	txn, err := transaction.New(
		identity,
		in.UserID,
		in.Amount,
		in.Currency,
		in.PaymentMethod,
		gateway,
		in.CustomerID,
		in.CustomerEmail,
		in.Description,
		in.Metadata,
		timeoutSec,
		in.CaptureMode,
	)
	if err != nil {
		return CreateResult{}, fmt.Errorf("payment: build transaction: %w", err)
	}
	txn.TenantID = identity
	txn.UserID = in.UserID
	txn.AttemptedGateway = gateway
	txn.CallbackURL = in.CallbackURL
	txn.RedirectURL = in.RedirectURL

	if feeModel, err := s.config.GetFeeModel(ctx, gateway, string(in.PaymentMethod)); err == nil && feeModel != nil {
		fee := feeModel.CalculateFee(in.Amount, 0)
		txn.GatewayFeeEstimate = &fee
		txn.GatewayFeeCurrency = in.Currency
		txn.GatewayFeeModelVersion = 1
	}

	rawToken, tokenHash, err := GenerateToken()
	if err != nil {
		return CreateResult{}, fmt.Errorf("payment: generate token: %w", err)
	}
	txn.TokenHash = tokenHash

	createdPayload, _ := json.Marshal(transactionCreatedPayload{
		TransactionID: txn.ID.String(),
		TenantID:      txn.TenantID.String(),
		UserID:        txn.UserID.String(),
		Amount:        txn.Amount,
		Currency:      txn.Currency,
		PaymentMethod: string(txn.PaymentMethod),
		Gateway:       gateway,
		Status:        string(txn.Status),
		CreatedAt:     txn.CreatedAt.Format(time.RFC3339Nano),
		AggregateVersion: txn.Version,
	})

	initiatePayload, _ := json.Marshal(gatewayInitiatePayload{
		TransactionID: txn.ID.String(),
		TenantID:      txn.TenantID.String(),
		GatewayID:     gateway,
	})

	events := []ports.OutboxEvent{
		{
			AggregateID:      txn.ID,
			AggregateType:    "transaction",
			EventType:        ports.EventTypeTransactionCreated,
			Payload:          createdPayload,
			EventVersion:     1,
			AggregateVersion: txn.Version,
		},
		{
			AggregateID:      txn.ID,
			AggregateType:    "transaction",
			EventType:        ports.EventTypeGatewayInitiate,
			Payload:          initiatePayload,
			EventVersion:     1,
			AggregateVersion: txn.Version,
		},
	}

	if s.idem == nil {
		if err := s.tx.WithinTx(ctx, func(ctx context.Context) error {
			if err := s.repo.Insert(ctx, txn); err != nil {
				return fmt.Errorf("insert transaction: %w", err)
			}
			if s.audit != nil {
				_ = s.audit.WriteEntry(ctx, &ports.AuditEntry{
					TransactionID: &txn.ID,
					EventType:     ports.AuditEventTypeStateChange,
					Actor:         string(transaction.ActorSystem),
					NewState:      string(txn.Status),
					Reason:        "payment_created",
				})
			}
			for i := range events {
				if err := s.outbox.Write(ctx, events[i]); err != nil {
					return fmt.Errorf("write outbox event %s: %w", events[i].EventType, err)
				}
			}
			return nil
		}); err != nil {
			return CreateResult{}, err
		}
		s.logCreated(txn)
		if txn.CaptureMode == transaction.CaptureModeManual {
			return CreateResult{Verdict: idempotency.Created, Transaction: txn, Token: rawToken}, nil
		}
		reloaded, err := s.syncInitiate(ctx, txn)
		if err != nil {
			return CreateResult{Verdict: idempotency.Created, Transaction: reloaded, Token: rawToken}, err
		}
		return CreateResult{Verdict: idempotency.Created, Transaction: reloaded, Token: rawToken}, nil
	}

	if in.IdempotencyKey == "" {
		return CreateResult{}, idempotency.ErrKeyRequired
	}
	composite := idempotency.Composite(identityKey(CreateInput{TenantID: in.TenantID}), "create_payment", in.IdempotencyKey)
	requestHash := idempotency.RequestHash(
		identityKey(CreateInput{TenantID: in.TenantID}),
		strconv.FormatInt(in.Amount, 10),
		in.Currency,
		string(in.PaymentMethod),
		in.CallbackURL,
		in.RedirectURL,
	)

	res, err := s.idem.Execute(ctx, composite, requestHash, func(ctx context.Context) ([]byte, error) {
		if err := s.repo.Insert(ctx, txn); err != nil {
			return nil, fmt.Errorf("insert transaction: %w", err)
		}
		if s.audit != nil {
			_ = s.audit.WriteEntry(ctx, &ports.AuditEntry{
				TransactionID: &txn.ID,
				EventType:     ports.AuditEventTypeStateChange,
				Actor:         string(transaction.ActorSystem),
				NewState:      string(txn.Status),
				Reason:        "payment_created",
			})
		}
		for i := range events {
			if err := s.outbox.Write(ctx, events[i]); err != nil {
				return nil, fmt.Errorf("write outbox event %s: %w", events[i].EventType, err)
			}
		}
		return json.Marshal(idempotencyTransactionResponse{TransactionID: txn.ID.String(), Token: rawToken})
	})
	if err != nil {
		return CreateResult{}, err
	}

	switch res.Verdict {
	case idempotency.Created:
		s.logCreated(txn)
		reloaded, err := s.syncInitiate(ctx, txn)
		if err != nil {
			return CreateResult{Verdict: res.Verdict, Transaction: reloaded, Token: rawToken}, err
		}
		return CreateResult{Verdict: res.Verdict, Transaction: reloaded, Token: rawToken}, nil
	case idempotency.Replayed:
		var stored idempotencyTransactionResponse
		if err := json.Unmarshal(res.Response, &stored); err != nil {
			return CreateResult{}, fmt.Errorf("payment: decode idempotent response: %w", err)
		}
		id, err := uuid.Parse(stored.TransactionID)
		if err != nil {
			return CreateResult{}, fmt.Errorf("payment: bad stored transaction id: %w", err)
		}
		reloaded, err := s.repo.GetByID(ctx, id)
		if err != nil {
			return CreateResult{}, fmt.Errorf("payment: reload idempotent transaction %s: %w", id, err)
		}
		reloaded, _ = s.syncInitiate(ctx, reloaded)
		return CreateResult{Verdict: res.Verdict, Transaction: reloaded, Token: stored.Token}, nil
	default:
		return CreateResult{Verdict: res.Verdict}, nil
	}
}

func (s *Service) syncInitiate(ctx context.Context, txn *transaction.Txn) (*transaction.Txn, error) {
	if err := s.ProcessGatewayInitiate(ctx, txn.ID); err != nil {
		s.log.Warn("payment.sync_initiate_failed", map[string]any{
			"transaction_id": txn.ID.String(),
			"error":          err.Error(),
		})
		reloaded, loadErr := s.repo.GetByID(ctx, txn.ID)
		if loadErr != nil {
			return txn, err
		}
		return reloaded, err
	}
	reloaded, err := s.repo.GetByID(ctx, txn.ID)
	if err != nil {
		return txn, nil
	}
	return reloaded, nil
}

func (s *Service) logCreated(txn *transaction.Txn) {
	s.log.Info(ports.LogEventTransactionCreated, map[string]any{
		ports.FieldTransactionID: txn.ID.String(),
		ports.FieldTenantID:      txn.TenantID.String(),
		ports.FieldUserID:        txn.UserID.String(),
		ports.FieldGatewayID:     txn.GatewayID,
		ports.FieldPaymentMethod: string(txn.PaymentMethod),
	})
	s.metrics.Increment(ports.MetricTransactionCreated, map[string]string{
		"gateway_id":     txn.GatewayID,
		"payment_method": string(txn.PaymentMethod),
	})
}

func identityKey(in CreateInput) string {
	return in.TenantID.String()
}

func (s *Service) ListTransactions(ctx context.Context, filter ports.TransactionFilter) (*ports.TransactionListResult, error) {
	return s.repo.List(ctx, filter)
}

var ErrInvalidTransition = errors.New("payment: invalid state transition")

func (s *Service) Authorize(ctx context.Context, transactionID uuid.UUID) (*transaction.Txn, error) {
	txn, err := s.repo.GetByID(ctx, transactionID)
	if err != nil {
		return nil, fmt.Errorf("payment: load transaction %s: %w", transactionID, err)
	}
	if txn.CaptureMode != transaction.CaptureModeManual {
		return nil, fmt.Errorf("payment: authorize requires manual capture mode, got %s", txn.CaptureMode)
	}
	if err := transaction.TransitionState(txn, transaction.StatusProcessing, transaction.ActorSystem); err != nil {
		return nil, ErrInvalidTransition
	}
	now := time.Now().UTC()
	txn.ProcessingStartedAt = &now
	timeout := time.Duration(txn.EstimatedTimeoutSeconds) * time.Second
	txn.ProcessingTimeout = &timeout
	if err := s.repo.UpdateStatus(ctx, txn); err != nil {
		return nil, fmt.Errorf("payment: update status %s: %w", transactionID, err)
	}

	adapter, err := s.gateways.Get(txn.GatewayID)
	if err != nil {
		return nil, fmt.Errorf("payment: resolve adapter for %s: %w", txn.GatewayID, err)
	}

	resp, gwErr := s.callGateway(ctx, adapter, txn)
	if gwErr != nil {
		if gwErr.Category == ports.ErrorCategoryAmbiguous || gwErr.Category == ports.ErrorCategoryNetworkTimeout {
			return txn, nil
		}
		if err := transaction.TransitionState(txn, transaction.StatusFailed, transaction.ActorGateway); err != nil {
			return nil, err
		}
		txn.FailureReason = &transaction.FailureReason{
			Category:       string(gwErr.Category),
			Code:           gwErr.Code,
			GatewayCode:    gwErr.GatewayCode,
			GatewayMessage: gwErr.GatewayMessage,
			Source:         transaction.FailureReasonSourceGateway,
		}
		if err := s.repo.UpdateStatus(ctx, txn); err != nil {
			return nil, fmt.Errorf("payment: update status %s: %w", transactionID, err)
		}
		return txn, nil
	}

	if resp.GatewayReferenceID != "" {
		txn.GatewayReferenceID = resp.GatewayReferenceID
	}
	txn.ActualGateway = txn.GatewayID

	switch resp.Status {
	case ports.GatewayPaymentStatusSucceeded, ports.GatewayPaymentStatusPending:
		if err := transaction.TransitionState(txn, transaction.StatusAuthorized, transaction.ActorGateway); err != nil {
			return nil, err
		}
	case ports.GatewayPaymentStatusFailed:
		if err := transaction.TransitionState(txn, transaction.StatusFailed, transaction.ActorGateway); err != nil {
			return nil, err
		}
		txn.FailureReason = &transaction.FailureReason{
			Category:       "gateway_declined",
			Code:           resp.ErrorCode,
			GatewayCode:    resp.ErrorCode,
			GatewayMessage: resp.ErrorMessage,
			Source:         transaction.FailureReasonSourceGateway,
		}
	default:
		if err := transaction.TransitionState(txn, transaction.StatusFailed, transaction.ActorGateway); err != nil {
			return nil, err
		}
	}
	txn.ProcessingStartedAt = nil
	txn.ProcessingTimeout = nil
	if err := s.repo.UpdateStatus(ctx, txn); err != nil {
		return nil, fmt.Errorf("payment: update status %s: %w", transactionID, err)
	}
	return txn, nil
}

func (s *Service) Capture(ctx context.Context, transactionID uuid.UUID) (*transaction.Txn, error) {
	txn, err := s.repo.GetByID(ctx, transactionID)
	if err != nil {
		return nil, fmt.Errorf("payment: load transaction %s: %w", transactionID, err)
	}
	if txn.Status != transaction.StatusAuthorized {
		return nil, ErrInvalidTransition
	}
	if err := transaction.TransitionState(txn, transaction.StatusProcessing, transaction.ActorSystem); err != nil {
		return nil, ErrInvalidTransition
	}
	now := time.Now().UTC()
	txn.ProcessingStartedAt = &now
	timeout := time.Duration(txn.EstimatedTimeoutSeconds) * time.Second
	txn.ProcessingTimeout = &timeout
	if err := s.repo.UpdateStatus(ctx, txn); err != nil {
		return nil, fmt.Errorf("payment: update status %s: %w", transactionID, err)
	}

	adapter, err := s.gateways.Get(txn.GatewayID)
	if err != nil {
		return nil, fmt.Errorf("payment: resolve adapter for %s: %w", txn.GatewayID, err)
	}

	resp, rawErr := adapter.CapturePayment(ctx, ports.GatewayCaptureRequest{
		TransactionID:      txn.ID,
		GatewayReferenceID: txn.GatewayReferenceID,
		Amount:             txn.Amount,
		Currency:           txn.Currency,
	})
	var gwErr *ports.GatewayError
	if rawErr != nil {
		if !errors.As(rawErr, &gwErr) {
			gwErr = &ports.GatewayError{
				Category:       ports.ErrorCategoryGatewayError,
				Code:           "capture_call_failed",
				GatewayMessage: rawErr.Error(),
				Underlying:     rawErr,
			}
		}
	}
	if gwErr != nil {
		if gwErr.Category == ports.ErrorCategoryAmbiguous || gwErr.Category == ports.ErrorCategoryNetworkTimeout {
			txn.ProcessingStartedAt = nil
			txn.ProcessingTimeout = nil
			_ = s.repo.UpdateStatus(ctx, txn)
			return txn, nil
		}
		if err := transaction.TransitionState(txn, transaction.StatusFailed, transaction.ActorGateway); err != nil {
			return nil, err
		}
		txn.FailureReason = &transaction.FailureReason{
			Category:       string(gwErr.Category),
			Code:           gwErr.Code,
			GatewayCode:    gwErr.GatewayCode,
			GatewayMessage: gwErr.GatewayMessage,
			Source:         transaction.FailureReasonSourceGateway,
		}
		txn.ProcessingStartedAt = nil
		txn.ProcessingTimeout = nil
		if err := s.repo.UpdateStatus(ctx, txn); err != nil {
			return nil, fmt.Errorf("payment: update status %s: %w", transactionID, err)
		}
		return txn, nil
	}

	txn.ActualGateway = txn.GatewayID
	switch resp.Status {
	case ports.GatewayPaymentStatusSucceeded:
		if err := transaction.TransitionState(txn, transaction.StatusCaptured, transaction.ActorGateway); err != nil {
			return nil, err
		}
	case ports.GatewayPaymentStatusFailed:
		if err := transaction.TransitionState(txn, transaction.StatusFailed, transaction.ActorGateway); err != nil {
			return nil, err
		}
		txn.FailureReason = &transaction.FailureReason{
			Category:       "gateway_declined",
			Code:           resp.ErrorCode,
			GatewayCode:    resp.ErrorCode,
			GatewayMessage: resp.ErrorMessage,
			Source:         transaction.FailureReasonSourceGateway,
		}
	default:
		if err := transaction.TransitionState(txn, transaction.StatusFailed, transaction.ActorGateway); err != nil {
			return nil, err
		}
	}
	txn.ProcessingStartedAt = nil
	txn.ProcessingTimeout = nil
	if err := s.repo.UpdateStatus(ctx, txn); err != nil {
		return nil, fmt.Errorf("payment: update status %s: %w", transactionID, err)
	}
	return txn, nil
}

func (s *Service) Settle(ctx context.Context, transactionID uuid.UUID) (*transaction.Txn, error) {
	txn, err := s.repo.GetByID(ctx, transactionID)
	if err != nil {
		return nil, fmt.Errorf("payment: load transaction %s: %w", transactionID, err)
	}
	if err := transaction.TransitionState(txn, transaction.StatusSettled, transaction.ActorSystem); err != nil {
		return nil, ErrInvalidTransition
	}
	if err := s.repo.UpdateStatus(ctx, txn); err != nil {
		return nil, fmt.Errorf("payment: update status %s: %w", transactionID, err)
	}
	return txn, nil
}


