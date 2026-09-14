package stripe

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/ports"
)

type balanceTransaction struct {
	ID              string   `json:"id"`
	Amount          int64    `json:"amount"`
	Fee              int64    `json:"fee"`
	Currency        string   `json:"currency"`
	Type            string   `json:"type"`
	Created         int64    `json:"created"`
	PaymentIntent   string   `json:"payment_intent"`
	ExchangeRate    *float64 `json:"exchange_rate,omitempty"`
}

type balanceTxnListResponse struct {
	Data []balanceTransaction `json:"data"`
}

func (a *Adapter) FetchSettlementReport(ctx context.Context, tenantID uuid.UUID, gatewayID string, start, end time.Time) (*ports.SettlementReport, error) {
	tc, err := a.resolve(ctx, tenantID)
	if err != nil {
		return nil, &ports.GatewayError{
			Category: ports.ErrorCategoryGatewayError, Code: "config_resolve_error",
			GatewayMessage: err.Error(),
		}
	}

	q := url.Values{}
	q.Set("created[gte]", strconv.FormatInt(start.Unix(), 10))
	q.Set("created[lte]", strconv.FormatInt(end.Unix(), 10))
	q.Set("limit", "100")

	var list balanceTxnListResponse
	path := "/v1/balance_transactions?" + q.Encode()
	if err := a.do(ctx, tc, http.MethodGet, path, nil, "", &list); err != nil {
		return nil, err
	}

	report := &ports.SettlementReport{
		GatewayID:   gatewayID,
		PeriodStart: start,
		PeriodEnd:   end,
		Entries:     make([]ports.SettlementEntry, 0, len(list.Data)),
	}

	for _, bt := range list.Data {
		if bt.Type != "charge" || bt.PaymentIntent == "" {
			continue
		}
		entry := ports.SettlementEntry{
			TransactionRef: bt.PaymentIntent,
			GatewayStatus:  "succeeded",
			GatewayAmount:  bt.Amount,
			GatewayFees:    &bt.Fee,
			FXRate:         bt.ExchangeRate,
			SettledAt:      time.Unix(bt.Created, 0),
		}
		report.Entries = append(report.Entries, entry)
	}

	return report, nil
}

var _ ports.SettlementReportFetcher = (*Adapter)(nil)
