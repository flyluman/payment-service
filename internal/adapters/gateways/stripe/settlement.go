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
	ID            string   `json:"id"`
	Amount        int64    `json:"amount"`
	Fee           int64    `json:"fee"`
	Currency      string   `json:"currency"`
	Type          string   `json:"type"`
	Created       int64    `json:"created"`
	PaymentIntent string   `json:"payment_intent"`
	ExchangeRate  *float64 `json:"exchange_rate,omitempty"`
}

type balanceTxnListResponse struct {
	Data []balanceTransaction `json:"data"`
	// HasMore is true when there are more results to paginate through.
	HasMore bool `json:"has_more"`
}

func (a *Adapter) FetchSettlementReport(ctx context.Context, tenantID uuid.UUID, gatewayID string, start, end time.Time) (*ports.SettlementReport, error) {
	tc, err := a.resolve(ctx, tenantID)
	if err != nil {
		return nil, &ports.GatewayError{
			Category: ports.ErrorCategoryGatewayError, Code: "config_resolve_error",
			GatewayMessage: err.Error(),
		}
	}

	var all []balanceTransaction
	var startingAfter string

	for page := 0; page < 100; page++ {
		q := url.Values{}
		q.Set("created[gte]", strconv.FormatInt(start.Unix(), 10))
		q.Set("created[lte]", strconv.FormatInt(end.Unix(), 10))
		q.Set("limit", "100")
		if startingAfter != "" {
			q.Set("starting_after", startingAfter)
		}

		var list balanceTxnListResponse
		path := "/v1/balance_transactions?" + q.Encode()
		if err := a.do(ctx, tc, http.MethodGet, path, nil, "", &list); err != nil {
			return nil, err
		}

		all = append(all, list.Data...)
		if !list.HasMore || len(list.Data) == 0 {
			break
		}
		startingAfter = list.Data[len(list.Data)-1].ID
	}

	report := &ports.SettlementReport{
		GatewayID:   gatewayID,
		PeriodStart: start,
		PeriodEnd:   end,
		Entries:     make([]ports.SettlementEntry, 0, len(all)),
	}

	for _, bt := range all {
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
