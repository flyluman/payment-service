package razorpay

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/ports"
)

type rzpPayment struct {
	ID        string `json:"id"`
	OrderID   string `json:"order_id"`
	Amount    int64  `json:"amount"`
	Currency  string `json:"currency"`
	Status    string `json:"status"`
	CreatedAt int64  `json:"created_at"`
	Fee       *int64 `json:"fee,omitempty"`
	Tax       *int64 `json:"tax,omitempty"`
}

type rzpPaymentListResponse struct {
	Count int          `json:"count"`
	Items []rzpPayment `json:"items"`
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
	q.Set("from", strconv.FormatInt(start.Unix(), 10))
	q.Set("to", strconv.FormatInt(end.Unix(), 10))
	q.Set("count", "100")

	var all []rzpPayment
	var skip int

	for range 100 {
		q.Set("skip", strconv.Itoa(skip))

		var list rzpPaymentListResponse
		path := "/v1/payments?" + q.Encode()
		if err := a.do(ctx, tc, http.MethodGet, path, nil, &list); err != nil {
			return nil, err
		}

		all = append(all, list.Items...)
		if len(list.Items) < 100 {
			break
		}
		skip += len(list.Items)
	}

	report := &ports.SettlementReport{
		GatewayID:   gatewayID,
		PeriodStart: start,
		PeriodEnd:   end,
		Entries:     make([]ports.SettlementEntry, 0, len(all)),
	}

	for _, p := range all {
		status := "pending"
		if p.Status == "captured" {
			status = "succeeded"
		}
		entry := ports.SettlementEntry{
			TransactionRef: p.OrderID,
			GatewayStatus:  status,
			GatewayAmount:  p.Amount,
			SettledAt:      time.Unix(p.CreatedAt, 0),
		}
		if p.Fee != nil {
			entry.GatewayFees = p.Fee
		}
		report.Entries = append(report.Entries, entry)
	}

	return report, nil
}

var _ ports.SettlementReportFetcher = (*Adapter)(nil)
