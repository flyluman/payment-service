package fees

import (
	"fmt"
	"math"
	"strings"
)

// CurrencyRate represents an exchange rate with platform markup.
type CurrencyRate struct {
	FromCurrency string
	ToCurrency   string
	Rate         float64
	MarkupPct    float64
	MarkupFixed  float64
}

// ErrMissingRate is returned when a cross-currency fee calculation is
// requested but no exchange rate is configured for the pair.
type ErrMissingRate struct {
	FromCurrency string
	ToCurrency   string
}

func (e ErrMissingRate) Error() string {
	return fmt.Sprintf("fees: no exchange rate for %s -> %s", e.FromCurrency, e.ToCurrency)
}

// Breakdown holds the complete fee calculation result in 3 parts:
//   - Summary: what the user pays (amount, fees, total)
//   - Fees: how fees are composed (service fee, fixed charge, optional exchange info)
//   - Gateway: what the gateway receives (amount + currency)
type Breakdown struct {
	Summary Summary  `json:"summary"`
	Fees    FeeDetail `json:"fees"`
	Gateway *Gateway  `json:"gateway,omitempty"`
}

// Summary is what the user sees at checkout. All amounts in user's currency.
type Summary struct {
	Amount   int64  `json:"amount"`
	Fees     int64  `json:"fees"`
	Total    int64  `json:"total"`
	Currency string `json:"currency"`
}

// FeeDetail breaks down each fee component. All amounts in user's currency.
type FeeDetail struct {
	ServiceFee  int64    `json:"service_fee"`
	FixedCharge int64    `json:"fixed_charge"`
	Total       int64    `json:"total"`
	Exchange    *Exchange `json:"exchange,omitempty"`
}

// Exchange is present only when currency conversion applies.
type Exchange struct {
	BaseRate  float64 `json:"base_rate"`
	MarkupPct float64 `json:"markup_pct"`
	Effective float64 `json:"effective"`
}

// Gateway shows what the gateway receives. Always in gateway's currency.
type Gateway struct {
	Amount   int64  `json:"amount"`
	Currency string `json:"currency"`
}

// Calculate computes the two-layer fee breakdown:
//   - Layer 1: exchange rate with markup (currency conversion)
//   - Layer 2: reverse-inclusive service fee + fixed charge
//
// It returns an error when the transaction and gateway charge currencies
// differ but no exchange rate is configured, so callers never silently
// compute fees in the wrong currency.
func Calculate(
	amount int64,
	transactionCurrency string,
	gatewayChargesCurrency string,
	fixedCharge float64,
	serviceFeeRatio float64,
	rates []CurrencyRate,
) (*Breakdown, error) {
	transactionCurrency = strings.ToUpper(transactionCurrency)
	gatewayChargesCurrency = strings.ToUpper(gatewayChargesCurrency)

	if transactionCurrency == gatewayChargesCurrency {
		return calculateSameCurrency(amount, transactionCurrency, fixedCharge, serviceFeeRatio), nil
	}

	rate := findRate(rates, transactionCurrency, gatewayChargesCurrency)
	if rate == nil {
		return nil, ErrMissingRate{FromCurrency: transactionCurrency, ToCurrency: gatewayChargesCurrency}
	}

	// Layer 1: convert user amount to gateway charges currency with markup
	gatewayBaseAmount := applyExchangeRate(float64(amount), rate)

	// Layer 2: reverse-inclusive service fee + fixed charge (in gateway currency)
	serviceFeeGW := computeServiceFee(gatewayBaseAmount, serviceFeeRatio)
	totalFeesGW := serviceFeeGW + fixedCharge
	gatewayTotal := gatewayBaseAmount + totalFeesGW

	// Convert each fee component back to user currency for display
	userServiceFee := convertBack(serviceFeeGW, rates, gatewayChargesCurrency, transactionCurrency)
	userFixedCharge := convertBack(fixedCharge, rates, gatewayChargesCurrency, transactionCurrency)

	serviceFeeR := int64(math.Ceil(userServiceFee))
	fixedChargeR := int64(math.Ceil(userFixedCharge))
	totalFeesR := serviceFeeR + fixedChargeR

	// Exchange rate info
	effectiveRate := rate.Rate + (rate.Rate/100)*rate.MarkupPct + rate.MarkupFixed

	return &Breakdown{
		Summary: Summary{
			Amount:   amount,
			Fees:     totalFeesR,
			Total:    amount + totalFeesR,
			Currency: transactionCurrency,
		},
		Fees: FeeDetail{
			ServiceFee:  serviceFeeR,
			FixedCharge: fixedChargeR,
			Total:       totalFeesR,
			Exchange: &Exchange{
				BaseRate:  rate.Rate,
				MarkupPct: rate.MarkupPct,
				Effective: effectiveRate,
			},
		},
		Gateway: &Gateway{
			Amount:   int64(math.Ceil(gatewayTotal)),
			Currency: gatewayChargesCurrency,
		},
	}, nil
}

// calculateSameCurrency handles the case where transaction and gateway use the same currency.
func calculateSameCurrency(amount int64, currency string, fixedCharge, serviceFeeRatio float64) *Breakdown {
	baseAmount := float64(amount)
	serviceFee := computeServiceFee(baseAmount, serviceFeeRatio)

	serviceFeeR := int64(math.Ceil(serviceFee))
	fixedChargeR := int64(math.Ceil(fixedCharge))
	totalFeesR := serviceFeeR + fixedChargeR
	totalR := amount + totalFeesR

	return &Breakdown{
		Summary: Summary{
			Amount:   amount,
			Fees:     totalFeesR,
			Total:    totalR,
			Currency: currency,
		},
		Fees: FeeDetail{
			ServiceFee:  serviceFeeR,
			FixedCharge: fixedChargeR,
			Total:       totalFeesR,
		},
		Gateway: &Gateway{
			Amount:   totalR,
			Currency: currency,
		},
	}
}

// applyExchangeRate converts an amount using the rate with markup.
// effectiveRate = rate + (rate / 100 * markupPct) + markupFixed
func applyExchangeRate(amount float64, cr *CurrencyRate) float64 {
	markupOnRate := (cr.Rate / 100) * cr.MarkupPct
	effectiveRate := cr.Rate + markupOnRate + cr.MarkupFixed
	return effectiveRate * amount
}

// computeServiceFee uses reverse-inclusive formula:
// fee = amount / (1 - ratio/100) - amount
func computeServiceFee(amount float64, ratio float64) float64 {
	if ratio <= 0 || ratio >= 100 {
		return 0
	}
	return amount/(1-ratio/100) - amount
}

// convertBack converts an amount in fromCurrency to toCurrency using the reverse rate.
// Each direction has its own independent markup — the reverse rate includes its own markup.
func convertBack(amount float64, rates []CurrencyRate, fromCurrency, toCurrency string) float64 {
	rate := findRate(rates, fromCurrency, toCurrency)
	if rate == nil {
		return amount
	}
	return applyExchangeRate(amount, rate)
}

// findRate locates a matching currency pair (case-insensitive).
func findRate(rates []CurrencyRate, from, to string) *CurrencyRate {
	from = strings.ToUpper(from)
	to = strings.ToUpper(to)
	for i := range rates {
		if strings.EqualFold(rates[i].FromCurrency, from) && strings.EqualFold(rates[i].ToCurrency, to) {
			return &rates[i]
		}
	}
	return nil
}

// Validate checks that the breakdown is consistent.
func Validate(b *Breakdown) error {
	if b == nil {
		return fmt.Errorf("breakdown is nil")
	}
	if b.Summary.Amount <= 0 {
		return fmt.Errorf("summary.amount must be positive, got %d", b.Summary.Amount)
	}
	if b.Gateway != nil && b.Gateway.Amount <= 0 {
		return fmt.Errorf("gateway.amount must be positive, got %d", b.Gateway.Amount)
	}
	if b.Summary.Fees != b.Fees.Total {
		return fmt.Errorf("summary.fees (%d) must equal fees.total (%d)", b.Summary.Fees, b.Fees.Total)
	}
	if b.Fees.Total != b.Fees.ServiceFee+b.Fees.FixedCharge {
		return fmt.Errorf("fees.total (%d) must equal service_fee (%d) + fixed_charge (%d)", b.Fees.Total, b.Fees.ServiceFee, b.Fees.FixedCharge)
	}
	if b.Summary.Total != b.Summary.Amount+b.Summary.Fees {
		return fmt.Errorf("summary.total (%d) must equal summary.amount (%d) + summary.fees (%d)", b.Summary.Total, b.Summary.Amount, b.Summary.Fees)
	}
	if b.Gateway != nil && b.Gateway.Currency == b.Summary.Currency && b.Gateway.Amount != b.Summary.Total {
		return fmt.Errorf("gateway.amount (%d) must equal summary.total (%d) in same-currency breakdown", b.Gateway.Amount, b.Summary.Total)
	}
	return nil
}
