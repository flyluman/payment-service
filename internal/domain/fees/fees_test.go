package fees

import (
	"math"
	"testing"
)

func TestCalculate_SameCurrency_NoFees(t *testing.T) {
	b := Calculate(50000, "IQD", "IQD", 0, 0, nil)
	if b.Summary.Amount != 50000 {
		t.Errorf("summary.amount: got %d, want 50000", b.Summary.Amount)
	}
	if b.Summary.Total != 50000 {
		t.Errorf("summary.total: got %d, want 50000", b.Summary.Total)
	}
	if b.Summary.Fees != 0 {
		t.Errorf("summary.fees: got %d, want 0", b.Summary.Fees)
	}
	if b.Gateway == nil || b.Gateway.Amount != 50000 {
		t.Errorf("gateway.amount: got %v, want 50000", b.Gateway)
	}
	if b.Summary.Currency != "IQD" {
		t.Errorf("summary.currency: got %s, want IQD", b.Summary.Currency)
	}
	if b.Fees.Exchange != nil {
		t.Error("exchange should be nil for same currency")
	}
}

func TestCalculate_SameCurrency_WithFees(t *testing.T) {
	// 2.5% reverse-inclusive service fee + 500 fixed charge
	b := Calculate(10000, "IQD", "IQD", 500, 2.5, nil)

	expectedServiceFee := 10000.0/0.975 - 10000.0
	if math.Abs(float64(b.Fees.ServiceFee)-expectedServiceFee) > 1 {
		t.Errorf("fees.service_fee: got %d, want ~%d", b.Fees.ServiceFee, int64(math.Ceil(expectedServiceFee)))
	}

	expectedTotalFees := expectedServiceFee + 500
	if math.Abs(float64(b.Fees.Total)-expectedTotalFees) > 1 {
		t.Errorf("fees.total: got %d, want ~%d", b.Fees.Total, int64(math.Ceil(expectedTotalFees)))
	}

	expectedTotal := 10000.0 + expectedTotalFees
	if math.Abs(float64(b.Summary.Total)-expectedTotal) > 1 {
		t.Errorf("summary.total: got %d, want ~%d", b.Summary.Total, int64(math.Ceil(expectedTotal)))
	}
}

func TestCalculate_SameCurrency_FixedOnly(t *testing.T) {
	b := Calculate(10000, "IQD", "IQD", 500, 0, nil)
	if b.Fees.FixedCharge != 500 {
		t.Errorf("fees.fixed_charge: got %d, want 500", b.Fees.FixedCharge)
	}
	if b.Summary.Total != 10500 {
		t.Errorf("summary.total: got %d, want 10500", b.Summary.Total)
	}
	if b.Fees.ServiceFee != 0 {
		t.Errorf("fees.service_fee: got %d, want 0", b.Fees.ServiceFee)
	}
}

func TestCalculate_SameCurrency_PercentageOnly(t *testing.T) {
	b := Calculate(10000, "IQD", "IQD", 0, 5.0, nil)
	expectedServiceFee := 10000.0/0.95 - 10000.0
	if math.Abs(float64(b.Fees.ServiceFee)-expectedServiceFee) > 1 {
		t.Errorf("fees.service_fee: got %d, want ~%d", b.Fees.ServiceFee, int64(math.Ceil(expectedServiceFee)))
	}
	if b.Fees.FixedCharge != 0 {
		t.Errorf("fees.fixed_charge: got %d, want 0", b.Fees.FixedCharge)
	}
}

func TestCalculate_CrossCurrency_WithMarkup(t *testing.T) {
	rates := []CurrencyRate{
		{FromCurrency: "USD", ToCurrency: "IQD", Rate: 1500, MarkupPct: 3, MarkupFixed: 0},
		{FromCurrency: "IQD", ToCurrency: "USD", Rate: 0.000667, MarkupPct: 2, MarkupFixed: 0},
	}

	// $10 USD -> IQD gateway, with 2.5% service fee + 500 fixed
	b := Calculate(10, "USD", "IQD", 500, 2.5, rates)

	if b.Gateway == nil {
		t.Fatal("gateway should not be nil for cross currency")
	}
	if b.Gateway.Currency != "IQD" {
		t.Errorf("gateway.currency: got %s, want IQD", b.Gateway.Currency)
	}
	if b.Fees.Exchange == nil {
		t.Fatal("exchange should not be nil for cross currency")
	}
	if b.Fees.Exchange.BaseRate != 1500 {
		t.Errorf("exchange.base_rate: got %f, want 1500", b.Fees.Exchange.BaseRate)
	}
	if b.Fees.Exchange.MarkupPct != 3 {
		t.Errorf("exchange.markup_pct: got %f, want 3", b.Fees.Exchange.MarkupPct)
	}
	expectedEffective := 1500.0 + (1500.0/100)*3
	if math.Abs(b.Fees.Exchange.Effective-expectedEffective) > 0.01 {
		t.Errorf("exchange.effective: got %f, want %f", b.Fees.Exchange.Effective, expectedEffective)
	}
}

func TestCalculate_NoExchangeRate_SameCurrencyFallback(t *testing.T) {
	b := Calculate(10000, "IQD", "IQD", 500, 2.5, nil)
	if b.Summary.Total <= 10000 {
		t.Errorf("summary.total should be > base when fees applied, got %d", b.Summary.Total)
	}
}

func TestCalculate_RoundUp(t *testing.T) {
	b := Calculate(100, "IQD", "IQD", 33, 2.5, nil)
	expectedTotal := int64(math.Ceil(float64(b.Summary.Amount) + float64(b.Fees.Total)))
	if b.Summary.Total != expectedTotal {
		t.Errorf("summary.total: got %d, want %d", b.Summary.Total, expectedTotal)
	}
}

func TestComputeServiceFee_ZeroRatio(t *testing.T) {
	fee := computeServiceFee(10000, 0)
	if fee != 0 {
		t.Errorf("expected 0, got %f", fee)
	}
}

func TestComputeServiceFee_HighRatio(t *testing.T) {
	fee := computeServiceFee(10000, 10)
	expected := 10000.0/0.9 - 10000.0
	if math.Abs(fee-expected) > 0.01 {
		t.Errorf("got %f, want %f", fee, expected)
	}
}

func TestApplyExchangeRate_WithMarkup(t *testing.T) {
	cr := &CurrencyRate{
		FromCurrency: "USD",
		ToCurrency:   "IQD",
		Rate:         1500,
		MarkupPct:    3,
		MarkupFixed:  5,
	}
	// effectiveRate = 1500 + (1500/100 * 3) + 5 = 1500 + 45 + 5 = 1550
	// result = 1550 * 10 = 15500
	result := applyExchangeRate(10, cr)
	if math.Abs(result-15500) > 0.01 {
		t.Errorf("got %f, want 15500", result)
	}
}

func TestApplyExchangeRate_NoMarkup(t *testing.T) {
	cr := &CurrencyRate{
		FromCurrency: "USD",
		ToCurrency:   "IQD",
		Rate:         1500,
	}
	result := applyExchangeRate(10, cr)
	if math.Abs(result-15000) > 0.01 {
		t.Errorf("got %f, want 15000", result)
	}
}

func TestFindRate_CaseInsensitive(t *testing.T) {
	rates := []CurrencyRate{
		{FromCurrency: "usd", ToCurrency: "iqd", Rate: 1500},
	}
	r := findRate(rates, "USD", "IQD")
	if r == nil {
		t.Fatal("expected to find rate")
	}
	if r.Rate != 1500 {
		t.Errorf("rate: got %f, want 1500", r.Rate)
	}
}

func TestFindRate_NotFound(t *testing.T) {
	r := findRate(nil, "USD", "IQD")
	if r != nil {
		t.Errorf("expected nil, got %v", r)
	}
}

func TestValidate(t *testing.T) {
	b := &Breakdown{
		Summary: Summary{Amount: 10000, Total: 10500},
		Gateway: &Gateway{Amount: 10500, Currency: "IQD"},
	}
	if err := Validate(b); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if err := Validate(nil); err == nil {
		t.Error("expected error for nil breakdown")
	}

	b2 := &Breakdown{
		Summary: Summary{Amount: 0, Total: 0},
		Gateway: &Gateway{Amount: 0, Currency: "IQD"},
	}
	if err := Validate(b2); err == nil {
		t.Error("expected error for zero amount")
	}
}
