# Frontend Integration — Payment Checkout

## Quick Start

When user clicks "Pay", your backend creates a payment and gives you a `transaction_id` + `token`. Redirect to the hosted checkout:

```javascript
window.location.href =
  `${PAYMENT_DOMAIN}/pay/${transaction_id}?token=${token}`;
```

That's it. The hosted page handles QR code, payment processing, and redirects.

## Flow

```
Your App → Your Backend → Payment Service API
   1. User clicks "Pay"
   2. Backend calls POST /api/v1/payments
   3. Backend returns { transaction_id, token }
   4. You redirect to /pay/{id}?token={token}
   5. Hosted page renders FIB QR code + app links
   6. User scans QR → pays → page redirects to redirect_url
```

## Getting the Checkout Token

Your backend calls the Payment Service API. You receive:

```json
{
  "transaction_id": "uuid",
  "token": "checkout-token-string"
}
```

Pass `token` to your frontend. Never expose your backend's `X-Service-Token`.

## Hosted Checkout Page

Redirect to:

```
{PAYMENT_DOMAIN}/pay/{transaction_id}?token={token}
```

**What the page does:**
- Shows FIB QR code, app links (Personal/Business/Corporate), countdown timer
- Listens for payment completion via SSE (real-time)
- On success: redirects to your `redirect_url` after 3 seconds
- On failure: shows error message, redirects to your `redirect_url` after 3 seconds

## Building Your Own Checkout (Optional)

If you need a custom UI instead of the hosted page:

### Get Payment Details

```javascript
const res = await fetch(
  `${PAYMENT_DOMAIN}/api/v1/payments/${transactionId}?token=${token}`
);
const { data } = await res.json();
```

### Display Fee Breakdown (Optional)

The payment response includes a `fee_breakdown` object showing the complete fee structure:

```javascript
const { fee_breakdown, gateway_amount, gateway_currency } = data;

if (fee_breakdown) {
  // What the user pays (in their currency)
  const baseAmount = fee_breakdown.summary.amount;    // e.g. 50000
  const fees = fee_breakdown.summary.fees;             // e.g. 1750
  const total = fee_breakdown.summary.total;           // e.g. 51750
  const currency = fee_breakdown.summary.currency;     // e.g. "IQD"

  // Fee composition
  const serviceFee = fee_breakdown.fees.service_fee;   // e.g. 1250
  const fixedCharge = fee_breakdown.fees.fixed_charge; // e.g. 500

  // Exchange rate info (only present for cross-currency)
  if (fee_breakdown.fees.exchange) {
    const { base_rate, markup_pct, effective } = fee_breakdown.fees.exchange;
  }

  // What the gateway receives
  const gatewayAmt = fee_breakdown.gateway.amount;     // e.g. 51750
  const gatewayCurr = fee_breakdown.gateway.currency;  // e.g. "IQD"
}
```

Display example:
```
Item:       50,000 IQD
Service fee: 1,250 IQD
Fixed charge:   500 IQD
─────────────────────
Total:      51,750 IQD
```

### Read FIB QR Code

```javascript
// data.gateway_metadata contains gateway-specific output
const { gateway_metadata } = data;

// FIB checkout
displayQRCode(gateway_metadata.qr_code);           // base64 image
showCode(gateway_metadata.readable_code);           // "FIB-ABC-123"
showLinks({
  personal: gateway_metadata.personal_app_link,
  business: gateway_metadata.business_app_link,
  corporate: gateway_metadata.corporate_app_link,
});
startTimer(gateway_metadata.valid_until);           // countdown
```

### Listen for Status Updates (SSE)

```javascript
const sse = new EventSource(
  `${PAYMENT_DOMAIN}/api/v1/payments/${transactionId}/events?token=${token}`
);

sse.addEventListener('status', (e) => {
  const { status } = JSON.parse(e.data);

  if (status === 'SUCCEEDED') {
    window.location.href = redirectUrl;
  } else if (status === 'FAILED' || status === 'CANCELLED') {
    showError('Payment failed');
  }
});

sse.addEventListener('error', () => {
  sse.close();
  startPolling(); // fallback
});
```

### Polling Fallback

If SSE fails, poll every 5 seconds:

```javascript
const interval = setInterval(async () => {
  const res = await fetch(
    `${PAYMENT_DOMAIN}/api/v1/payments/${transactionId}?token=${token}`
  );
  const { data } = await res.json();

  if (['SUCCEEDED', 'FAILED', 'CANCELLED'].includes(data.status)) {
    clearInterval(interval);
    handleStatus(data.status);
  }
}, 5000);
```

## Checkout Token Rules

- Single-use per transaction
- Expires after payment finalizes (SUCCEEDED/FAILED/CANCELLED)
- Never expose in URLs that could be logged (use query params, not path)
- Only works on `/pay/` and `/api/v1/payments/{id}` endpoints

## Troubleshooting

| Issue | Fix |
|-------|-----|
| "Invalid or missing token" | Token expired or already used. Create new payment. |
| QR code not showing | Check `gateway_metadata.qr_code` exists in payment response |
| Page stuck on "Loading..." | Verify `transaction_id` is a valid UUID |
| Redirect not happening | Ensure `redirect_url` was set at payment creation |
| "Taking longer than expected" | 30s timeout. User may still be paying — check status. |
