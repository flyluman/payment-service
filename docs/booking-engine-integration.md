# Booking Engine → Payment Service Integration

## Setup

```bash
# Environment variables (set in your service)
PAYMENT_SERVICE_URL=http://localhost:8080   # or production URL
SERVICE_TOKEN=your-service-token            # server-to-server auth
OPS_TOKEN=your-ops-token                    # for refunds/cancellations
```

## 1. Create a Payment

```bash
POST {PAYMENT_SERVICE_URL}/api/v1/payments
Headers:
  X-Service-Token: {SERVICE_TOKEN}
  Idempotency-Key: {uuid}                  # required, unique per request
  Content-Type: application/json
```

**Request:**

```json
{
  "gateway_id": "fib",
  "amount": 50000,
  "currency": "IQD",
  "payment_method": "card",
  "customer_id": "optional-uuid",
  "customer_email": "optional@email.com",
  "description": "Order #12345",
  "metadata": { "order_id": "12345" },
  "callback_url": "https://your-service.com/webhooks/payment",
  "redirect_url": "https://your-app.com/order/12345/complete"
}
```

**Response (201):**

```json
{
  "success": true,
  "data": {
    "transaction_id": "uuid",
    "token": "checkout-token-string",
    "status": "PENDING",
    "fee_breakdown": {
      "summary": { "amount": 50000, "fees": 1750, "total": 51750, "currency": "IQD" },
      "fees": { "service_fee": 1250, "fixed_charge": 500, "total": 1750 },
      "gateway": { "amount": 51750, "currency": "IQD" }
    },
    "gateway_amount": 51750,
    "gateway_currency": "IQD"
  }
}
```

Save `transaction_id` and `token`. Give `token` to the frontend for checkout. `fee_breakdown` shows the complete fee structure — what the user pays, how fees are composed, and what the gateway receives.

## 2. Redirect User to Checkout

```
{PAYMENT_SERVICE_URL}/pay/{transaction_id}?token={token}
```

The hosted page handles everything — QR code, payment, status updates. No frontend code needed.

## 3. Handle Webhook Callback

Configure `callback_url` in step 1. Payment Service POSTs here when status changes.

**Webhook body:**

```json
{
  "transaction_id": "uuid",
  "status": "CAPTURED",
  "amount": 50000,
  "currency": "IQD",
  "gateway_reference_id": "fib_pay_123"
}
```

**Status values:** `PENDING`, `PROCESSING`, `AUTHORIZED`, `CAPTURED`, `SETTLED`, `FAILED`, `CANCELLED`, `REFUND_PENDING`, `PARTIALLY_REFUNDED`, `REFUNDED`, `REFUND_FAILED`, `DISPUTED`

**Your handler must:**
- Return `200 OK` within 10 seconds
- Be idempotent (webhooks can be duplicated)
- Update your order based on `status`

## 4. Get Payment Status

```bash
GET {PAYMENT_SERVICE_URL}/api/v1/payments/{transaction_id}
Headers:
  X-Service-Token: {SERVICE_TOKEN}
```

**Response:**

```json
{
  "success": true,
  "data": {
    "transaction_id": "uuid",
    "status": "CAPTURED",
    "amount": 50000,
    "currency": "IQD",
    "payment_method": "card",
    "gateway_reference_id": "fib_pay_123",
    "customer_id": "uuid",
    "customer_email": "email",
    "description": "text",
    "metadata": {},
    "gateway_metadata": {},
    "fee_breakdown": {
      "summary": { "amount": 50000, "fees": 1750, "total": 51750, "currency": "IQD" },
      "fees": { "service_fee": 1250, "fixed_charge": 500, "total": 1750 },
      "gateway": { "amount": 51750, "currency": "IQD" }
    },
    "gateway_amount": 51750,
    "gateway_currency": "IQD",
    "created_at": "2026-07-30T10:25:00Z"
  }
}
```

## 5. Issue a Refund

```bash
POST {PAYMENT_SERVICE_URL}/api/v1/payments/{transaction_id}/refunds
Headers:
  X-Ops-Token: {OPS_TOKEN}
  Idempotency-Key: {uuid}
  Content-Type: application/json
```

**Request:**

```json
{
  "amount": 50000,
  "reason": "Customer request"
}
```

**Response (201):**

```json
{
  "success": true,
  "data": {
    "id": "refund-uuid",
    "transaction_id": "uuid",
    "status": "REFUNDED",
    "amount": 50000,
    "gateway_refund_id": "fib_ref_123"
  }
}
```

**Refund statuses:** `REFUND_INITIATED`, `REFUND_PROCESSING`, `REFUNDED`, `REFUND_FAILED`

## 6. Cancel a Payment

```bash
POST {PAYMENT_SERVICE_URL}/api/v1/payments/{transaction_id}/cancel
Headers:
  X-Ops-Token: {OPS_TOKEN}
  Idempotency-Key: {uuid}
```

No request body. Only `PENDING` transactions can be cancelled.

## 7. List Payments

```bash
GET {PAYMENT_SERVICE_URL}/api/v1/payments?status=CAPTURED&limit=50
Headers:
  X-Service-Token: {SERVICE_TOKEN}
```

**Filters:** `tenant_id`, `user_id`, `status`, `gateway_id`, `min_amount`, `max_amount`, `currency`, `payment_method`, `date_from`, `date_to`, `cursor`, `limit`

**Response:**

```json
{
  "success": true,
  "data": [ { "transaction_id": "uuid", "amount": 50000, "status": "CAPTURED", ... } ],
  "has_more": false,
  "next_cursor": null
}
```

**CSV download:** Add `Accept: text/csv` header.

## Error Responses

All errors return:

```json
{
  "success": false,
  "error": { "code": "error_code", "message": "Human-readable" }
}
```

| Status | Code | Meaning |
|--------|------|---------|
| 400 | `missing_idempotency_key` | Idempotency-Key header required |
| 401 | `unauthorized` | Invalid or missing token |
| 404 | `not_found` | Payment not found |
| 409 | `idempotency_in_progress` | Previous request still processing |
| 409 | `idempotency_key_reused` | Key reused with different body |
| 422 | `not_refundable` | Transaction not in refundable state |

## Notes

- `Idempotency-Key` is required on all POST mutations (create, refund, cancel)
- FIB gateway: IQD currency only, no partial refunds
- Checkout tokens are single-use and expire after payment finalizes
- Never expose `X-Service-Token` or `X-Ops-Token` in browser code
