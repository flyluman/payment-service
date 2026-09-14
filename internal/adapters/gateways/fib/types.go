package fib

type tokenResponse struct {
	AccessToken string `json:"access_token"`
	ExpiresIn   int    `json:"expires_in"`
	Scope       string `json:"scope"`
	TokenType   string `json:"token_type"`
}

type monetaryValueObj struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

type createPaymentRequest struct {
	MonetaryValue     monetaryValueObj `json:"monetaryValue"`
	StatusCallbackURL string           `json:"statusCallbackUrl"`
	RedirectURI       string           `json:"redirectUri,omitempty"`
	Description       string           `json:"description,omitempty"`
	Category          string           `json:"category,omitempty"`
}

type createPaymentResponse struct {
	ID               string `json:"id"`
	PaymentID        string `json:"paymentId"`
	ReadableCode     string `json:"readableCode"`
	QRCode           string `json:"qrCode,omitempty"`
	ValidUntil       string `json:"validUntil,omitempty"`
	PersonalAppLink  string `json:"personalAppLink,omitempty"`
	BusinessAppLink  string `json:"businessAppLink,omitempty"`
	CorporateAppLink string `json:"corporateAppLink,omitempty"`
}

type paidBy struct {
	Name string `json:"name"`
	IBAN string `json:"iban"`
}

type paymentStatusResponse struct {
	ID              string        `json:"id"`
	PaymentID       string        `json:"paymentId"`
	Status          string        `json:"status"`
	ValidUntil      *string       `json:"validUntil,omitempty"`
	Amount          *monetaryValueObj `json:"amount,omitempty"`
	DecliningReason *string       `json:"decliningReason,omitempty"`
	PaidAt          *string       `json:"paidAt,omitempty"`
	DeclinedAt      *string       `json:"declinedAt,omitempty"`
	PaidBy          *paidBy       `json:"paidBy,omitempty"`
	PaidAmount      *int64        `json:"paidAmount,omitempty"`
}

type fibError struct {
	Error string `json:"error"`
}

type fibErrorEnvelope struct {
	Error *fibError `json:"error"`
}

type fibWebhookPayload struct {
	ID        string `json:"id"`
	PaymentID string `json:"paymentId"`
	Status    string `json:"status"`
}

type refundResponse struct {
	RefundID string `json:"refundId,omitempty"`
	Status   string `json:"status,omitempty"`
}

type actionResponse struct {
	Status string `json:"status"`
}
