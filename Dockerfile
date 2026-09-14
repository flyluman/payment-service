# syntax=docker/dockerfile:1

# ---- build ----
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/paymentservice ./cmd/server

# ---- certs (alpine only to extract the CA bundle) ----
FROM alpine:3.21 AS certs
RUN apk add --no-cache ca-certificates

# ---- runtime: scratch, static stripped binary, non-root ----
FROM scratch
WORKDIR /app
COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/paymentservice /app/paymentservice
EXPOSE 8080
USER 65532:65532
ENTRYPOINT ["/app/paymentservice"]
