package middleware

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"

	"github.com/crownroutes/payment-service/internal/ports"
)

type ResponseCacheStore interface {
	Get(ctx context.Context, key string) (found bool, response []byte, err error)
	Put(ctx context.Context, key string, response []byte) error
}

type cachedResponse struct {
	StatusCode int    `json:"status_code"`
	Body       []byte `json:"body"`
}

func ResponseCache(cache ResponseCacheStore, log ports.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := r.Header.Get("Idempotency-Key")
			if key == "" || !isMutating(r.Method) {
				next.ServeHTTP(w, r)
				return
			}

			bodyHash, err := readBodyHash(r)
			if err != nil {
				next.ServeHTTP(w, r)
				return
			}

			composite := cacheKey(r, key, bodyHash)

			if found, body, err := cache.Get(r.Context(), composite); err == nil && found {
				replayCached(w, body, log)
				return
			}

			rec := &responseRecorder{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(rec, r)

			if rec.status >= 200 && rec.status < 300 {
				if envelope, err := json.Marshal(cachedResponse{StatusCode: rec.status, Body: rec.body.Bytes()}); err == nil {
					_ = cache.Put(r.Context(), composite, envelope)
				}
			}
		})
	}
}

func cacheKey(r *http.Request, key, bodyHash string) string {
	tenant := TenantIDFromContext(r.Context())
	user := UserIDFromContext(r.Context())
	return hashString(tenant + ":" + user + ":" + r.Method + " " + r.URL.Path + ":" + key + ":" + bodyHash)
}

// readBodyHash reads the request body, computes a SHA-256 hash of it, and
// restores the body so downstream handlers can still read it.
func readBodyHash(r *http.Request) (string, error) {
	if r.Body == nil {
		return hashString(""), nil
	}
	data, err := io.ReadAll(r.Body)
	if err != nil {
		return "", err
	}
	r.Body = io.NopCloser(bytes.NewReader(data))
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

type responseRecorder struct {
	http.ResponseWriter
	status      int
	body        bytes.Buffer
	wroteHeader bool
}

func (rec *responseRecorder) WriteHeader(code int) {
	if rec.wroteHeader {
		return
	}
	rec.status = code
	rec.wroteHeader = true
	rec.ResponseWriter.WriteHeader(code)
}

func (rec *responseRecorder) Write(b []byte) (int, error) {
	if !rec.wroteHeader {
		rec.WriteHeader(http.StatusOK)
	}
	rec.body.Write(b)
	return rec.ResponseWriter.Write(b)
}

func replayCached(w http.ResponseWriter, raw []byte, log ports.Logger) {
	var cached cachedResponse
	if err := json.Unmarshal(raw, &cached); err != nil {
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Idempotent-Replayed", "true")
	w.WriteHeader(cached.StatusCode)
	_, _ = w.Write(cached.Body)
}

func isMutating(method string) bool {
	switch method {
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return true
	}
	return false
}

func hashString(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}
